%% @doc Process handling one single MQTT session.
%%      Transports attaches and detaches from this session.
%% @author Marc Worrell <marc@worrell.nl>
%% @copyright 2018-2019 Marc Worrell

%% Copyright 2018-2019 Marc Worrell
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.


%% TODO: Limit in-flight acks (both ways)
%% TODO: Drop outgoing QoS 0 messages if pending gets too large
%% TODO: Refuse incoming publish messages if too many publish_jobs

-module(mqtt_sessions_process).

-behaviour(gen_server).

-export([
    get_user_context/1,
    set_user_context/2,
    update_user_context/2,

    kill/1,
    incoming_message/3,
    fetch_queue/1,
    start_link/3,
    connect_transport/2,
    disconnect_transport/2
    ]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
    ]).

-define(MAX_PACKET_ID, 65535).
-define(RECEIVE_MAXIMUM, 65535).
-define(KEEP_ALIVE, 30).            % Default keep alive in seconds
-define(SESSION_EXPIRY, 600).       % Default session expiration
-define(SESSION_EXPIRY_MAX, 3600).  % Maximum allowed session expiration
-define(DEFAULT_MESSAGE_EXPIRY, 3600).

-type packet_id() :: 0..?MAX_PACKET_ID.

-record(state, {
    pool :: atom(),
    runtime :: atom(),
    client_id :: binary(),
    routing_id :: binary(),
    is_connected = false :: boolean(),
    is_session_present = false :: boolean(),
    user_context :: term(),
    transport = undefined :: pid(),
    pending_connack = undefined :: term(),
    pending :: queue:queue(),
    packet_id = 1 :: packet_id(),
    send_quota = ?RECEIVE_MAXIMUM :: non_neg_integer(),
    awaiting_ack = #{} :: map(),  % Initiated by server
    awaiting_rel = #{} :: map(),  % Initiated by client
    will = undefined :: undefined | map(),
    will_pid = undefined :: undefined | pid(),
    msg_nr = 0 :: non_neg_integer(),
    keep_alive = ?KEEP_ALIVE :: non_neg_integer(),
    keep_alive_counter = 3,
    session_expiry_interval = ?SESSION_EXPIRY :: non_neg_integer(),

    % Tracking publish jobs
    publish_jobs = #{} :: map()
}).

-record(queued, {
    type :: atom(),
    msg_nr :: pos_integer(),
    packet_id = undefined :: undefined | non_neg_integer(),
    queued :: pos_integer(),
    expiry :: pos_integer(),
    qos :: 0..2,
    message :: mqtt_packet_map:mqtt_packet()
}).


-include_lib("../include/mqtt_sessions.hrl").
-include_lib("mqtt_packet_map/include/mqtt_packet_map.hrl").


-spec get_user_context( pid() ) -> {ok, term()} | {error, noproc}.
get_user_context(Pid) ->
    try
        gen_server:call(Pid, get_user_context, infinity)
    catch
        exit:{noproc, _} ->
            {error, noproc}
    end.

-spec set_user_context( pid(), term() ) -> ok | {error, noproc}.
set_user_context(Pid, UserContext) ->
    try
        gen_server:call(Pid, {set_user_context, UserContext}, infinity)
    catch
        exit:{noproc, _} ->
            {error, noproc}
    end.

-spec update_user_context( pid(), fun( (term()) -> term() ) ) -> ok | {error, noproc}.
update_user_context(Pid, Fun) ->
    try
        gen_server:call(Pid, {update_user_context, Fun}, infinity)
    catch
        exit:{noproc, _} ->
            {error, noproc}
    end.


-spec kill( pid() ) -> ok.
kill(Pid) ->
    gen_server:cast(Pid, kill).

-spec incoming_message(pid(), mqtt_packet_map:mqtt_packet(), mqtt_sessions:msg_options()) -> ok.
incoming_message(Pid, Msg, Options) when is_map(Options) ->
    gen_server:cast(Pid, {incoming, Msg, Options}).

-spec fetch_queue(pid()) -> {ok, list( map() | binary() )}.
fetch_queue( Pid ) ->
    gen_server:call(Pid, fetch_queue, infinity).

% @doc Register or unregister the transport used by the process.
-spec connect_transport(pid(), pid() | undefined) -> ok.
connect_transport(Pid, TransportPid) ->
    gen_server:call(Pid, {connect_transport, TransportPid}, infinity).

% @doc Register or unregister the transport used by the process.
-spec disconnect_transport(pid(), pid() | undefined) -> ok.
disconnect_transport(Pid, TransportPid) ->
    gen_server:call(Pid, {disconnect_transport, TransportPid}, infinity).

-spec start_link( Pool::atom(), ClientId::binary(), mqtt_sessions:session_options() ) -> {ok, pid()}.
start_link( Pool, ClientId, SessionOptions ) ->
    gen_server:start_link(?MODULE, [ Pool, ClientId, SessionOptions ], []).


% ---------------------------------------------------------------------------------------
% --------------------------- gen_server functions --------------------------------------
% ---------------------------------------------------------------------------------------

init([ Pool, ClientId, SessionOptions ]) ->
    RoutingId = mqtt_sessions_registry:routing_id(Pool),
    mqtt_sessions_registry:register(Pool, ClientId, self()),
    {ok, WillPid} = mqtt_sessions_will_sup:start(Pool, self()),
    {ok, Runtime} = application:get_env(mqtt_sessions, runtime),
    erlang:monitor(process, WillPid),
    SessionOptions1 = SessionOptions#{
        routing_id => RoutingId
    },
    {ok, #state{
        pool = Pool,
        runtime = Runtime,
        user_context = Runtime:new_user_context(Pool, ClientId, SessionOptions1),
        client_id = ClientId,
        routing_id = RoutingId,
        pending = queue:new(),
        will_pid = WillPid
    }}.

handle_call({connect_transport, TransportPid}, _From, State) ->
    State1 = select_transport([ {transport, TransportPid} ], State),
    {reply, ok, State1};
handle_call({disconnect_transport, TransportPid}, _From, #state{ transport = TransportPid } = State) ->
    State1 = do_disconnected(State),
    {noreply, State1};
handle_call({disconnect_transport, TransportPid}, _From, State) ->
    lager:info("Transport disconnect for ~p, but connected to ~p",
               [TransportPid, State#state.transport]),
    {reply, {error, notconnected}, State};

handle_call(fetch_queue, _From, #state{ pending_connack = undefined } = State) ->
    Qs = [ Msg || #queued{ message = Msg } <- queue:to_list(State#state.pending) ],
    {reply, {ok, encode(Qs)}, State#state{ pending = queue:new() }};
handle_call(fetch_queue, _From, #state{ pending_connack = ConnAck } = State) ->
    {reply, {ok, encode([ ConnAck ])}, State#state{ pending_connack = undefined }};

handle_call(get_user_context, _From, #state{ user_context = UserContext } = State) ->
    {reply, {ok, UserContext}, State};
handle_call({set_user_context, UserContext}, _From, State) ->
    {reply, ok, State#state{ user_context = UserContext }};
handle_call({update_user_context, Fun}, _From, #state{ user_context = UserContext} = State) ->
    {reply, ok, State#state{ user_context = Fun(UserContext) }};

handle_call(Cmd, _From, State) ->
    {stop, {unknown_cmd, Cmd}, State}.

handle_cast({incoming, Msg, Options}, State) ->
    case handle_incoming_context(Msg, Options, State) of
        {ok, State1} ->
            {noreply, State1#state{ keep_alive_counter = 3 }};
        {stop, State1} ->
            % Error, stop session and force disconnect
            case State1#state.transport of
                undefined -> ok;
                Pid -> Pid ! {reply, disconnect}
            end,
            {stop, shutdown, State1}
    end;
handle_cast(kill, State) ->
    {stop, shutdown, State}.


handle_info({mqtt_msg, #{ type := publish } = MqttMsg}, State) ->
    State1 = relay_publish(MqttMsg, State),
    {noreply, State1};

handle_info({keep_alive, Pid}, #state{ keep_alive_counter = 0, transport = Pid } = State) ->
    lager:debug("MQTT past keep_alive, disconnecting ~p", [Pid]),
    Pid ! {reply, disconnect},
    {noreply, State};
handle_info({keep_alive, Pid}, #state{ keep_alive_counter = N, transport = Pid } = State) ->
    erlang:send_after(State#state.keep_alive * 500, self(), {keep_alive, Pid}),
    {noreply, State#state{ keep_alive_counter = erlang:max(N-1, 0) }};
handle_info({keep_alive, _Pid}, State) ->
    {noreply, State};

handle_info({publish_job, JobPid}, #state{ publish_jobs = Jobs } = State) ->
    State1 = case erlang:is_process_alive(JobPid) of
        true ->
            State#state{ publish_jobs = Jobs#{ JobPid => erlang:monitor(process, JobPid) } };
        false ->
            State
    end,
    {noreply, State1};

handle_info({'DOWN', _Mref, process, Pid, _Reason}, #state{ transport = Pid } = State) ->
    State1 = do_disconnected(State),
    {noreply, State1};
handle_info({'DOWN', _Mref, process, Pid, _Reason}, #state{ will_pid = Pid } = State) ->
    send_transport(#{
        type => disconnect,
        reason_code => ?MQTT_RC_ERROR
    }, State),
    {stop, shutdown, State};
handle_info({'DOWN', _Mref, process, Pid, _Reason}, State) ->
    State1 = case maps:is_key(Pid, State#state.publish_jobs) of
        true ->
            State#state{ publish_jobs = maps:remove(Pid, State#state.publish_jobs) };
        false ->
            State
    end,
    {noreply, State1};

handle_info(Info, State) ->
    lager:info("Unknown info message ~p", [Info]),
    {noreply, State}.

code_change(_Vsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

% ---------------------------------------------------------------------------------------
% ----------------------------- support functions ---------------------------------------
% ---------------------------------------------------------------------------------------

handle_incoming_context(Msg, Options, #state{ runtime = Runtime, user_context = UserContext } = State) ->
    case Runtime:is_valid_message(Msg, Options, UserContext) of
        true ->
            handle_incoming(Msg, Options, State);
        false ->
            {stop, State}
    end.

handle_incoming(#{ type := connect } = Msg, Options, #state{ is_connected = false } = State) ->
    connect(Msg, Options, State);
handle_incoming(#{ type := connect, clean_start := false } = Msg, Options, #state{ is_connected = true } = State) ->
    % A client reopens a connection. Check if the credentials match with the current
    % session credentials (otherwise someone else might steal this session).
    connect(Msg, Options, State);
handle_incoming(#{ type := connect, clean_start := true }, _Options, #state{ is_connected = true } = State) ->
    lager:info("Received connect with clean_start for existing MQTT session ~p ~s (~p)",
               [State#state.pool, State#state.client_id, self()]),
    {stop, State};
handle_incoming(#{ type := auth } = Msg, Options, State) ->
    connect_auth(Msg, Options, State);
handle_incoming(#{ type := Type }, _Options, #state{ is_connected = false } = State) ->
    lager:info("Killing MQTT session ~p ~s (~p) for receiving ~p when not connected.",
               [State#state.pool, State#state.client_id, self(), Type]),
    {stop, State};
handle_incoming(#{ type := Type }, _Options, #state{ is_session_present = false } = State) ->
    % Only AUTH and CONNECT before the CONNACK
    lager:info("Killing MQTT session ~p ~s (~p) for receiving ~p when no session started.",
               [State#state.pool, State#state.client_id, self(), Type]),
    {stop, State};
handle_incoming(#{ type := publish } = Msg, Options, State) ->
    publish(Msg, Options, State);

% PUBREL is for publish messages sent by the client
handle_incoming(#{ type := pubrel } = Msg, Options, State) ->
    pubrel(Msg, Options, State);

% PUBREC, PUBACK, PUBCOMP is for publish messages sent by us to the client
handle_incoming(#{ type := pubrec } = Msg, Options, State) ->
    pubrec(Msg, Options, State);
handle_incoming(#{ type := pubcomp } = Msg, Options, State) ->
    pubcomp(Msg, Options, State);
handle_incoming(#{ type := puback } = Msg, Options, State) ->
    puback(Msg, Options, State);

handle_incoming(#{ type := subscribe } = Msg, Options, State) ->
    subscribe(Msg, Options, State);
handle_incoming(#{ type := unsubscribe } = Msg, Options, State) ->
    unsubscribe(Msg, Options, State);

handle_incoming(#{ type := pingreq }, Options, State) ->
    State1 = reply(#{ type => pingresp }, Options, State),
    {ok, State1};
handle_incoming(#{ type := pingresp }, _Options, State) ->
    {ok, State};
handle_incoming(#{ type := disconnect } = Msg, Options, State) ->
    disconnect(Msg, Options, State).


% ---------------------------------------------------------------------------------------
% --------------------------- message type functions ------------------------------------
% ---------------------------------------------------------------------------------------


%% @doc Handle the connect message. Either this is a re-connect or the first connect.
connect(#{ protocol_version := 5, protocol_name := <<"MQTT">>, properties := Props } = Msg, Options, State) ->
    ExpiryInterval = case maps:get(session_expiry_interval, Props, 0) of
        0 -> ?SESSION_EXPIRY_MAX;
        EI -> EI
    end,
    State1 = State#state{
        will = extract_will(Msg),
        session_expiry_interval = ExpiryInterval,
        keep_alive = maps:get(keep_alive, Msg, 0),
        is_connected = false
    },
    connect_auth(Msg, Options, State1);
connect(_ConnectMsg, Options, State) ->
    ConnAck = #{
        type => connack,
        reason_code => ?MQTT_RC_PROTOCOL_VERSION
    },
    State1 = reply(ConnAck, Options, State),
    {stop, State1}.

connect_auth(Msg, Options, #state{ runtime = Runtime, is_connected = IsConnected, user_context = UserContext } = State) ->
    Fun = case IsConnected of
        false -> connect;
        true -> reauth
    end,
    case Runtime:Fun(Msg, UserContext) of
        {ok, #{ type := connack, reason_code := ReasonCode } = ConnAck, UserContext1} ->
            State1 = State#state{
                user_context = UserContext1,
                will = undefined
            },
            State2 = reply_connack(ConnAck, Options, State1),
            case ReasonCode of
                ?MQTT_RC_SUCCESS ->
                    mqtt_sessions_will:connected(State2#state.will_pid, State#state.will,
                                                 State2#state.session_expiry_interval, State2#state.user_context),
                    State3 = State2#state{
                        is_connected = true,
                        is_session_present = true,
                        will = undefined
                    },
                    State4 = resendUnacknowledged( cleanupPending(State3) ),
                    {ok, State4};
                _ ->
                    State3 = State2#state{
                        is_connected = false
                    },
                    {stop, State3}
            end;
        {ok, #{ type := auth } = Auth, UserContext1} ->
            State1 = State#state{
                user_context = UserContext1
            },
            State2 = reply(Auth, Options, State1),
            mqtt_sessions_will:connected(State2#state.will_pid, undefined,
                                         State2#state.session_expiry_interval, State2#state.user_context),
            {ok, State2};
        {error, Reason} ->
            lager:info("MQTT connect/auth refused (~p): ~p", [Reason, Msg]),
            {stop, State}
    end.


%% @doc Handle a publish request
publish(#{ topic := Topic, qos := 0 } = Msg, _Options,
        #state{ runtime = Runtime, user_context = UCtx } = State) ->
    case Runtime:is_allowed(publish, Topic, Msg, UCtx) of
        true ->
            MsgPub = mqtt_sessions_payload:decode(Msg#{ dup => false }),
            ok = mqtt_sessions_router:publish(State#state.pool, Topic, MsgPub, UCtx);
        false ->
            ok
    end,
    {ok, State};
publish(#{ topic := Topic, qos := 1, dup := Dup, packet_id := PacketId } = Msg, Options,
        #state{ runtime = Runtime, user_context = UCtx, awaiting_rel = WaitRel } = State) ->
    case maps:find(PacketId, WaitRel) of
        {ok, _} when not Dup ->
            % There is a qos 2 level message with the same packet id
            PubAck = #{
                type => puback,
                packet_id => PacketId,
                reason_code => ?MQTT_RC_PACKET_ID_IN_USE
            },
            reply(PubAck, Options, State);
        {ok, {pubrel, RC, _}} when Dup ->
            % There is a qos 2 level message with the same packet id
            % But the received mesage is a duplicate, just ack.
            PubAck = #{
                type => puback,
                packet_id => PacketId,
                reason_code => RC
            },
            reply(PubAck, Options, State);
        error ->
            RC = case Runtime:is_allowed(publish, Topic, Msg, UCtx) of
                true ->
                    MsgPub = mqtt_sessions_payload:decode(Msg#{ dup => false }),
                    ok = mqtt_sessions_router:publish(State#state.pool, Topic, MsgPub, UCtx),
                    ?MQTT_RC_SUCCESS;
                false ->
                    ?MQTT_RC_NOT_AUTHORIZED
            end,
            PubAck = #{
                type => puback,
                packet_id => PacketId,
                reason_code => RC
            },
            State1 = reply(PubAck, Options, State),
            {ok, State1}
    end;
publish(#{ topic := Topic, qos := 2, dup := Dup, packet_id := PacketId } = Msg, Options,
        #state{ runtime = Runtime, user_context = UCtx, awaiting_rel = WaitRel } = State) ->
    case maps:find(PacketId, WaitRel) of
        {ok, _} when not Dup ->
            PubRec = #{
                type => pubrec,
                packet_id => PacketId,
                reason_code => ?MQTT_RC_PACKET_ID_IN_USE
            },
            reply(PubRec, Options, State);
        {ok, {pubrel, RC, _}} when Dup ->
            PubRec = #{
                type => pubrec,
                packet_id => PacketId,
                reason_code => RC
            },
            State1 = reply(PubRec, Options, State),
            {ok, State1};
        error ->
            RC = case Runtime:is_allowed(publish, Topic, Msg, UCtx) of
                true ->
                    MsgPub = mqtt_sessions_payload:decode(Msg#{ dup => false }),
                    ok = mqtt_sessions_router:publish(State#state.pool, Topic, MsgPub, UCtx),
                    ?MQTT_RC_SUCCESS;
                false ->
                    ?MQTT_RC_NOT_AUTHORIZED
            end,
            State1 = if
                RC < 16#80 ->
                    State#state{ awaiting_rel = WaitRel#{ PacketId => {pubrel, RC, mqtt_sessions_timestamp:timestamp()} } };
                true ->
                    State
            end,
            PubRec = #{
                type => pubrec,
                packet_id => PacketId,
                reason_code => RC
            },
            State2 = reply(PubRec, Options, State1),
            {ok, State2}
    end.

%% @doc Handle the pubrel
pubrel(#{ packet_id := PacketId, reason_code := ?MQTT_RC_SUCCESS }, Options, #state{ awaiting_rel = WaitRel } = State) ->
    case maps:find(PacketId, WaitRel) of
        {ok, {pubrel, _RC, _Tm}} ->
            PubComp = #{
                type => pubcomp,
                packet_id => PacketId,
                reason_code => ?MQTT_RC_SUCCESS
            },
            WaitRel1 = maps:remove(PacketId, WaitRel),
            State1 = reply(PubComp, Options, State),
            {ok, State1#state{ awaiting_rel = WaitRel1 }};
        error ->
            PubComp = #{
                type => pubcomp,
                packet_id => PacketId,
                reason_code => ?MQTT_RC_PACKET_ID_NOT_FOUND
            },
            State1 = reply(PubComp, Options, State),
            {ok, State1}
    end;
pubrel(#{ packet_id := PacketId, reason_code := RC }, _Options, #state{ awaiting_rel = WaitRel } = State) ->
    % Error server/client out of sync - remove the wait-rel for this packet_id
    lager:info("PUBREL with reason ~p for packet ~p",
               [ RC, PacketId ]),
    WaitRel1 = maps:remove(PacketId, WaitRel),
    {ok, State#state{ awaiting_rel = WaitRel1 }}.


%% @doc Handle puback for QoS 1 publish messages sent to the client
puback(#{ packet_id := PacketId }, _Options, #state{ awaiting_ack = WaitAck } = State) ->
    WaitAck1 = case maps:find(PacketId, WaitAck) of
        {ok, {_MsgNr, puback, _Msg}} ->
            maps:remove(PacketId, WaitAck);
        {ok, {_MsgNr, Wait, Msg}} ->
            lager:warning("PUBACK for message ~p waiting for ~p. Message: ~p",
                          [ PacketId, Wait, Msg ]),
            maps:remove(PacketId, WaitAck);
        error ->
            WaitAck
    end,
    {ok, State#state{ awaiting_ack = WaitAck1 }}.

%% @doc Handle pubrec for QoS 2 publish messages sent to the client
pubrec(#{ packet_id := PacketId, reason_code := RC }, _Options, #state{ awaiting_ack = WaitAck } = State) when RC >= 16#80 ->
    WaitAck1 = case maps:find(PacketId, WaitAck) of
        {ok, {_MsgNr, pubrec, _Msg}} ->
            maps:remove(PacketId, WaitAck);
        {ok, {_MsgNr, pubcomp, _Msg}} ->
            maps:remove(PacketId, WaitAck);
        {ok, {_MsgNr, Wait, Msg}} ->
            lager:warning("PUBREC for message ~p waiting for ~p. Message: ~p",
                          [ PacketId, Wait, Msg ]),
            maps:remove(PacketId, WaitAck);
        error ->
            WaitAck
    end,
    {ok, State#state{ awaiting_ack = WaitAck1 }};
pubrec(#{ packet_id := PacketId }, Options, #state{ awaiting_ack = WaitAck } = State) ->
    {WaitAck1, RC} = case maps:find(PacketId, WaitAck) of
        {ok, {MsgNr, pubrec, _Msg}} ->
            {WaitAck#{ PacketId => {MsgNr, pubcomp, undefined} }, ?MQTT_RC_SUCCESS};
        {ok, {_MsgNr, pubcomp, _Msg}} ->
            {WaitAck, ?MQTT_RC_SUCCESS};
        {ok, {_MsgNr, Wait, Msg}} ->
            lager:warning("PUBREC for message ~p waiting for ~p. Message: ~p",
                          [ PacketId, Wait, Msg ]),
            {maps:remove(PacketId, WaitAck), ?MQTT_RC_PACKET_ID_NOT_FOUND};
        error ->
            {WaitAck, ?MQTT_RC_PACKET_ID_NOT_FOUND}
    end,
    State1 = State#state{ awaiting_ack = WaitAck1 },
    PubRel = #{
        type => pubrel,
        packet_id => PacketId,
        reason_code => RC
    },
    {ok, reply(PubRel, Options, State1)}.

%% @doc Handle pubcomp for QoS 2 publish messages sent to the client
pubcomp(#{ packet_id := PacketId }, _Options, #state{ awaiting_ack = WaitAck } = State) ->
    WaitAck1 = case maps:find(PacketId, WaitAck) of
        {ok, {_MsgNr, pubcomp, _Msg}} ->
            maps:remove(PacketId, WaitAck);
        {ok, {_MsgNr, Wait, Msg}} ->
            lager:warning("PUBREC for message ~p waiting for ~p. Message: ~p",
                          [ PacketId, Wait, Msg ]),
            maps:remove(PacketId, WaitAck);
        error ->
            WaitAck
    end,
    {ok, State#state{ awaiting_ack = WaitAck1 }}.


%% @doc Handle a subscribe request
subscribe(#{ topics := Topics } = Msg, Options, #state{ runtime = Runtime, user_context = UCtx } = State) ->
    Resp = lists:map(
        fun(#{ topic := TopicFilter0 } = Sub) ->
            TopicFilter = mqtt_sessions:normalize_topic(TopicFilter0),
            case Runtime:is_allowed(subscribe, TopicFilter, Msg, State#state.user_context) of
                true ->
                    QoS = maps:get(qos, Sub, 0),
                    SubOptions = Sub#{
                        qos => QoS,
                        no_local => maps:get(no_local, Sub, false)
                    },
                    SubOptions1 = maps:remove(topic, SubOptions),
                    case mqtt_sessions_router:subscribe(State#state.pool, TopicFilter, self(), self(), SubOptions1, UCtx) of
                        ok -> {ok, QoS};
                        {error, _} -> {error, ?MQTT_RC_ERROR}
                    end;
                false ->
                    {error, ?MQTT_RC_NOT_AUTHORIZED}
            end
        end,
        Topics),
    SubAck = #{
        type => suback,
        packet_id => maps:get(packet_id, Msg, 0),
        acks => Resp
    },
    State1 = reply(SubAck, Options, State),
    {ok, State1}.

%% @doc Handle the unsubscribe request
unsubscribe(#{ topics := Topics } = Msg, Options, State) ->
    Resp = lists:map(
        fun(TopicFilter) ->
            case mqtt_sessions_router:unsubscribe(State#state.pool, TopicFilter, self()) of
                ok -> {ok, found};
                {error, notfound} -> {ok, notfound}
            end
        end,
        Topics),
    UnsubAck = #{
        type => unsuback,
        packet_id => maps:get(packet_id, Msg, 0),
        acks => Resp
    },
    State1 = reply(UnsubAck, Options, State),
    {ok, State1}.


%% @doc Handle a disconnect from the client.
disconnect(#{ reason_code := RC, properties := Props }, _Options, State) ->
    ExpiryInterval = maps:get(session_expiry_interval, Props, State#state.session_expiry_interval),
    {ok, will_disconnected(RC =/= ?MQTT_RC_SUCCESS, ExpiryInterval, State)}.



% ---------------------------------------------------------------------------------------
% --------------------------- relay publish to client -----------------------------------
% ---------------------------------------------------------------------------------------


relay_publish(#{ type := publish, message := Msg } = MqttMsg, State) ->
    QoS = erlang:min( maps:get(qos, Msg, 0), maps:get(qos, MqttMsg, 0) ),
    Msg2 = mqtt_sessions_payload:encode(Msg#{
        qos => QoS,
        dup => false
    }),
    {StateN, MsgN} = case QoS of
        0 ->
            {State, Msg2#{ packet_id => 0 }};
        _ ->
            State1 = #state{ packet_id = PacketId } = inc_packet_id(State),
            State2 = #state{ msg_nr = MsgNr } = inc_msg_nr(State1),
            AckRec = case QoS of
                1 -> puback;
                2 -> pubrec
            end,
            Msg3 = Msg2#{
                packet_id => PacketId
            },
            State3 = State2#state{
                awaiting_ack = (State2#state.awaiting_ack)#{ PacketId => {MsgNr, AckRec, Msg3} }
            },
            {State3, Msg3}
    end,
    reply(MsgN, #{}, StateN).


% ---------------------------------------------------------------------------------------
% ------------------------------- queue functions ---------------------------------------
% ---------------------------------------------------------------------------------------

cleanupPending(#state{ pending = Pending } = State) ->
    L1 = lists:filter(
            fun
                (#{ type := publish, qos := 0 }) -> true;
                (_) -> false
            end,
            queue:to_list(Pending)),
    State#state{ pending = queue:from_list(L1) }.

resendUnacknowledged(#state{ awaiting_ack = AwaitAck } = State) ->
    Msgs = maps:fold(
        fun
            (_PacketId, {MsgNr, pubrec, Msg}, Acc) ->
                [ {MsgNr, Msg#{ dup => true }} | Acc ];
            (PacketId, {MsgNr, pubcomp, _Msg}, Acc) ->
                PubComp = #{
                    type => pubrec,
                    packet_id => PacketId
                },
                [ {MsgNr, PubComp} | Acc ];
            (_PacketId, {MsgNr, suback, Msg}, Acc) ->
                [ {MsgNr, Msg} | Acc ];
            (_PacketId, {MsgNr, unsuback, Msg}, Acc) ->
                [ {MsgNr, Msg} | Acc ];
            (_PacketId, _, Acc) ->
                Acc
        end,
        [],
        AwaitAck),
    lists:foldl(
        fun({_Nr, Msg}, StateAcc) ->
            reply(Msg, #{}, StateAcc)
        end,
        State,
        lists:sort(Msgs)).


% ---------------------------------------------------------------------------------------
% -------------------------------- misc functions ---------------------------------------
% ---------------------------------------------------------------------------------------


%% @doc Signal the will-watchdog that the session is disconnected. It will start
%%      a timer for automatic expiry of this session process.
will_disconnected(IsWill, Expiry, State) ->
    mqtt_sessions_will:disconnected(State#state.will_pid, IsWill, Expiry),
    send_transport({reply, disconnect}, State),
    cleanup_state_disconnected(State).

%% @doc Called when the connection disconnects or crashes/stops
do_disconnected(#state{ will_pid = WillPid } = State) ->
    mqtt_sessions_will:disconnected(WillPid),
    cleanup_state_disconnected(State).

%% @todo Cleanup pending messages and awaiting states.
cleanup_state_disconnected(State) ->
    cleanupPending(State#state{
        pending_connack = undefined,
        is_connected = false,
        transport = undefined,
        awaiting_rel = #{}
    }).


%% @doc Send a connack to the remote, ping the will-watchdog that we connected
reply_connack(#{ type := connack } = ConnAck, Options, State) ->
    AckProps = maps:get(properties, ConnAck, #{}),
    ConnAck1 = ConnAck#{
        session_present => State#state.is_session_present,
        properties => AckProps#{
            session_expiry_interval => State#state.session_expiry_interval,
            server_keep_alive => State#state.keep_alive,
            assigned_client_identifier => State#state.client_id,
            subscription_identifier_available => false,
            shared_subscription_available => false,
            <<"cotonic-routing-id">> => State#state.routing_id
        }
    },
    reply(ConnAck1, Options, State).


%% @doc Check the connect packet, extract the will as a map for the will-watchdog.
extract_will(#{ type := connect, will_flag := false }) ->
    #{};
extract_will(#{ type := connect, will_flag := true, properties := Props } = Msg) ->
    #{
        expiry_interval => maps:get(will_expiry_interval, Props, 0),
        topic => maps:get(will_topic, Msg),
        payload => maps:get(will_payload, Msg, <<>>),
        properties => maps:get(will_properties, Msg, #{}),
        qos => maps:get(will_qos, Msg, 0),
        retain => maps:get(will_retain, Msg, false)
    }.

%% @doc Send (and maybe queue) a message back via the current transport.
reply(Msg, Options, State) ->
    State1 = select_transport(Options, State),
    reply(Msg, State1).

reply(undefined, State) ->
    State;
reply(Msg, #state{ transport = undefined } = State) ->
    queue(Msg, State);
reply(Msg, #state{ transport = Transport } = State) ->
    Transport ! {reply, encode(Msg)},
    State.

send_transport(_Msg, #state{ transport = undefined }) ->
    ok;
send_transport(Msg, #state{ transport = Pid }) when is_pid(Pid) ->
    Pid ! Msg.

%% @doc Queue a message, extract, type, message expiry, and QoS
queue(#{ type := connack } = Msg, State) ->
    State#state{ pending_connack = Msg };
queue(#{ type := auth } = Msg, State) ->
    State#state{ pending_connack = Msg };
queue(Msg, State) ->
    queue_1(Msg, inc_msg_nr(State)).

queue_1(#{ type := Type } = Msg, #state{ msg_nr = MsgNr, pending = Pending } = State) ->
    Props = maps:get(properties, Msg, #{}),
    Now = mqtt_sessions_timestamp:timestamp(),
    Item = #queued{
        msg_nr = MsgNr,
        type = Type,
        queued = Now,
        expiry = Now + maps:get(message_expiry_interval, Props, ?DEFAULT_MESSAGE_EXPIRY),
        qos = maps:get(qos, Msg, 1),
        message = Msg
    },
    State#state{ pending = queue:in(Item, Pending) }.


-spec encode( mqtt_packet_map:mqtt_packet() | list( mqtt_packet_map:mqtt_packet() )) -> binary().
encode(Msg) when is_map(Msg) ->
    {ok, Bin} = mqtt_packet_map:encode(Msg),
    Bin;
encode(Ms) when is_list(Ms) ->
    iolist_to_binary([ encode(M) || M <- Ms ]).

%% @doc Select the transport for sending a message (if any).
%%      If there was another transport connected, then disconnect that one.
select_transport(Options, #state{ transport = Transport } = State) ->
    case maps:get(transport, Options, Transport) of
        Transport ->
            State;
        Pid when is_pid(Pid), is_pid(Transport) ->
            Transport ! {reply, disconnect},
            erlang:monitor(process, Pid),
            mqtt_sessions_will:reconnected(State#state.will_pid),
            start_keep_alive(State#state{ transport = Pid });
        Pid when is_pid(Pid), Transport =:= undefined ->
            erlang:monitor(process, Pid),
            mqtt_sessions_will:reconnected(State#state.will_pid),
            start_keep_alive(State#state{ transport = Pid });
        undefined ->
            State
    end.

start_keep_alive(#state{ keep_alive = 0 } = State) ->
    State;
start_keep_alive(#state{ keep_alive = N, transport = Pid } = State) ->
    erlang:send_after(N * 500, self(), {keep_alive, Pid}),
    State#state{ keep_alive_counter = 3 }.

%% @doc Increment the message number, this number is used for order of resent messages
inc_msg_nr(#state{ msg_nr = Nr } = State) ->
    State#state{ msg_nr = Nr + 1 }.

%% @doc Fetch a packet id that is not yet used.
inc_packet_id(#state{ packet_id = PacketId, awaiting_ack = Acks } = State) ->
    PacketId1 = case PacketId >= ?MAX_PACKET_ID of
        true -> 1;
        false -> PacketId + 1
    end,
    State1 = State#state{ packet_id = PacketId1 },
    case maps:is_key(PacketId1, Acks) of
        true -> inc_packet_id(State1);
        false -> State1
    end.
