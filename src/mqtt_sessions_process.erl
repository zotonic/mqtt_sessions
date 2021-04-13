%% @doc Process handling one single MQTT session.
%%      Transports attaches and detaches from this session.
%% @author Marc Worrell <marc@worrell.nl>
%% @copyright 2018-2020 Marc Worrell

%% Copyright 2018-2020 Marc Worrell
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
%% TODO: Limit incoming_data buffer size


% MQTTv5     spec http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
% MQTTv3.1.1 spec http://docs.oasis-open.org/mqtt/mqtt/v5.0/cos01/mqtt-v5.0-cos01.html


-module(mqtt_sessions_process).

-behaviour(gen_server).

-export([
    get_user_context/1,
    set_user_context/2,
    update_user_context/2,

    get_transport/1,
    kill/1,
    incoming_connect/3,
    incoming_data/2,
    fetch_queue/1,
    start_link/3
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
-define(KEEP_ALIVE_DEFAULT, 30).        % Default keep alive in seconds
-define(SESSION_EXPIRY, 600).           % Default session expiration
-define(SESSION_EXPIRY_DEFAULT, 3600).  % Maximum allowed session expiration
-define(MESSAGE_EXPIRY_DEFAULT, 3600).

-define(KILL_TIMEOUT, 5000).

-type packet_id() :: 0..?MAX_PACKET_ID.

-record(state, {
    protocol_version :: mqtt_packet_map:mqtt_version(),
    pool :: atom(),
    runtime :: atom(),
    client_id :: binary(),
    routing_id :: binary(),
    user_context :: term(),
    transport = undefined :: pid() | function() | undefined,
    connection_pid = undefined :: pid() | undefined,
    is_session_present = false :: boolean(),
    pending_connack = undefined :: term(),
    pending :: queue:queue(),
    packet_id = 1 :: packet_id(),
    send_quota = ?RECEIVE_MAXIMUM :: non_neg_integer(),
    awaiting_ack = #{} :: map(),  % Initiated by server
    awaiting_rel = #{} :: map(),  % Initiated by client
    will = undefined :: undefined | map(),
    will_pid = undefined :: undefined | pid(),
    msg_nr = 0 :: non_neg_integer(),
    keep_alive = ?KEEP_ALIVE_DEFAULT :: non_neg_integer(),
    keep_alive_counter = 3 :: integer(),
    keep_alive_ref :: undefined | reference(),
    session_expiry_interval = ?SESSION_EXPIRY :: non_neg_integer(),

    % Number of times we had a succesful connect to this session
    connect_count = 0 :: non_neg_integer(),

    % Buffering incoming data for a complete packet
    incoming_data = <<>> :: binary(),

    % Tracking publish jobs
    publish_jobs = #{} :: map()
}).

-record(queued, {
    type :: atom(),
    msg_nr :: pos_integer(),
    packet_id = undefined :: undefined | non_neg_integer(),
    queued :: non_neg_integer(),
    expiry :: non_neg_integer(),
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

-spec get_transport( pid() ) -> {ok, pid()} | {error, notransport | noproc}.
get_transport(Pid) ->
    try
        gen_server:call(Pid, get_transport, infinity)
    catch
        exit:{noproc, _} ->
            {error, noproc}
    end.

-spec kill( pid() ) -> ok.
kill(Pid) when is_pid(Pid) ->
    MRef = monitor(process, Pid),
    gen_server:cast(Pid, kill),
    receive
        {'DOWN', MRef, process, Pid, _Reason} ->
            ok
    after ?KILL_TIMEOUT ->
        erlang:exit(Pid, kill),
        receive
            {'DOWN', MRef, process, Pid, _Reason} ->
                ok
        end
    end.


-spec incoming_connect(pid(), mqtt_packet_map:mqtt_packet(), mqtt_sessions:msg_options()) -> ok.
incoming_connect(Pid, Msg, Options) when is_map(Options) ->
    gen_server:cast(Pid, {incoming_connect, Msg, Options}).

-spec incoming_data(pid(), binary()) -> ok | {error, wrong_connection | mqtt_packet_map:decode_error()}. 
incoming_data(Pid, Data) ->
    gen_server:call(Pid, {incoming_data, Data, self()}).

-spec fetch_queue(pid()) -> {ok, list( map() | binary() )}.
fetch_queue( Pid ) ->
    gen_server:call(Pid, fetch_queue, infinity).

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
    KeepAliveRef = erlang:make_ref(),
    erlang:send_after(?KEEP_ALIVE_DEFAULT * 500, self(), {keep_alive, KeepAliveRef}),
    {ok, #state{
        pool = Pool,
        runtime = Runtime,
        user_context = Runtime:new_user_context(Pool, ClientId, SessionOptions1),
        client_id = ClientId,
        routing_id = RoutingId,
        pending = queue:new(),
        will_pid = WillPid,
        keep_alive = ?KEEP_ALIVE_DEFAULT,
        keep_alive_counter = 3,
        keep_alive_ref = KeepAliveRef
    }}.

handle_call(fetch_queue, _From, #state{ pending_connack = undefined } = State) ->
    Qs = [ Msg || #queued{ message = Msg } <- queue:to_list(State#state.pending) ],
    {reply, {ok, encode(State#state.protocol_version, Qs)}, State#state{ pending = queue:new() }};
handle_call(fetch_queue, _From, #state{ pending_connack = ConnAck } = State) ->
    {reply, {ok, encode(State#state.protocol_version, ConnAck)}, State#state{ pending_connack = undefined }};

handle_call(get_user_context, _From, #state{ user_context = UserContext } = State) ->
    {reply, {ok, UserContext}, State};
handle_call({set_user_context, UserContext}, _From, State) ->
    {reply, ok, State#state{ user_context = UserContext }};
handle_call({update_user_context, Fun}, _From, #state{ user_context = UserContext} = State) ->
    {reply, ok, State#state{ user_context = Fun(UserContext) }};

handle_call(get_transport, _From, #state{ transport = undefined } = State) ->
    {reply, {error, notransport}, State};
handle_call(get_transport, _From, #state{ transport = TransportPid } = State) ->
    {reply, {ok, TransportPid}, State};

handle_call({incoming_data, NewData, ConnectionPid}, _From, #state{ incoming_data = Data, connection_pid = ConnectionPid } = State) ->
    Data1 = << Data/binary, NewData/binary >>,
    case handle_incoming_data(Data1, State) of
        {ok, {Rest, StateRest}} ->
            {reply, ok, StateRest#state{ keep_alive_counter = 3, incoming_data = Rest }};
        {error, Reason} when is_atom(Reason) ->
            % illegal packet, disconnect and wait for new connection
            lager:info("Error decoding incoming data: ~p", [ Reason ]),
            {reply, {error, Reason}, force_disconnect(State)}
    end;
handle_call({incoming_data, _NewData, ConnectionPid}, _From, State) ->
    lager:debug("MQTT session incoming data from ~p, expected from ~p", [ConnectionPid, State#state.connection_pid]),
    {reply, {error, wrong_connection}, State};
handle_call(Cmd, _From, State) ->
    {stop, {unknown_cmd, Cmd}, State}.

handle_cast({incoming_connect, Msg, Options}, State) ->
    case handle_incoming_with_context(Msg, Options, State) of
        {ok, State1} ->
            {noreply, State1#state{ keep_alive_counter = 3, incoming_data = <<>> }};
        {error, _} ->
            {noreply, force_disconnect(State)}
    end;
handle_cast(kill, State) ->
    {stop, shutdown, State}.

handle_info({mqtt_msg, #{ type := publish } = MqttMsg}, State) ->
    State1 = relay_publish(MqttMsg, State),
    {noreply, State1};

handle_info({keep_alive, Ref}, #state{ keep_alive_counter = 0, keep_alive_ref = Ref } = State) ->
    lager:debug("MQTT past keep_alive, disconnecting transport"),
    {noreply, force_disconnect(State)};
handle_info({keep_alive, Ref}, #state{ keep_alive_counter = N, keep_alive_ref = Ref } = State) ->
    erlang:send_after(State#state.keep_alive * 500, self(), {keep_alive, Ref}),
    {noreply, State#state{ keep_alive_counter = erlang:max(N-1, 0) }};
handle_info({keep_alive, _Ref}, State) ->
    {noreply, State};

handle_info({publish_job, undefined}, State) ->
    {noreply, State};
handle_info({publish_job, JobPid}, #state{ publish_jobs = Jobs } = State) when is_pid(JobPid) ->
    State1 = case erlang:is_process_alive(JobPid) of
        true ->
            State#state{ publish_jobs = Jobs#{ JobPid => erlang:monitor(process, JobPid) } };
        false ->
            State
    end,
    {noreply, State1};

handle_info({'DOWN', _Mref, process, Pid, _Reason}, #state{ connection_pid = Pid } = State) ->
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


handle_incoming_data(<<>>, State) ->
    {ok, {<<>>, State}};
handle_incoming_data(Data, State) ->
    case mqtt_packet_map:decode(State#state.protocol_version, Data) of
        {ok, {Msg, Rest}} ->
            case handle_incoming_with_context(Msg, #{}, State) of
                {ok, State1} ->
                    handle_incoming_data(Rest, State1);
                {error, _Reason} = Error ->
                    Error
            end;
        {error, incomplete_packet} ->
            % @todo Limit buffer size, disconnect if over max size
            {ok, {Data, State}};
        {error, _Reason} = Error ->
            Error
    end.

handle_incoming_with_context(Msg, Options, #state{ runtime = Runtime, user_context = UserContext } = State) ->
    case Runtime:is_valid_message(Msg, Options, UserContext) of
        true ->
            handle_incoming(Msg, Options, State);
        false ->
            % We don't want this here, drop connection
            {error, invalid_message}
    end.

handle_incoming(#{ type := connect } = Msg, Options, #state{ is_session_present = false } = State) ->
    % First time connect, accept.
    packet_connect(Msg, Options, State);
handle_incoming(#{ type := connect } = Msg, Options, State) ->
    % A client reopens a connection. Check if the credentials match with the current
    % session credentials (otherwise someone else might steal this session).
    packet_connect(Msg, Options, State);
handle_incoming(#{ type := auth } = Msg, _Options, State) ->
    packet_connect_auth(Msg, State);
handle_incoming(#{ type := Type }, _Options, #state{ connection_pid = undefined } = State) ->
    lager:info("Dropping packet for MQTT session ~p ~s (~p) for receiving ~p when not connected.",
               [State#state.pool, State#state.client_id, self(), Type]),
    {error, not_connected};
handle_incoming(#{ type := Type }, _Options, #state{ is_session_present = false } = State) ->
    % Only AUTH and CONNECT before the CONNACK
    lager:info("Killing MQTT session ~p ~s (~p) for receiving ~p when no session started.",
               [State#state.pool, State#state.client_id, self(), Type]),
    {stop, State};
handle_incoming(#{ type := publish } = Msg, _Options, State) ->
    packet_publish(Msg, State);

% PUBREL is for publish messages sent by the client
handle_incoming(#{ type := pubrel } = Msg, _Options, State) ->
    packet_pubrel(Msg, State);

% PUBREC, PUBACK, PUBCOMP is for publish messages sent by us to the client
handle_incoming(#{ type := pubrec } = Msg, _Options, State) ->
    packet_pubrec(Msg, State);
handle_incoming(#{ type := pubcomp } = Msg, _Options, State) ->
    packet_pubcomp(Msg, State);
handle_incoming(#{ type := puback } = Msg, _Options, State) ->
    packet_puback(Msg, State);

handle_incoming(#{ type := subscribe } = Msg, _Options, State) ->
    packet_subscribe(Msg, State);
handle_incoming(#{ type := unsubscribe } = Msg, _Options, State) ->
    packet_unsubscribe(Msg, State);

handle_incoming(#{ type := pingreq }, _Options, State) ->
    State1 = reply(#{ type => pingresp }, State),
    {ok, State1};
handle_incoming(#{ type := pingresp }, _Options, State) ->
    {ok, State};

handle_incoming(#{ type := disconnect } = Msg, _Options, State) ->
    packet_disconnect(Msg, State);

handle_incoming(#{ type := Type }, _Options, State) ->
    lager:info("MQTT dropping unhandled packet with type ~p", [Type]),
    {ok, State}.

% ---------------------------------------------------------------------------------------
% --------------------------- message type functions ------------------------------------
% ---------------------------------------------------------------------------------------


%% @doc Handle the connect message. Either this is a re-connect or the first connect.
packet_connect(#{ protocol_version := V, protocol_name := <<"MQTT">> }, Options, #state{ protocol_version = PV } = State)
    when is_integer(PV), V =/= PV ->
    % Do not change protocol versions for an existing session
    ConnAck = #{
        type => connack,
        reason_code => ?MQTT_RC_NOT_AUTHORIZED
    },
    _ = reply(ConnAck, set_connection(Options, State)),
    {error, protocol_version_changed};
packet_connect(#{ protocol_version := 5, protocol_name := <<"MQTT">>, properties := Props } = Msg, Options, State) ->
    % MQTT v5
    ExpiryInterval = case maps:get(session_expiry_interval, Props, none) of
        none -> ?SESSION_EXPIRY_DEFAULT;
        EI -> EI
    end,
    KeepAlive = maps:get(keep_alive, Msg, ?KEEP_ALIVE_DEFAULT),
    StateIfAccept = State#state{
        protocol_version = 5,
        will = extract_will(Msg),
        session_expiry_interval = ExpiryInterval,
        keep_alive = KeepAlive,
        incoming_data = <<>>
    },
    StateIfAccept1 = set_connection(Options, StateIfAccept),
    handle_connect_auth(Msg, Options, StateIfAccept1, State);
packet_connect(#{ protocol_version := 4, protocol_name := <<"MQTT">> } = Msg, Options, State) ->
    % MQTT v3.1.1
    KeepAlive = maps:get(keep_alive, Msg, ?KEEP_ALIVE_DEFAULT),
    StateIfAccept = State#state{
        protocol_version = 4,
        will = extract_will(Msg),
        session_expiry_interval = KeepAlive * 3,
        keep_alive = KeepAlive,
        incoming_data = <<>>
    },
    StateIfAccept1 = set_connection(Options, StateIfAccept),
    handle_connect_auth(Msg, Options, StateIfAccept1, State);
packet_connect(_ConnectMsg, Options, State) ->
    ConnAck = #{
        type => connack,
        reason_code => ?MQTT_RC_PROTOCOL_VERSION
    },
    _ = reply(ConnAck, set_connection(Options, State)),
    {error, protocol_version}.

packet_connect_auth(Msg, #state{ runtime = Runtime, user_context = UserContext } = State) ->
    handle_connect_auth_1(Runtime:reauth(Msg, UserContext), Msg, State, State).

handle_connect_auth(Msg, Options, StateIfAccept, #state{ runtime = Runtime, is_session_present = IsSessionPresent, user_context = UserContext } = State) ->
    handle_connect_auth_1(Runtime:connect(Msg, IsSessionPresent, Options, UserContext), Msg, StateIfAccept, State).

%% @doc Accept the new connection with the given ConnAck or Auth message.
%%      If an Auth message is sent then we need further authenticaion handshakes.
%%      Only after a succesful connack we will set the is_session_present flag.
handle_connect_auth_1({ok, #{ type := connack, reason_code := ?MQTT_RC_SUCCESS } = ConnAck, UserContext1},
        #{ clean_start := CleanStart }, StateIfAccept, _State) ->
    StateCleaned = maybe_clean_start(CleanStart, StateIfAccept),
    State1 = StateCleaned#state{
        user_context = UserContext1,
        is_session_present = true,
        will = undefined
    },
    State2 = reply_connack(ConnAck, State1),
    mqtt_sessions_will:connected(State2#state.will_pid, StateIfAccept#state.will,
                                 State2#state.session_expiry_interval, State2#state.user_context),
    State3 = resend_unacknowledged( cleanup_pending(State2) ),
    {ok, State3};
handle_connect_auth_1({ok, #{ type := connack, reason_code := ReasonCode } = ConnAck, _UserContext1}, _Msg, StateIfAccept, _State) ->
    _ = reply_connack(ConnAck, StateIfAccept),
    lager:debug("MQTT connect/auth refused (~p): ~p", [ReasonCode, ConnAck]),
    {error, connection_refused};
handle_connect_auth_1({ok, #{ type := auth } = Auth, UserContext1}, _Msg, StateIfAccept, _State) ->
    State1 = StateIfAccept#state{
        user_context = UserContext1
    },
    State2 = reply(Auth, State1),
    mqtt_sessions_will:connected(State2#state.will_pid, undefined,
                                 State2#state.session_expiry_interval, State2#state.user_context),
    {ok, State2};
handle_connect_auth_1({error, Reason}, Msg, _StateIfAccept, _State) ->
    lager:info("MQTT connect/auth refused (~p): ~p", [Reason, Msg]),
    {error, connection_refused}.


%% @doc Drop all current subscriptions and pending messages on a clean start
maybe_clean_start(false, State) ->
    State;
maybe_clean_start(true, #state{ pool = Pool } = State) ->
    mqtt_sessions_router:unsubscribe_pid(Pool, self()),
    State#state{ pending = queue:new() }.


%% @doc Handle a publish request
packet_publish(#{ topic := Topic, qos := 0 } = Msg,
        #state{ runtime = Runtime, user_context = UCtx, client_id = ClientId } = State) ->
    case Topic of
        [ <<"$client">>, ClientId | Rest ] ->
            MsgPub = mqtt_sessions_payload:decode(Msg#{ dup => false }),
            {ok, UCtx1} = Runtime:control_message(Rest, MsgPub, UCtx),
            {ok, State#state{ user_context = UCtx1 }};
        _ ->
            case Runtime:is_allowed(publish, Topic, Msg, UCtx) of
                true ->
                    MsgPub = mqtt_sessions_payload:decode(Msg#{ dup => false }),
                    {ok, JobPid} = mqtt_sessions_router:publish(State#state.pool, Topic, MsgPub, UCtx),
                    self() ! {publish_job, JobPid},
                    {ok, State};
                false ->
                    {ok, State}
            end
    end;
packet_publish(#{ topic := Topic, qos := 1, dup := Dup, packet_id := PacketId } = Msg,
        #state{ runtime = Runtime, user_context = UCtx, awaiting_rel = WaitRel } = State) ->
    case maps:find(PacketId, WaitRel) of
        {ok, _} when not Dup ->
            % There is a qos 2 level message with the same packet id
            PubAck = #{
                type => puback,
                packet_id => PacketId,
                reason_code => ?MQTT_RC_PACKET_ID_IN_USE
            },
            reply(PubAck, State);
        {ok, {pubrel, RC, _}} when Dup ->
            % There is a qos 2 level message with the same packet id
            % But the received mesage is a duplicate, just ack.
            PubAck = #{
                type => puback,
                packet_id => PacketId,
                reason_code => RC
            },
            reply(PubAck, State);
        error ->
            RC = case Runtime:is_allowed(publish, Topic, Msg, UCtx) of
                true ->
                    MsgPub = mqtt_sessions_payload:decode(Msg#{ dup => false }),
                    {ok, JobPid} = mqtt_sessions_router:publish(State#state.pool, Topic, MsgPub, UCtx),
                    self() ! {publish_job, JobPid},
                    ?MQTT_RC_SUCCESS;
                false ->
                    ?MQTT_RC_NOT_AUTHORIZED
            end,
            PubAck = #{
                type => puback,
                packet_id => PacketId,
                reason_code => RC
            },
            State1 = reply(PubAck, State),
            {ok, State1}
    end;
packet_publish(#{ topic := Topic, qos := 2, dup := Dup, packet_id := PacketId } = Msg,
        #state{ runtime = Runtime, user_context = UCtx, awaiting_rel = WaitRel } = State) ->
    case maps:find(PacketId, WaitRel) of
        {ok, _} when not Dup ->
            PubRec = #{
                type => pubrec,
                packet_id => PacketId,
                reason_code => ?MQTT_RC_PACKET_ID_IN_USE
            },
            reply(PubRec, State);
        {ok, {pubrel, RC, _}} when Dup ->
            PubRec = #{
                type => pubrec,
                packet_id => PacketId,
                reason_code => RC
            },
            State1 = reply(PubRec, State),
            {ok, State1};
        error ->
            RC = case Runtime:is_allowed(publish, Topic, Msg, UCtx) of
                true ->
                    MsgPub = mqtt_sessions_payload:decode(Msg#{ dup => false }),
                    {ok, JobPid} = mqtt_sessions_router:publish(State#state.pool, Topic, MsgPub, UCtx),
                    self() ! {publish_job, JobPid},
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
            State2 = reply(PubRec, State1),
            {ok, State2}
    end.

%% @doc Handle the pubrel
packet_pubrel(#{ packet_id := PacketId, reason_code := ?MQTT_RC_SUCCESS }, #state{ awaiting_rel = WaitRel } = State) ->
    case maps:find(PacketId, WaitRel) of
        {ok, {pubrel, _RC, _Tm}} ->
            PubComp = #{
                type => pubcomp,
                packet_id => PacketId,
                reason_code => ?MQTT_RC_SUCCESS
            },
            WaitRel1 = maps:remove(PacketId, WaitRel),
            State1 = reply(PubComp, State),
            {ok, State1#state{ awaiting_rel = WaitRel1 }};
        error ->
            PubComp = #{
                type => pubcomp,
                packet_id => PacketId,
                reason_code => ?MQTT_RC_PACKET_ID_NOT_FOUND
            },
            State1 = reply(PubComp, State),
            {ok, State1}
    end;
packet_pubrel(#{ packet_id := PacketId, reason_code := RC }, #state{ awaiting_rel = WaitRel } = State) ->
    % Error server/client out of sync - remove the wait-rel for this packet_id
    lager:info("PUBREL with reason ~p for packet ~p",
               [ RC, PacketId ]),
    WaitRel1 = maps:remove(PacketId, WaitRel),
    {ok, State#state{ awaiting_rel = WaitRel1 }}.


%% @doc Handle puback for QoS 1 publish messages sent to the client
packet_puback(#{ packet_id := PacketId }, #state{ awaiting_ack = WaitAck } = State) ->
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
packet_pubrec(#{ packet_id := PacketId, reason_code := RC }, #state{ awaiting_ack = WaitAck } = State) when RC >= 16#80 ->
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
packet_pubrec(#{ packet_id := PacketId }, #state{ awaiting_ack = WaitAck } = State) ->
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
    {ok, reply(PubRel, State1)}.

%% @doc Handle pubcomp for QoS 2 publish messages sent to the client
packet_pubcomp(#{ packet_id := PacketId }, #state{ awaiting_ack = WaitAck } = State) ->
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
packet_subscribe(#{ topics := Topics } = Msg, #state{ runtime = Runtime, user_context = UCtx } = State) ->
    Resp = lists:map(
        fun(#{ topic := TopicFilter0 } = Sub) ->
            case mqtt_packet_map_topic:validate_topic(TopicFilter0) of
                {ok, TopicFilter} ->
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
                    end;
                {error, _} ->
                    {error, ?MQTT_RC_TOPIC_FILTER_INVALID}
            end
        end,
        Topics),
    SubAck = #{
        type => suback,
        packet_id => maps:get(packet_id, Msg, 0),
        acks => Resp
    },
    State1 = reply(SubAck, State),
    {ok, State1}.

%% @doc Handle the unsubscribe request
packet_unsubscribe(#{ topics := Topics } = Msg, State) ->
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
    State1 = reply(UnsubAck, State),
    {ok, State1}.


%% @doc Handle a disconnect from the client.
packet_disconnect(#{ reason_code := RC, properties := Props },
                  #state{ will_pid = WillPid, session_expiry_interval = SessionExpiryInterval } = State) ->
    NewExpiryInterval = case SessionExpiryInterval of
        0 ->
            % TODO: If the props.session_expiry_interval > 0 then send disconnect with MQTT_RC_PROTOCOL_ERROR
            SessionExpiryInterval;
        _ ->
            maps:get(session_expiry_interval, Props, SessionExpiryInterval)
    end,
    IsSendWill = (RC =/= ?MQTT_RC_SUCCESS),
    mqtt_sessions_will:disconnected(WillPid, IsSendWill, NewExpiryInterval),
    State1 = force_disconnect(State),
    case NewExpiryInterval of
        0 -> gen_server:cast(self(), kill);
        _ -> ok
    end,
    {ok, State1}.


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
    reply(MsgN, StateN).


% ---------------------------------------------------------------------------------------
% ------------------------------- queue functions ---------------------------------------
% ---------------------------------------------------------------------------------------

cleanup_pending(#state{ pending = Pending } = State) ->
    L1 = lists:filter(
            fun
                (#{ type := publish, qos := 0 }) -> true;
                (_) -> false
            end,
            queue:to_list(Pending)),
    State#state{ pending = queue:from_list(L1) }.

resend_unacknowledged(#state{ awaiting_ack = AwaitAck } = State) ->
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
            reply(Msg, StateAcc)
        end,
        State,
        lists:sort(Msgs)).


% ---------------------------------------------------------------------------------------
% -------------------------------- misc functions ---------------------------------------
% ---------------------------------------------------------------------------------------


%% @doc Called when the connection disconnects or crashes/stops
do_disconnected(#state{ will_pid = WillPid } = State) ->
    mqtt_sessions_will:disconnected(WillPid),
    cleanup_state_disconnected(State).

%% @todo Cleanup pending messages and awaiting states.
cleanup_state_disconnected(State) ->
    cleanup_pending(State#state{
        pending_connack = undefined,
        connection_pid = undefined,
        transport = undefined,
        awaiting_rel = #{}
    }).


%% @doc Send a connack to the remote, ping the will-watchdog that we connected
reply_connack(#{ type := connack, reason_code := ?MQTT_RC_SUCCESS } = ConnAck, State) ->
    AckProps = maps:get(properties, ConnAck, #{}),
    ConnAck1 = ConnAck#{
        properties => AckProps#{
            session_expiry_interval => State#state.session_expiry_interval,
            server_keep_alive => State#state.keep_alive,
            assigned_client_identifier => State#state.client_id,
            subscription_identifier_available => false,
            shared_subscription_available => false,
            <<"cotonic-routing-id">> => State#state.routing_id
        }
    },
    reply(ConnAck1, State);
reply_connack(#{ type := connack } = ConnAck, State) ->
    reply(ConnAck, State).


%% @doc Check the connect packet, extract the will as a map for the will-watchdog.
extract_will(#{ type := connect, will_flag := false }) ->
    #{};
extract_will(#{ protocol_version := 4, type := connect, will_flag := true } = Msg) ->
    #{
        expiry_interval => 0,
        topic => maps:get(will_topic, Msg),
        payload => maps:get(will_payload, Msg, <<>>),
        properties => maps:get(will_properties, Msg, #{}),
        qos => maps:get(will_qos, Msg, 0),
        retain => maps:get(will_retain, Msg, false)
    };
extract_will(#{ type := connect, will_flag := true, properties := Props } = Msg) ->
    #{
        expiry_interval => maps:get(will_expiry_interval, Props, 0),
        topic => maps:get(will_topic, Msg),
        payload => maps:get(will_payload, Msg, <<>>),
        properties => maps:get(will_properties, Msg, #{}),
        qos => maps:get(will_qos, Msg, 0),
        retain => maps:get(will_retain, Msg, false)
    }.

force_disconnect(State) ->
    State1 = disconnect_transport(State),
    State2 = cleanup_state_disconnected(State1),
    case State2#state.is_session_present of
        false ->
            gen_server:cast(self(), kill);
        true ->
            ok
    end,
    State2.

disconnect_transport(#state{ transport = undefined } = State) ->
    State;
disconnect_transport(#state{ transport = Transport } = State) when is_pid(Transport) ->
    Transport ! {mqtt_transport, self(), disconnect},
    State#state{ transport = undefined };
disconnect_transport(#state{ transport = Transport } = State) when is_function(Transport) ->
    Transport(disconnect),
    State#state{ transport = undefined }.

reply(undefined, State) ->
    State;
reply(Msg, #state{ transport = undefined } = State) ->
    queue(Msg, State);
reply(Msg, State) ->
    case send_transport(Msg, State) of
        ok ->
            State;
        {error, _} ->
            queue(Msg, State#state{ transport = undefined })
    end.

send_transport(_Msg, #state{ transport = undefined }) ->
    ok;
send_transport(Msg, #state{ protocol_version = PV } = State) when is_map(Msg) ->
    send_transport(encode(PV, Msg), State);
send_transport(Msg, #state{ transport = Pid }) when is_pid(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            Pid ! {mqtt_transport, self(), Msg},
            ok;
        false ->
            ok
    end;
send_transport(Msg, #state{ transport = Fun }) when is_function(Fun) ->
    Fun(Msg).


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
        expiry = Now + maps:get(message_expiry_interval, Props, ?MESSAGE_EXPIRY_DEFAULT),
        qos = maps:get(qos, Msg, 1),
        message = Msg
    },
    State#state{ pending = queue:in(Item, Pending) }.


-spec encode( mqtt_packet_map:mqtt_version(), mqtt_packet_map:mqtt_packet() | list( mqtt_packet_map:mqtt_packet() )) -> binary().
encode(ProtocolVersion, Msg) when is_map(Msg) ->
    {ok, Bin} = mqtt_packet_map:encode(ProtocolVersion, Msg),
    Bin;
encode(ProtocolVersion, Ms) when is_list(Ms) ->
    iolist_to_binary([ encode(ProtocolVersion, M) || M <- Ms ]).


%% @doc Set the new connection, disconnect existing transport.
set_connection(#{ connection_pid := ConnectionPid, transport := Transport }, State) ->
    case State#state.connection_pid of
        ConnectionPid ->
            State;
        undefined ->
            set_connection_1(ConnectionPid, Transport, State);
        OldConnectionPid ->
            OldConnectionPid ! {mqtt_transport, self(), disconnect},
            set_connection_1(ConnectionPid, Transport, State)
    end.

set_connection_1(ConnectionPid, Transport, State) ->
    erlang:monitor(process, ConnectionPid),
    start_keep_alive(State#state{ connection_pid = ConnectionPid, transport = Transport }).


start_keep_alive(#state{ keep_alive = 0 } = State) ->
    State;
start_keep_alive(#state{ keep_alive = N } = State) ->
    Ref = erlang:make_ref(),
    erlang:send_after(N * 500, self(), {keep_alive, Ref}),
    State#state{ keep_alive_counter = 3, keep_alive_ref = Ref }.

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
