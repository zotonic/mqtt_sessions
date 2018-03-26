%% @doc Process handling one single MQTT session.
%%      A single session can have multiple transports attached.

-module(mqtt_sessions_process).

-behaviour(gen_server).

-export([
    kill/1,
    incoming_message/3,
    fetch_queue/1,
    start_link/2,
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

-type packet_id() :: 0..65535.

-record(state, {
    pool :: atom(),
    runtime :: atom(),
    client_id :: binary(),
    is_connected = false :: boolean(),
    is_session_present = false :: boolean(),
    user_context :: term(),
    transport = undefined :: pid(),
    pending_connack = undefined :: term(),
    pending :: queue:queue(),
    packet_id = 0 :: packet_id(),
    awaiting_ack = #{} :: map(),  % Initiated by server
    awaiting_rel = #{} :: map(),  % Initiated by client
    will = undefined :: undefined | map(),
    will_pid = undefined :: undefined | pid()
}).

-record(queued, {
    type :: atom(),
    packet_id = undefined :: undefined | non_neg_integer(),
    queued :: pos_integer(),
    expiry :: pos_integer(),
    qos :: 0..2,
    message :: mqtt_packet_map:mqtt_packet()
}).


-include_lib("../include/mqtt_sessions.hrl").
-include_lib("mqtt_packet_map/include/mqtt_packet_map.hrl").

-define(DEFAULT_MESSAGE_EXPIRY, 3600).


-spec kill( pid() ) -> ok.
kill(Pid) ->
    gen_server:cast(Pid, kill).

-spec incoming_message(pid(), mqtt_packet_map:mqtt_packet(), mqtt_sessions:msg_options()) -> ok.
incoming_message(Pid, Msg, Options) ->
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

-spec start_link( Pool::atom(), ClientId::binary() ) -> {ok, pid()}.
start_link( Pool, ClientId ) ->
    gen_server:start_link(?MODULE, [ Pool, ClientId ], []).


% ---------------------------------------------------------------------------------------
% --------------------------- gen_server functions --------------------------------------
% ---------------------------------------------------------------------------------------

init([ Pool, ClientId ]) ->
    mqtt_sessions_registry:register(Pool, ClientId, self()),
    {ok, WillPid} = mqtt_sessions_will_sup:start(Pool, self()),
    {ok, Runtime} = application:get_env(mqtt_sessions, runtime),
    {ok, #state{
        pool = Pool,
        runtime = Runtime,
        user_context = Runtime:new_user_context(Pool, ClientId),
        client_id = ClientId,
        pending = queue:new(),
        will_pid = WillPid
    }}.

handle_call({connect_transport, TransportPid}, _From, State) ->
    {reply, ok, State#state{ transport = TransportPid }};
handle_call({disconnect_transport, TransportPid}, _From, #state{ transport = TransportPid } = State) ->
    {reply, ok, State#state{ transport = undefined }};
handle_call({disconnect_transport, TransportPid}, _From, State) ->
    lager:info("Transport disconnect for ~p, but connected to ~p",
               [TransportPid, State#state.transport]),
    {reply, {error, notconnected}, State};
handle_call(fetch_queue, _From, #state{ pending_connack = undefined } = State) ->
    Qs = [ Msg || #queued{ message = Msg } <- queue:to_list(State#state.pending) ],
    {reply, {ok, Qs}, State#state{ pending = queue:new() }};
handle_call(fetch_queue, _From, #state{ pending_connack = ConnAck } = State) ->
    {reply, {ok, [ ConnAck ]}, State#state{ pending_connack = undefined }};
handle_call(Cmd, _From, State) ->
    {stop, {unknown_cmd, Cmd}, State}.

handle_cast({incoming, Msg, Options}, State) ->
    case handle_incoming(Msg, Options, State) of
        {ok, State1} ->
            {noreply, State1};
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


handle_info(#mqtt_msg{ message = Msg, subscriber_context = SubOpts } = Msg, State) ->
    io:format("RECEIVED MESSAGE ~p", [Msg]),
    % Set the QoS and retain flags according to the SubOpts
    % Forward or queue the Msg (which must be a 'publish' message)
    {noreply, State};

handle_info({'DOWN', _Mref, process, Pid, _Reason}, #state{ transport = Pid } = State) ->
    {noreply, State#state{ transport = undefined }};
handle_info({'DOWN', _Mref, process, _Pid, _Reason}, State) ->
    {noreply, State};

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
handle_incoming(#{ type := pubrel } = Msg, Options, State) ->
    pubrel(Msg, Options, State);
handle_incoming(#{ type := subscribe } = Msg, Options, State) ->
    subscribe(Msg, Options, State);
handle_incoming(#{ type := pingreq }, Options, State) ->
    State1 = reply(#{ type => pingresp }, Options, State),
    {ok, State1};
handle_incoming(#{ type := pingresp }, _Options, State) ->
    {ok, State};
handle_incoming(#{ type := disconnect } = Msg, Options, State) ->
    disconnect(Msg, Options, State).




%% @doc Handle the connect message. Either this is a re-connect or the first connect.
connect(#{ protocol_version := 5, protocol_name := <<"MQTT">> } = Msg, Options, State) ->
    State1 = State#state{
        will = extract_will(Msg),
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
                transport = proplists:get_value(transport, Options, State#state.transport),
                will = undefined
            },
            State2 = reply_connack(ConnAck, Options, State1),
            case ReasonCode of
                ?MQTT_RC_SUCCESS ->
                    mqtt_sessions_will:connected(State2#state.will_pid, State#state.will, State2#state.user_context),
                    State3 = State2#state{
                        is_connected = true,
                        is_session_present = true,
                        will = undefined
                    },
                    {ok, State3};
                _ ->
                    State3 = State2#state{
                        is_connected = false
                    },
                    {stop, State3}
            end;
        {ok, #{ type := auth } = Auth, UserContext1} ->
            State1 = State#state{
                user_context = UserContext1,
                transport = proplists:get_value(transport, Options, State#state.transport)
            },
            State2 = reply(Auth, Options, State1),
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
            MsgPub = Msg#{ dup => false },
            ok = mqtt_sessions_router:publish(State#state.pool, Topic, MsgPub, UCtx);
        false ->
            ok
    end,
    {ok, State};
publish(#{ topic := Topic, qos := 1, dup := Dup, packet_id := PacketId } = Msg, Options,
        #state{ runtime = Runtime, user_context = UCtx, awaiting_rel = WaitRel } = State) ->
    case maps:find(PacketId, WaitRel) of
        {ok, _} when not Dup ->
            PubAck = #{
                type => puback,
                packet_id => PacketId,
                reason_code => ?MQTT_RC_PACKET_ID_IN_USE
            },
            reply(PubAck, Options, State);
        {ok, {pubrel, RC, _}} when Dup ->
            PubAck = #{
                type => puback,
                packet_id => PacketId,
                reason_code => RC
            },
            reply(PubAck, Options, State);
        error ->
            RC = case Runtime:is_allowed(publish, Topic, Msg, UCtx) of
                true ->
                    MsgPub = Msg#{ dup => false },
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
                    MsgPub = Msg#{ dup => false },
                    ok = mqtt_sessions_router:publish(State#state.pool, Topic, MsgPub, UCtx),
                    ?MQTT_RC_SUCCESS;
                false ->
                    ?MQTT_RC_NOT_AUTHORIZED
            end,
            State1 = if
                RC < 16#80 ->
                    State#state{ awaiting_rel = WaitRel#{ PacketId => {pubrel, RC, timestamp()} } };
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


%% @doc Handle a subscribe request
subscribe(#{ topics := Topics } = Msg, Options, #state{ runtime = Runtime } = State) ->
    % Check if packet_id is unused
    Resp = lists:map(
        fun(#{
            topic := TopicFilter,
            qos := QoS,
            no_local := NoLocal,
            retain_as_published := Retain,
            retain_handling := RetainHandling
        }) ->
            case Runtime:is_allowed(subscribe, TopicFilter, Msg, State#state.user_context) of
                true ->
                    SubCtx = #{
                        qos => QoS,
                        no_local => NoLocal,
                        retain_as_published => Retain
                    },
                    case mqtt_sessions_router:subscribe(State#state.pool, TopicFilter, {pid, self(), SubCtx}) of
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


%% @doc Handle a disconnect from the client.
disconnect(#{ reason_code := ?MQTT_RC_SUCCESS }, _Options, State) ->
    will_disconnected(false, undefined, State);
disconnect(#{ properties := Props }, _Options, State) ->
    ExpiryInterval = maps:get(session_expiry_interval, Props, undefined),
    will_disconnected(true, ExpiryInterval, State).

%% @doc Signal the will-watchdog that the session is disconnected. It will start
%%      a timer for automatic expiry of this session process.
will_disconnected(IsWill, Expiry, State) ->
    mqtt_sessions_will:disconnected(State#state.will_pid, IsWill, Expiry),
    send_transport({reply, disconnect}, State),
    {ok, State#state{
        is_connected = false,
        transport = undefined
    }}.

%% @doc Send a connack to the remote, ping the will-watchdog that we connected
reply_connack(#{ type := connack } = ConnAck, Options, State) ->
    AckProps = maps:get(properties, ConnAck, #{}),
    ConnAck1 = ConnAck#{
        session_present => State#state.is_session_present,
        properties => AckProps#{
            assigned_client_identifier => State#state.client_id,
            subscription_identifier_available => false,
            shared_subscription_available => false
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
queue(#{ type := Type } = Msg, State) ->
    Props = maps:get(properties, Msg, #{}),
    Now = timestamp(),
    Item = #queued{
        type = Type,
        queued = Now,
        expiry = Now + maps:get(message_expiry_interval, Props, ?DEFAULT_MESSAGE_EXPIRY),
        qos = maps:get(qos, Msg, 1),
        message = Msg
    },
    State#state{ pending = queue:in(Item, State#state.pending) }.


-spec encode( mqtt_packet_map:mqtt_packet() ) -> binary().
encode(Msg) ->
    mqtt_packet_map:encode(Msg).

%% @doc Select the transport for sending a message (if any).
%%      If there was another transport connected, then disconnect that one.
select_transport(Options, #state{ transport = Transport } = State) ->
    case proplists:get_value(transport, Options, Transport) of
        Transport ->
            State;
        Pid when is_pid(Pid), is_pid(Transport) ->
            Transport ! {reply, disconnect},
            erlang:monitor(process, Pid),
            State#state{ transport = Pid };
        Pid when is_pid(Pid), Transport =:= undefined ->
            erlang:monitor(process, Pid),
            State#state{ transport = Pid };
        undefined ->
            State
    end.


% Constant value of calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}})
-define(SECS_1970, 62167219200).

%% @doc Calculate the current UNIX timestamp (seconds since Jan 1, 1970)
timestamp() ->
    calendar:datetime_to_gregorian_seconds(calendar:universal_time())-?SECS_1970.
