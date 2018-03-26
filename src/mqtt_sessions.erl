-module(mqtt_sessions).

-behaviour(application).

-export([
    start/0,
    start/2,
    stop/1
    ]).

-export([
    runtime/0,
    find_session/1,
    find_session/2,
    fetch_queue/1,
    fetch_queue/2,
    incoming_message/3,
    incoming_message/4,
    connect_transport/2,
    connect_transport/3,
    disconnect_transport/2,
    disconnect_transport/3
    ]).


-type session_ref() :: pid() | binary().
-type opt_session_ref() :: session_ref() | undefined.
-type msg_options() :: list( msg_option() ).
-type msg_option() :: {transport, pid()}.

-export_type([
    session_ref/0,
    msg_options/0,
    msg_option/0
]).

-include("../include/mqtt_sessions.hrl").

%%====================================================================
%% API
%%====================================================================

-spec start() -> {ok, pid()} | {error, term()}.
start() ->
    ensure_started(mqtt_sessions).

-spec start( application:start_type(), term() ) -> {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
    mqtt_sessions_sup:start_link().

-spec stop( term() ) -> ok.
stop(_State) ->
    ok.


-spec find_session( session_ref() ) -> {ok, pid()} | {error, notfound}.
find_session( ClientId ) ->
    find_session(?MQTT_SESSIONS_DEFAULT, ClientId).

-spec find_session( atom(), session_ref() ) -> {ok, pid()} | {error, notfound}.
find_session(Pool, ClientId) ->
    mqtt_sessions_registry:find_session(Pool, ClientId).


-spec fetch_queue( session_ref() ) -> {ok, list( mqtt_packet_map:mqtt_message() | binary() )} | {error, notfound}.
fetch_queue(ClientId) ->
    fetch_queue(?MQTT_SESSIONS_DEFAULT, ClientId).

-spec fetch_queue( atom(), session_ref() ) -> {ok, list( mqtt_packet_map:mqtt_message() | binary() )} | {error, notfound}.
fetch_queue( Pool, ClientId ) ->
    case find_session(Pool, ClientId) of
        {ok, Pid} -> mqtt_sessions_process:fetch_queue(Pid);
        {error, _} = Error -> Error
    end.

%%--------------------------------------------------------------------

-spec ensure_started(atom()) -> ok | {error, term()}.
ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {not_started, Dep}} ->
            case ensure_started(Dep) of
                ok -> ensure_started(App);
                {error, _} = Error -> Error
            end;
        {error, {already_started, App}} ->
            ok;
        {error, {Tag, Msg}} when is_list(Tag), is_list(Msg) ->
            {error, lists:flatten(io_lib:format("~s: ~s", [Tag, Msg]))};
        {error, {bad_return, {{M, F, Args}, Return}}} ->
            A = string:join([io_lib:format("~p", [A])|| A <- Args], ", "),
            {error, lists:flatten(
                        io_lib:format("~s failed to start due to a bad return value from call ~s:~s(~s):~n~p",
                                      [App, M, F, A, Return]))};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Return the runtime module for AUTH and other callbacks.
-spec runtime() -> atom().
runtime() ->
    case application:get_env(mqtt_sessions, runtime) of
        {ok, Runtime} when is_atom(Runtime) -> Runtime;
        undefined -> mqtt_sessions_runtime
    end.


%% @doc Handle an incoming message using the default pool.
-spec incoming_message( opt_session_ref(), mqtt_packet_map:mqtt_packet(), msg_options() ) -> {ok, session_ref()} | {error, term()}.
incoming_message(OptSessionRef, Packet, Options) ->
    incoming_message(?MQTT_SESSIONS_DEFAULT, OptSessionRef, Packet, Options).

%% @doc Handle an incoming message, if there is an session-ref then forward the message to there.
%%      If there is no session-ref then the package MUST be a connect package.
-spec incoming_message( Pool :: atom(), opt_session_ref(), mqtt_packet_map:mqtt_packet(), msg_options() ) -> {ok, session_ref()} | {error, term()}.
incoming_message(Pool, undefined, #{ type := connect, client_id := <<>> } = Msg, Options) ->
    {ok, {Pid, _ClientId}} = mqtt_sessions_process_sup:new_session(Pool),
    ok = mqtt_sessions_process:incoming_message(Pid, Msg, Options),
    {ok, Pid};
incoming_message(Pool, undefined, #{ type := connect, client_id := ClientId, clean_start := true } = Msg, Options) ->
    % a. Close existing client (if is running)
    % b. Continue as if no client id present
    % WARNING: we should use the client-id, but in our case we divert from the specs
    mqtt_sessions_registry:kill_session(Pool, ClientId),
    incoming_message(Pool, undefined, Msg#{ client_id => <<>> }, Options);
incoming_message(Pool, undefined, #{ type := connect, client_id := ClientId, clean_start := false } = Msg, Options) ->
    % Check client_id in connect message
    case find_session(Pool, ClientId) of
        {ok, SessionRef} ->
            ok = mqtt_sessions_process:incoming_message(SessionRef, Msg, Options),
            {ok, SessionRef};
        {error, _} = Error ->
            Error
    end;
incoming_message(Pool, undefined, Msg, _Options) ->
    lager:error("MQTT msg for unknown session in ~p: ~p", [ Pool, Msg ]),
    {error, must_connect};
incoming_message(Pool, ClientId, Msg, Options) when is_binary(ClientId) ->
    case find_session(Pool, ClientId) of
        {ok, SessionRef} ->
            ok = mqtt_sessions_process:incoming_message(SessionRef, Msg, Options),
            {ok, SessionRef};
        {error, _} = Error ->
            Error
    end;
incoming_message(_Pool, SessionRef, Msg, Options) when is_pid(SessionRef) ->
    ok = mqtt_sessions_process:incoming_message(SessionRef, Msg, Options),
    {ok, SessionRef}.


%% @doc Connect an outgoing transport to the session using the default pool
-spec connect_transport( session_ref(), pid() ) -> ok | {error, term()}.
connect_transport(ClientId, Pid) ->
    connect_transport(?MQTT_SESSIONS_DEFAULT, ClientId, Pid).

%% @doc Connect an outgoing transport to the session
-spec connect_transport( atom(), session_ref(), pid() ) -> ok | {error, term()}.
connect_transport(Pool, ClientId, Pid) when is_binary(ClientId) ->
    case find_session(Pool, ClientId) of
        {ok, SessionPid} ->
            mqtt_sessions_process:connect_transport(SessionPid, Pid);
        {error, _} = Error ->
            Error
    end.


%% @doc Disconnect an outgoing transport from the session using the default pool
-spec disconnect_transport( session_ref(), pid() ) -> ok | {error, term()}.
disconnect_transport(ClientId, Pid) ->
    disconnect_transport(?MQTT_SESSIONS_DEFAULT, ClientId, Pid).

%% @doc Disconnect an outgoing transport from the session
-spec disconnect_transport( atom(), session_ref(), pid() ) -> ok | {error, term()}.
disconnect_transport(Pool, ClientId, Pid) ->
    case find_session(Pool, ClientId) of
        {ok, ClientId} ->
            mqtt_sessions_process:connect_transport(ClientId, Pid);
        {error, _} = Error ->
            Error
    end.

