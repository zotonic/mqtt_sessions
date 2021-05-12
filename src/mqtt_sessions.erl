%% @author Marc Worrell <marc@worrell.nl>
%% @copyright 2018 Marc Worrell

%% Copyright 2018 Marc Worrell
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

-module(mqtt_sessions).

-behaviour(application).

-export([
    start/0,
    start/2,
    stop/1
    ]).

-export([
    runtime/0,
    set_runtime/1,

    find_session/1,
    find_session/2,
    fetch_queue/1,
    fetch_queue/2,

    session_count/1,
    router_info/1,

    get_user_context/1,
    get_user_context/2,
    set_user_context/2,
    set_user_context/3,
    update_user_context/2,
    update_user_context/3,

    get_transport/1,

    publish/2,
    publish/3,
    publish/4,
    publish/5,
    subscribe/2,
    subscribe/3,
    subscribe/4,
    subscribe/6,
    unsubscribe/1,
    unsubscribe/2,
    unsubscribe/3,

    temp_response_topic/1,
    temp_response_topic/2,
    await_response/1,
    await_response/2,
    await_response/3,

    incoming_connect/2,
    incoming_connect/3,

    incoming_data/2,

    sidejobs_limit/0,
    sidejobs_per_session/0
    ]).


-type session_ref() :: pid() | binary().
-type msg_options() :: #{
        transport => pid() | function(),
        peer_ip => tuple() | undefined,
        context_prefs => map(),
        connection_pid => pid()
    }.
-type session_options() :: #{
        routing_id => binary(),
        peer_ip => tuple() | undefined,
        context_prefs => map()
    }.
-type mqtt_msg() :: mqtt_sessions_router:mqtt_msg().
-type subscriber() :: mqtt_sessions_router:subscriber().
-type subscriber_options() :: mqtt_sessions_router:subscriber_options().

-type topic() :: mqtt_packet_map:mqtt_topic().

-type callback() :: pid() | {module(), atom(), list()}.

-export_type([
    session_ref/0,
    msg_options/0,
    session_options/0,
    mqtt_msg/0,
    subscriber/0,
    subscriber_options/0,
    topic/0,
    callback/0
]).

-define(SIDEJOBS_PER_SESSION, 20).
-define(DEFAULT_CALL_TIMEOUT, 5000).

-include("../include/mqtt_sessions.hrl").
-include_lib("mqtt_packet_map/include/mqtt_packet_map.hrl").

%%====================================================================
%% API
%%====================================================================

-spec start() -> {ok, pid()} | {error, term()}.
start() ->
    ensure_started(mqtt_sessions).

-spec start( application:start_type(), term() ) -> {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
    sidejob:new_resource(?MQTT_SESSIONS_JOBS, sidejob_supervisor, sidejobs_limit()),
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

-spec session_count( atom() ) -> integer.
session_count(Pool) ->
    mqtt_sessions_process_sup:session_count(Pool).

-spec router_info( atom() ) -> list().
router_info(Pool) ->
    mqtt_sessions_router:info(Pool).

-spec fetch_queue( session_ref() ) -> {ok, list( mqtt_packet_map:mqtt_message() | binary() )} | {error, notfound}.
fetch_queue(ClientId) ->
    fetch_queue(?MQTT_SESSIONS_DEFAULT, ClientId).

-spec fetch_queue( atom(), session_ref() ) -> {ok, list( mqtt_packet_map:mqtt_message() | binary() )} | {error, notfound}.
fetch_queue(Pool, ClientId) ->
    case find_session(Pool, ClientId) of
        {ok, Pid} -> mqtt_sessions_process:fetch_queue(Pid);
        {error, _} = Error -> Error
    end.

-spec get_user_context( session_ref() ) -> {ok, term()} | {error, notfound | noproc}.
get_user_context(ClientId) ->
    get_user_context(?MQTT_SESSIONS_DEFAULT, ClientId).

-spec get_user_context( atom(), session_ref() ) -> {ok, term()} | {error, notfound | noproc}.
get_user_context(Pool, ClientId) ->
    case find_session(Pool, ClientId) of
        {ok, Pid} -> mqtt_sessions_process:get_user_context(Pid);
        {error, _} = Error -> Error
    end.

-spec set_user_context( session_ref(), term() ) -> {ok, term()} | {error, notfound | noproc}.
set_user_context(ClientId, UserContext) ->
    set_user_context(?MQTT_SESSIONS_DEFAULT, ClientId, UserContext).

-spec set_user_context( atom(), session_ref(), term() ) -> {ok, term()} | {error, notfound | noproc}.
set_user_context(Pool, ClientId, UserContext) ->
    case find_session(Pool, ClientId) of
        {ok, Pid} -> mqtt_sessions_process:set_user_context(Pid, UserContext);
        {error, _} = Error -> Error
    end.

-spec update_user_context( session_ref(), fun( (term()) -> term() ) ) -> {ok, term()} | {error, notfound | noproc}.
update_user_context(ClientId, Fun) ->
    update_user_context(?MQTT_SESSIONS_DEFAULT, ClientId, Fun).

-spec update_user_context( atom(), session_ref(), fun( (term()) -> term() ) ) -> {ok, term()} | {error, notfound | noproc}.
update_user_context(Pool, ClientId, Fun) ->
    case find_session(Pool, ClientId) of
        {ok, Pid} -> mqtt_sessions_process:update_user_context(Pid, Fun);
        {error, _} = Error -> Error
    end.

-spec get_transport( pid() ) -> {ok, pid()} | {error, notransport | noproc}.
get_transport(SessionPid) ->
    mqtt_sessions_process:get_transport(SessionPid).

-spec publish( mqtt_packet_map:mqtt_message(), term() ) -> ok | {error, eacces | invalid_topic}.
publish(#{ type := publish } = Msg, UserContext) ->
    publish(?MQTT_SESSIONS_DEFAULT, Msg, UserContext).

-spec publish
        ( topic(), term(), term() ) -> ok | {error, eacces | invalid_topic};
        ( atom(), mqtt_packet_map:mqtt_message(), term() ) -> ok | {error, eacces | invalid_topic}.
publish(Pool, #{ type := publish, topic := Topic } = Msg, UserContext) when is_atom(Pool) ->
    case mqtt_packet_map_topic:validate_topic_publish(Topic) of
        {ok, TopicValidated} ->
            Runtime = runtime(),
            case Runtime:is_allowed(publish, TopicValidated, Msg, UserContext) of
                true ->
                    case mqtt_sessions_router:publish(Pool, TopicValidated, Msg, UserContext) of
                        {ok, _Pid} -> ok;
                        {error, _} = Error -> Error
                    end;
                false ->
                    {error, eacces}
            end;
        {error, _} = Error ->
            Error
    end;
publish(Topic, Payload, UserContext) when is_list(Topic); is_binary(Topic) ->
    publish(?MQTT_SESSIONS_DEFAULT, Topic, Payload, #{}, UserContext).

-spec publish( atom(), topic(), term(), term() ) -> ok | {error, eacces | invalid_topic}.
publish(Pool, Topic, Payload, UserContext) ->
    publish(Pool, Topic, Payload, #{}, UserContext).

-spec publish( atom(), topic(), term(), map(), term() ) -> ok | {error, eacces | invalid_topic}.
publish(Pool, Topic, Payload, Options, UserContext) ->
    Msg = #{
        type => publish,
        payload => Payload,
        topic => Topic,
        qos => maps:get(qos, Options, 0),
        retain => maps:get(retain, Options, false),
        properties => maps:get(properties, Options, #{})
    },
    publish(Pool, Msg, UserContext).


-spec subscribe( topic(), term() ) -> ok | {error, eacces | invalid_topic}.
subscribe(TopicFilter, UserContext) ->
    subscribe(?MQTT_SESSIONS_DEFAULT, TopicFilter, self(), self(), #{}, UserContext).

-spec subscribe( atom(), topic(), term() ) -> ok | {error, eacces | invalid_topic}.
subscribe(Pool, TopicFilter, UserContext) ->
    subscribe(Pool, TopicFilter, self(), self(), #{}, UserContext).

-spec subscribe( atom(), topic(), callback(), term() ) -> ok | {error, eacces | invalid_topic}.
subscribe(Pool, TopicFilter, {_, _, _} = MFA, UserContext) ->
    subscribe(Pool, TopicFilter, MFA, self(), #{}, UserContext);
subscribe(Pool, TopicFilter, Pid, UserContext) when is_pid(Pid) ->
    subscribe(Pool, TopicFilter, Pid, Pid, #{}, UserContext).

-spec subscribe( atom(), topic(), callback(), pid(), map(), term() ) -> ok | {error, eacces | invalid_topic}.
subscribe(Pool, TopicFilter, Receiver, OwnerPid, Options, UserContext) ->
    Runtime = runtime(),
    case mqtt_packet_map_topic:validate_topic(TopicFilter) of
        {ok, TopicFilterValidated} ->
            case Runtime:is_allowed(subscribe, TopicFilterValidated, #{}, UserContext) of
                true ->
                    SubOpts = #{
                        no_local => maps:get(no_local, Options, false)
                    },
                    mqtt_sessions_router:subscribe(Pool, TopicFilterValidated, Receiver, OwnerPid, SubOpts, UserContext);
                false ->
                    {error, eacces}
            end;
        {error, _} = Error ->
            Error
    end.

-spec unsubscribe( topic() ) -> ok | {error, notfound | invalid_topic}.
unsubscribe(TopicFilter) ->
    unsubscribe(?MQTT_SESSIONS_DEFAULT, TopicFilter, self()).

-spec unsubscribe( atom(), topic() ) -> ok | {error, notfound | invalid_topic}.
unsubscribe(Pool, TopicFilter) ->
    unsubscribe(Pool, TopicFilter, self()).

-spec unsubscribe( atom(), topic(), pid() ) -> ok | {error, notfound | invalid_topic}.
unsubscribe(Pool, TopicFilter, OwnerPid) ->
    case mqtt_packet_map_topic:validate_topic(TopicFilter) of
        {ok, TopicFilterValidated} ->
            mqtt_sessions_router:unsubscribe(Pool, TopicFilterValidated, OwnerPid);
        {error, _} = Error ->
            Error
    end.


-spec temp_response_topic( term() ) -> {ok, topic()} | {error, eacces}.
temp_response_topic(UserContext) ->
    temp_response_topic(?MQTT_SESSIONS_DEFAULT, UserContext).

-spec temp_response_topic( atom(), term() ) -> {ok, topic()} | {error, eacces}.
temp_response_topic(Pool, UserContext) ->
    case erlang:get(mqtt_session_response_topic) of
        {Pool, Topic, Self} when Self =:= self() ->
            {ok, topic_append_unique(Topic)};
        _ ->
            Topic = [ <<"reply">>, <<"call-", (random_key(20))/binary>> ],
            case subscribe(Pool, Topic ++ [ <<"+">> ], UserContext) of
                ok ->
                    erlang:put(mqtt_session_response_topic, {Pool, Topic, self()}),
                    erlang:put(mqtt_session_response_nr, 0),
                    {ok, topic_append_unique(Topic)};
                {error, _} = Error ->
                    Error
            end
    end.

-spec await_response( topic() ) -> {ok, mqtt_packet_map:mqtt_message()} | {error, timeout}.
await_response( Topic ) ->
    await_response(?MQTT_SESSIONS_DEFAULT, Topic).

-spec await_response
    ( topic(), pos_integer() ) -> {ok, mqtt_packet_map:mqtt_message()} | {error, timeout};
    ( atom(), topic() ) -> {ok, mqtt_packet_map:mqtt_message()} | {error, timeout}.

await_response( Topic, Timeout ) when is_list(Topic), is_integer(Timeout) ->
    await_response(?MQTT_SESSIONS_DEFAULT, Topic, Timeout);
await_response( Pool, Topic ) when is_atom(Pool), is_list(Topic) ->
    await_response(Pool, Topic, ?DEFAULT_CALL_TIMEOUT).

await_response( Pool, Topic, Timeout ) when is_list(Topic), is_atom(Pool), is_integer(Timeout) ->
    receive
        {mqtt_msg, #{ type := publish, topic := Topic } = MqttMsg} ->
            {ok, MqttMsg}
    after Timeout ->
        {error, timeout}
    end.


%%--------------------------------------------------------------------


-spec topic_append_unique( topic() ) -> topic().
topic_append_unique(Topic) ->
    N = case erlang:get(mqtt_session_response_nr) of
        undefined -> 0;
        N0 -> N0+1
    end,
    erlang:put(mqtt_session_response_nr, N),
    Topic ++ [ integer_to_binary(N) ].

%% @doc Generate a random key consisting of numbers and upper and lower case characters.
-spec random_key( Length::integer() ) -> binary().
random_key(Len) ->
    <<
        <<
            case N of
                C when C < 26 -> C  + $a;
                C when C < 52 -> C - 26 + $A;
                C -> C - 52 + $0
            end
        >>
        || N <- random_list(Len)
    >>.

random_list(N) ->
    [ rand:uniform(62) || _X <- lists:seq(1, N) ].


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

%% @doc Set the runtime module
-spec set_runtime( atom() ) -> ok.
set_runtime(Runtime) ->
    application:set_env(mqtt_sessions, runtime, Runtime).


%% @doc Stream the connect message - connect a MQTT session or return an error
-spec incoming_connect( binary(), msg_options() ) -> {ok, {session_ref(), binary()}} | {error, incomplete_packet} | {error, term()}.
incoming_connect(MsgBin, Options) ->
    incoming_connect(undefined, MsgBin, Options).

%% @doc Stream the connect message - connect a MQTT session or return an error
-spec incoming_connect( atom(), binary(), msg_options() ) -> {ok, {session_ref(), binary()}} | {error, incomplete_packet} | {error, term()}.
incoming_connect(Pool, MsgBin, Options) ->
    case mqtt_packet_map:decode(MsgBin) of
        {ok, {#{ type := connect } = Packet, Rest}} ->
            case connect_pool(Pool, Packet) of
                {ok, ConnectPool} ->
                    {ok, SessionRef} = mqtt_sessions_incoming:incoming_connect(ConnectPool, Packet, Options#{ connection_pid => self() }),
                    {ok, {SessionRef, Rest}};
                {error, _} = Error ->
                    mqtt_sessions_incoming:send_connack_error(?MQTT_RC_SERVER_UNAVAILABLE, Packet, Options),
                    Error
            end;
        {ok, {_Msg, _Rest}} ->
            {error, expect_connect};
        {error, _} = Error ->
            Error
    end.

connect_pool(Pool, Msg) ->
    Runtime = runtime(),
    case username_to_pool(Msg) of
        undefined when Pool =:= undefined ->
            Runtime:pool_default();
        undefined ->
            {ok, Pool};
        {ok, NewPool} when Pool =:= undefined ->
            {ok, NewPool};
        {ok, NewPool} when NewPool =/= Pool ->
            {error, pool_mismatch};
        {ok, Pool} ->
            {ok, Pool};
        {error, _} = Error ->
            Error
    end.

username_to_pool(#{ type := connect, username := Username }) when is_binary(Username) ->
    Runtime = runtime(),
    case binary:split(Username, <<":">>) of
        [VHost, _LocalUsername] ->
            Runtime:vhost_pool(VHost);
        _ ->
            undefined
    end;
username_to_pool(_ConnectMsg) ->
    undefined.


%% @doc Handle incoming data for session. Call this after a successful connect. The session will disconnect on an illegal packet.
-spec incoming_data( session_ref(), binary() ) -> ok | {error, wrong_connection | mqtt_packet_map:decode_error()}. 
incoming_data(SessionRef, MsgBin) ->
    mqtt_sessions_process:incoming_data(SessionRef, MsgBin).


%% @doc Limit the number of sidejobs for message dispatching.
-spec sidejobs_limit() -> pos_integer().
sidejobs_limit() ->
    case application:get_env(mqtt_sessions, sidejobs_limit) of
        {ok, N} -> N;
        undefined -> erlang:max(erlang:system_info(process_limit) div 10, 10000)
    end.

-spec sidejobs_per_session() -> pos_integer().
sidejobs_per_session() ->
    case application:get_env(mqtt_sessions, sidejobs_per_session) of
        {ok, N} -> N;
        undefined -> ?SIDEJOBS_PER_SESSION
    end.


