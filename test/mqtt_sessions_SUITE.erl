-module(mqtt_sessions_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("mqtt_sessions/include/mqtt_sessions.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(mqtt_sessions),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(mqtt_sessions),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

all() ->
    [
        subscribe_self_test,
        subscribe_mfa_test,
        subscribe_self_wildcard_test,
        subscribe_self_wildcard2_test,
        subscribe_temp_test,
        retained_memory_limit_test
    ].

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

subscribe_self_test(_Config) ->
    ok = mqtt_sessions:subscribe([ <<"test1">> ], subctx),
    mqtt_sessions:publish([ <<"test1">> ], <<"hello">>, pubctx),
    receive
        {mqtt_msg, MqttMsg} ->
            #{
                publisher_context := pubctx,
                topic := [ <<"test1">> ],
                topic_bindings := [],
                message := #{
                    type := publish,
                    topic := [ <<"test1">> ],
                    payload := <<"hello">>
                }
            } = MqttMsg,
            ok
    end.

subscribe_mfa_test(_Config) ->
    Ref = erlang:make_ref(),
    MFA = {?MODULE, subscribe_mfa, [self(), Ref]},
    ok = mqtt_sessions:subscribe(?MQTT_SESSIONS_DEFAULT, [ <<"test2">> ], MFA, subctx),
    mqtt_sessions:publish([ <<"test2">> ], <<"hello">>, pubctx),
    receive
        {yeah, Ref, MqttMsg} ->
            #{
                publisher_context := pubctx,
                topic := [ <<"test2">> ],
                topic_bindings := [],
                message := #{
                    type := publish,
                    topic := [ <<"test2">> ],
                    payload := <<"hello">>
                }
            } = MqttMsg,
            ok
    end.

subscribe_mfa(Pid, Ref, Msg) ->
    Pid ! {yeah, Ref, Msg}.

subscribe_self_wildcard_test(_Config) ->
    ok = mqtt_sessions:subscribe([ <<"test3">>, <<"#">> ], subctx),
    mqtt_sessions:publish([ <<"test3">>, <<"a">>, <<"b">> ], <<"hello">>, pubctx),
    receive
        {mqtt_msg, MqttMsg1} ->
            #{
                publisher_context := pubctx,
                topic := [ <<"test3">>, <<"a">>, <<"b">> ],
                topic_bindings := [ {'#',[<<"a">>, <<"b">>]} ],
                message := #{
                    type := publish,
                    topic := [ <<"test3">>, <<"a">>, <<"b">> ],
                    payload := <<"hello">>
                }
            } = MqttMsg1,
            ok
    end,
    mqtt_sessions:publish([ <<"test3">> ], <<"hello">>, pubctx),
    receive
        {mqtt_msg, MqttMsg2} ->
            #{
                publisher_context := pubctx,
                topic := [ <<"test3">> ],
                topic_bindings := [ {'#',[]} ],
                message := #{
                    type := publish,
                    topic := [ <<"test3">> ],
                    payload := <<"hello">>
                }
            } = MqttMsg2,
            ok
    end.

subscribe_self_wildcard2_test(_Config) ->
    ok = mqtt_sessions:subscribe([ <<"test4">>, <<"+">>, <<"+">>, <<"x">>, <<"#">> ], subctx),
    Topic = [ <<"test4">>, <<"a">>, <<"b">>, <<"x">>, <<"c">>, <<"d">> ],
    mqtt_sessions:publish(Topic, <<"hello">>, pubctx),
    receive
        {mqtt_msg, MqttMsg} ->
            #{
                publisher_context := pubctx,
                topic := Topic,
                topic_bindings := [
                    <<"a">>,
                    <<"b">>,
                    {'#',[ <<"c">>, <<"d">>]}
                ],
                message := #{
                    type := publish,
                    topic := Topic,
                    payload := <<"hello">>
                }
            } = MqttMsg,
            ok
    end.

subscribe_temp_test(_Config) ->
    {ok, Topic} = mqtt_sessions:temp_response_topic(subctx),
    mqtt_sessions:publish(Topic, <<"hello">>, pubctx),
    receive
        {mqtt_msg, MqttMsg} ->
            #{
                publisher_context := pubctx,
                message := #{
                    type := publish,
                    payload := <<"hello">>
                }
            } = MqttMsg,
            ok
    end.

retained_memory_limit_test(_Config) ->
    Pool = ?MQTT_SESSIONS_DEFAULT,
    Topic1 = [ <<"retained-memory-limit-1">> ],
    Topic2 = [ <<"retained-memory-limit-2">> ],
    Msg1 = #{
        type => publish,
        topic => Topic1,
        payload => <<"hello-1">>,
        retain => true
    },
    Msg2 = #{
        type => publish,
        topic => Topic2,
        payload => <<"hello-2">>,
        retain => true
    },
    ok = mqtt_sessions_retain:retain(Pool, Msg1, ctx1),
    {ok, [{_, ctx1}]} = mqtt_sessions_retain:lookup(Pool, Topic1),
    MemoryLimit = retained_memory_bytes(Pool),
    with_max_retained_memory(
        MemoryLimit,
        fun() ->
            ok = mqtt_sessions_retain:retain(Pool, Msg2, ctx2),
            {ok, []} = mqtt_sessions_retain:lookup(Pool, Topic1),
            {ok, [{RetainedMsg2, ctx2}]} = mqtt_sessions_retain:lookup(Pool, Topic2),
            #{ payload := <<"hello-2">> } = RetainedMsg2
        end),
    ok = mqtt_sessions_retain:retain(Pool, Msg1#{ payload => <<>> }, ctx1),
    ok = mqtt_sessions_retain:retain(Pool, Msg2#{ payload => <<>> }, ctx2).

with_max_retained_memory(MaxRetainedMemory, Fun) ->
    OldValue = application:get_env(mqtt_sessions, max_retained_memory),
    application:set_env(mqtt_sessions, max_retained_memory, MaxRetainedMemory),
    try
        Fun()
    after
        case OldValue of
            {ok, Value} ->
                application:set_env(mqtt_sessions, max_retained_memory, Value);
            undefined ->
                application:unset_env(mqtt_sessions, max_retained_memory)
        end
    end.

retained_memory_bytes(Pool) ->
    WordSize = erlang:system_info(wordsize),
    Topics = list_to_atom(atom_to_list(Pool) ++ "$retaintp"),
    Messages = list_to_atom(atom_to_list(Pool) ++ "$retainms"),
    (ets:info(Topics, memory) + ets:info(Messages, memory)) * WordSize.
