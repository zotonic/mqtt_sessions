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
        subscribe_temp_test
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
