-module(mqtt_sessions_protocol_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("mqtt_sessions/include/mqtt_sessions.hrl").
-include_lib("mqtt_packet_map/include/mqtt_packet_map.hrl").

%% Supress dialyzer warings on using the ct module.
-dialyzer({nowarn_function, connect_disconnect_v5_test/1}).
-dialyzer({nowarn_function, connect_reconnect_v5_test/1}).
-dialyzer({nowarn_function, connect_reconnect_clean_v5_test/1}).
-dialyzer({nowarn_function, connect_reconnect_buffered_v5_test/1}).

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
        connect_disconnect_v5_test,
        connect_reconnect_v5_test,
        connect_reconnect_clean_v5_test,
        connect_reconnect_buffered_v5_test,
        connect_expiry_cap_v5_test,
        connect_max_incoming_packet_size_v5_test,
        connect_packet_too_large_v5_test,
        incoming_packet_too_large_v5_test,
        outgoing_packet_too_large_v5_test,
        incoming_messages_rate_limit_qos1_test,
        incoming_messages_burst_limit_qos1_test,
        incoming_messages_rate_limit_qos0_test,
        anonymous_websocket_disconnect_timeout_test,
        crawler_websocket_disconnect_timeout_test,
        authenticated_websocket_disconnect_timeout_test
    ].

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

connect_disconnect_v5_test(_Config) ->
    Connect = #{
        type => connect,
        protocol_name => <<"MQTT">>,
        protocol_version => 5,
        clean_start => true,
        client_id => <<"test1">>,
        will_flag => false,
        username => <<>>,
        password => <<>>,
        properties => #{
        }
    },
    {ok, ConnectMsg} = mqtt_packet_map:encode(5, Connect),
    Options = #{
        transport => self()
    },
    {ok, {SessionPid, <<>>}} = mqtt_sessions:incoming_connect(ConnectMsg, Options),
    true = is_pid(SessionPid),
    receive
        {mqtt_transport, SessionPid, MsgBin} when is_binary(MsgBin) ->
            {ok, {ConnAck, <<>>}} = mqtt_packet_map:decode(MsgBin),
            #{
                type := connack,
                session_present := false,
                reason_code := 0
            } = ConnAck,
            ok
    end,
    Disconnect = #{
        type => disconnect,
        reason_code => 0,
        properties => #{
            session_expiry_interval => 0
        }
    },
    {ok, DisconnectMsg} = mqtt_packet_map:encode(5, Disconnect),
    ok = mqtt_sessions:incoming_data(SessionPid, DisconnectMsg),
    %
    % The connection should be signaled to disconnect
    %
    receive
        {mqtt_transport, SessionPid, disconnect} ->
            ok
    end,
    %
    % And the session process should stop
    %
    SessionMRef = erlang:monitor(process, SessionPid),
    receive
        {'DOWN', SessionMRef, process, SessionPid, _Reason} ->
            ok
        after 1000 ->
            ct:abort_current_testcase(waiting_for_down)
    end,
    ok.



connect_reconnect_v5_test(_Config) ->
    Connect = #{
        type => connect,
        protocol_name => <<"MQTT">>,
        protocol_version => 5,
        clean_start => true,
        client_id => <<"test2">>,
        will_flag => false,
        username => <<>>,
        password => <<>>,
        properties => #{
        }
    },
    {ok, ConnectMsg} = mqtt_packet_map:encode(5, Connect),
    Options = #{
        transport => self()
    },
    {ok, {SessionPid, <<>>}} = mqtt_sessions:incoming_connect(ConnectMsg, Options),
    true = is_pid(SessionPid),
    receive
        {mqtt_transport, SessionPid, MsgBin} when is_binary(MsgBin) ->
            {ok, {ConnAck, <<>>}} = mqtt_packet_map:decode(MsgBin),
            #{
                type := connack,
                session_present := false,
                reason_code := 0
            } = ConnAck,
            ok
    end,
    % Subscribe to a topic
    Subscribe = #{
        type => subscribe,
        topics => [ <<"reconnect_v5_test">> ]
    },
    {ok, SubMsg} = mqtt_packet_map:encode(5, Subscribe),
    mqtt_sessions:incoming_data(SessionPid, SubMsg),
    receive
        {mqtt_transport, SessionPid, SubAckMsgBin} when is_binary(SubAckMsgBin) ->
            {ok, {SubAck, <<>>}} = mqtt_packet_map:decode(SubAckMsgBin),
            #{
                type := suback,
                acks := [ {ok, 0} ]
            } = SubAck,
            ok
    end,
    % Subscription should be there
    PubMsg1 = #{
        type => publish,
        topic => <<"reconnect_v5_test">>,
        payload => <<"hello1">>,
        qos => 0
    },
    ok = mqtt_sessions:publish(PubMsg1, undefined),
    receive
        {mqtt_transport, SessionPid, PubMsgBin1}  when is_binary(PubMsgBin1) ->
            {ok, {PubMsg1Received, <<>>}} = mqtt_packet_map:decode(PubMsgBin1),
            #{
                type := publish,
                payload := <<"hello1">>
            } = PubMsg1Received,
            ok;
        X ->
            io:format("~p", [ X ])
    after 10 ->
        ct:fail(not_subscribed)
    end,
    % Reconnect without clean_start
    Reconnect = #{
        type => connect,
        protocol_name => <<"MQTT">>,
        protocol_version => 5,
        clean_start => false,
        client_id => <<"test2">>,
        will_flag => false,
        username => <<>>,
        password => <<>>,
        properties => #{
        }
    },
    {ok, ReconnectMsg} = mqtt_packet_map:encode(5, Reconnect),
    {ok, {SessionPid, <<>>}} = mqtt_sessions:incoming_connect(ReconnectMsg, Options),
    %
    % We have reconnected to the existing session, check the connack
    % for the 'session_present' flag.
    %
    receive
        {mqtt_transport, SessionPid, MsgBin2}  when is_binary(MsgBin2) ->
            {ok, {ConnAck2, <<>>}} = mqtt_packet_map:decode(MsgBin2),
            #{
                type := connack,
                session_present := true,
                reason_code := 0
            } = ConnAck2,
            ok
    end,
    % Subscription should still be there
    PubMsg2 = #{
        type => publish,
        topic => <<"reconnect_v5_test">>,
        payload => <<"hello2">>,
        qos => 0
    },
    ok = mqtt_sessions:publish(PubMsg2, undefined),
    receive
        {mqtt_transport, SessionPid, PubMsg2Bin}  when is_binary(PubMsg2Bin) ->
            {ok, {PubMsg2Received, <<>>}} = mqtt_packet_map:decode(PubMsg2Bin),
            #{
                type := publish,
                payload := <<"hello2">>
            } = PubMsg2Received,
            ok
    after 10 ->
        ct:fail(unsubscribed)
    end,
    ok.


connect_reconnect_clean_v5_test(_Config) ->
    Connect = #{
        type => connect,
        protocol_name => <<"MQTT">>,
        protocol_version => 5,
        clean_start => true,
        client_id => <<"test3">>,
        will_flag => false,
        username => <<>>,
        password => <<>>,
        properties => #{
        }
    },
    {ok, ConnectMsg} = mqtt_packet_map:encode(5, Connect),
    Options = #{
        transport => self()
    },
    {ok, {SessionPid, <<>>}} = mqtt_sessions:incoming_connect(ConnectMsg, Options),
    true = is_pid(SessionPid),
    receive
        {mqtt_transport, SessionPid, MsgBin} when is_binary(MsgBin) ->
            {ok, {ConnAck, <<>>}} = mqtt_packet_map:decode(MsgBin),
            #{
                type := connack,
                session_present := false,
                reason_code := 0
            } = ConnAck,
            ok
    end,
    % Subscribe to a topic
    Subscribe = #{
        type => subscribe,
        topics => [ <<"reconnect_clean_v5_test">> ]
    },
    {ok, SubMsg} = mqtt_packet_map:encode(5, Subscribe),
    mqtt_sessions:incoming_data(SessionPid, SubMsg),
    receive
        {mqtt_transport, SessionPid, SubAckMsgBin} when is_binary(SubAckMsgBin) ->
            {ok, {SubAck, <<>>}} = mqtt_packet_map:decode(SubAckMsgBin),
            #{
                type := suback,
                acks := [ {ok, 0} ]
            } = SubAck,
            ok
    end,
    % Subscription should be there
    PubMsg1 = #{
        type => publish,
        topic => <<"reconnect_clean_v5_test">>,
        payload => <<"hello1">>,
        qos => 0
    },
    ok = mqtt_sessions:publish(PubMsg1, undefined),
    receive
        {mqtt_transport, SessionPid, PubMsgBin1}  when is_binary(PubMsgBin1) ->
            {ok, {PubMsg1Received, <<>>}} = mqtt_packet_map:decode(PubMsgBin1),
            #{
                type := publish,
                payload := <<"hello1">>
            } = PubMsg1Received,
            ok;
        X ->
            io:format("~p", [ X ])
    after 10 ->
        ct:fail(not_subscribed)
    end,
    % Reconnect with clean_start
    Reconnect = #{
        type => connect,
        protocol_name => <<"MQTT">>,
        protocol_version => 5,
        clean_start => true,
        client_id => <<"test3">>,
        will_flag => false,
        username => <<>>,
        password => <<>>,
        properties => #{
        }
    },
    {ok, ReconnectMsg} = mqtt_packet_map:encode(5, Reconnect),
    {ok, {SessionPid, <<>>}} = mqtt_sessions:incoming_connect(ReconnectMsg, Options),
    %
    % We have reconnected to the existing session, check the connack
    % for the 'session_present' flag.
    %
    receive
        {mqtt_transport, SessionPid, MsgBin2}  when is_binary(MsgBin2) ->
            {ok, {ConnAck2, <<>>}} = mqtt_packet_map:decode(MsgBin2),
            #{
                type := connack,
                session_present := true,
                reason_code := 0
            } = ConnAck2,
            ok
    end,
    % Subscription should be gone
    PubMsg2 = #{
        type => publish,
        topic => <<"reconnect_clean_v5_test">>,
        payload => <<"hello2">>,
        qos => 0
    },
    ok = mqtt_sessions:publish(PubMsg2, undefined),
    receive
        {mqtt_transport, SessionPid, PubMsg2Bin}  when is_binary(PubMsg2Bin) ->
            {ok, {PubMsg2Received, <<>>}} = mqtt_packet_map:decode(PubMsg2Bin),
            #{
                type := publish,
                payload := <<"hello2">>
            } = PubMsg2Received,
            ct:fail(still_subscribed)
    after 20 ->
        ok
    end,
    ok.

connect_reconnect_buffered_v5_test(_Config) ->
    Connect = #{
        type => connect,
        protocol_name => <<"MQTT">>,
        protocol_version => 5,
        clean_start => true,
        client_id => <<"test4">>,
        will_flag => false,
        username => <<>>,
        password => <<>>,
        properties => #{
        }
    },
    {ok, ConnectMsg} = mqtt_packet_map:encode(5, Connect),
    Options = #{
        transport => self()
    },
    {ok, {SessionPid, <<>>}} = mqtt_sessions:incoming_connect(ConnectMsg, Options),
    true = is_pid(SessionPid),
    receive
        {mqtt_transport, SessionPid, MsgBin} when is_binary(MsgBin) ->
            {ok, {ConnAck, <<>>}} = mqtt_packet_map:decode(MsgBin),
            #{
                type := connack,
                session_present := false,
                reason_code := 0
            } = ConnAck,
            ok
    end,
    % Subscribe to a topic
    Subscribe = #{
        type => subscribe,
        topics => [ #{ topic => <<"reconnect_v5_test">>, qos => 2 } ]
    },
    {ok, SubMsg} = mqtt_packet_map:encode(5, Subscribe),
    mqtt_sessions:incoming_data(SessionPid, SubMsg),
    receive
        {mqtt_transport, SessionPid, SubAckMsgBin} when is_binary(SubAckMsgBin) ->
            {ok, {SubAck, <<>>}} = mqtt_packet_map:decode(SubAckMsgBin),
            #{
                type := suback,
                acks := [ {ok, 2} ]
            } = SubAck,
            ok
    end,
    Disconnect = #{
        type => disconnect,
        reason_code => 0,
        properties => #{
            session_expiry_interval => 3600
        }
    },
    {ok, DisconnectMsg} = mqtt_packet_map:encode(5, Disconnect),
    ok = mqtt_sessions:incoming_data(SessionPid, DisconnectMsg),
    %
    % The connection should be signaled to disconnect
    %
    receive
        {mqtt_transport, SessionPid, disconnect} ->
            ok
    end,
    %
    % And the session process should still be running
    %
    timer:sleep(100),
    true = erlang:is_process_alive(SessionPid),
    %
    % Message should be queued
    %
    PubMsg1 = #{
        type => publish,
        topic => <<"reconnect_v5_test">>,
        payload => <<"hello-offline-qos-0">>,
        qos => 0
    },
    ok = mqtt_sessions:publish(PubMsg1, undefined),

    PubMsg2 = #{
        type => publish,
        topic => <<"reconnect_v5_test">>,
        payload => <<"hello-offline-qos-1">>,
        qos => 1
    },
    ok = mqtt_sessions:publish(PubMsg2, undefined),

    PubMsg3 = #{
        type => publish,
        topic => <<"reconnect_v5_test">>,
        payload => <<"hello-offline-qos-2">>,
        qos => 2
    },
    ok = mqtt_sessions:publish(PubMsg3, undefined),
    %
    % Reconnect without clean_start
    %
    Reconnect = #{
        type => connect,
        protocol_name => <<"MQTT">>,
        protocol_version => 5,
        clean_start => false,
        client_id => <<"test4">>,
        will_flag => false,
        username => <<>>,
        password => <<>>,
        properties => #{
        }
    },
    {ok, ReconnectMsg} = mqtt_packet_map:encode(5, Reconnect),
    {ok, {SessionPid, <<>>}} = mqtt_sessions:incoming_connect(ReconnectMsg, Options),
    %
    % We have reconnected to the existing session, check the connack
    % for the 'session_present' flag.
    %
    receive
        {mqtt_transport, SessionPid, MsgBin2}  when is_binary(MsgBin2) ->
            {ok, {ConnAck2, <<>>}} = mqtt_packet_map:decode(MsgBin2),
            #{
                type := connack,
                session_present := true,
                reason_code := 0
            } = ConnAck2,
            ok
    end,
    %
    % Should receive queued QoS 0 message
    %
    receive
        {mqtt_transport, SessionPid, PubMsg1Bin} when is_binary(PubMsg1Bin) ->
            {ok, {PubMsg1Received, <<>>}} = mqtt_packet_map:decode(PubMsg1Bin),
            #{
                type := publish,
                payload := <<"hello-offline-qos-0">>,
                qos := 0,
                packet_id := undefined
            } = PubMsg1Received,
            ok
    after 10 ->
        ct:fail(unsubscribed)
    end,
    %
    % Should receive queued QoS 1 message
    %
    PacketId2 = receive
        {mqtt_transport, SessionPid, PubMsg2Bin} when is_binary(PubMsg2Bin) ->
            {ok, {PubMsg2Received, <<>>}} = mqtt_packet_map:decode(PubMsg2Bin),
            #{
                type := publish,
                qos := 1,
                payload := <<"hello-offline-qos-1">>,
                packet_id := PId2
            } = PubMsg2Received,
            PId2
    after 10 ->
        ct:fail(unsubscribed)
    end,
    Ack2 = #{
        type => puback,
        packet_id => PacketId2
    },
    {ok, Ack2Data} = mqtt_packet_map:encode(5, Ack2),
    mqtt_sessions_process:incoming_data(SessionPid, Ack2Data),
    %
    % Should receive queued QoS 2 message
    %
    PacketId3 = receive
        {mqtt_transport, SessionPid, PubMsg3Bin} when is_binary(PubMsg3Bin) ->
            {ok, {PubMsg3Received, <<>>}} = mqtt_packet_map:decode(PubMsg3Bin),
            #{
                type := publish,
                qos := 2,
                payload := <<"hello-offline-qos-2">>,
                packet_id := PId3
            } = PubMsg3Received,
            PId3
    after 10 ->
        ct:fail(unsubscribed)
    end,
    % Acknowledge with pubrel
    Rel3 = #{
        type => pubrel,
        packet_id => PacketId3
    },
    {ok, Rel3Data} = mqtt_packet_map:encode(5, Rel3),
    mqtt_sessions_process:incoming_data(SessionPid, Rel3Data),
    % Should receive a pubcomp back
    receive
        {mqtt_transport, SessionPid, PubMsg4Bin} when is_binary(PubMsg4Bin) ->
            {ok, {PubMsg4Received, <<>>}} = mqtt_packet_map:decode(PubMsg4Bin),
            #{
                type := pubcomp,
                packet_id := PacketId3
            } = PubMsg4Received,
            ok
    after 10 ->
        ct:fail(pubcomp)
    end,

    ok.

connect_expiry_cap_v5_test(_Config) ->
    Connect = #{
        type => connect,
        protocol_name => <<"MQTT">>,
        protocol_version => 5,
        clean_start => true,
        client_id => <<"test-expiry-cap">>,
        will_flag => false,
        username => <<>>,
        password => <<>>,
        properties => #{
            session_expiry_interval => 30
        }
    },
    {ok, ConnectMsg} = mqtt_packet_map:encode(5, Connect),
    Options = #{
        transport => self()
    },
    {ok, {SessionPid, <<>>}} = mqtt_sessions:incoming_connect(ConnectMsg, Options),
    true = is_pid(SessionPid),
    receive
        {mqtt_transport, SessionPid, MsgBin} when is_binary(MsgBin) ->
            {ok, {ConnAck, <<>>}} = mqtt_packet_map:decode(MsgBin),
            #{
                type := connack,
                reason_code := 0,
                properties := #{
                    session_expiry_interval := 30
                }
            } = ConnAck,
            ok
    end.

connect_max_incoming_packet_size_v5_test(_Config) ->
    with_max_incoming_packet_size(
        128,
        fun() ->
            Connect = #{
                type => connect,
                protocol_name => <<"MQTT">>,
                protocol_version => 5,
                clean_start => true,
                client_id => <<"test-max-packet-size">>,
                will_flag => false,
                username => <<>>,
                password => <<>>,
                properties => #{}
            },
            {ok, ConnectMsg} = mqtt_packet_map:encode(5, Connect),
            Options = #{
                transport => self()
            },
            {ok, {SessionPid, <<>>}} = mqtt_sessions:incoming_connect(ConnectMsg, Options),
            receive
                {mqtt_transport, SessionPid, MsgBin} when is_binary(MsgBin) ->
                    {ok, {ConnAck, <<>>}} = mqtt_packet_map:decode(MsgBin),
                    #{
                        type := connack,
                        reason_code := 0,
                        properties := #{
                            maximum_packet_size := 128
                        }
                    } = ConnAck,
                    mqtt_sessions_process:kill(SessionPid)
            after 1000 ->
                ct:fail(connect_timeout)
            end
        end).

connect_packet_too_large_v5_test(_Config) ->
    with_max_incoming_packet_size(
        40,
        fun() ->
            Connect = #{
                type => connect,
                protocol_name => <<"MQTT">>,
                protocol_version => 5,
                clean_start => true,
                client_id => <<"test-connect-packet-too-large-abcdefghijklmnopqrstuvwxyz">>,
                will_flag => false,
                username => <<>>,
                password => <<>>,
                properties => #{}
            },
            {ok, ConnectMsg} = mqtt_packet_map:encode(5, Connect),
            Options = #{
                transport => self()
            },
            {error, packet_too_large} = mqtt_sessions:incoming_connect(ConnectMsg, Options),
            receive
                {mqtt_transport, _ConnAckPid, MsgBin} when is_binary(MsgBin) ->
                    {ok, {ConnAck, <<>>}} = mqtt_packet_map:decode(MsgBin),
                    #{
                        type := connack,
                        reason_code := ?MQTT_RC_PACKET_TOO_LARGE
                    } = ConnAck
            after 1000 ->
                ct:fail(connack_timeout)
            end,
            receive
                {mqtt_transport, _DisconnectPid, disconnect} ->
                    ok
            after 1000 ->
                ct:fail(disconnect_timeout)
            end
        end).

incoming_packet_too_large_v5_test(_Config) ->
    with_max_incoming_packet_size(
        100,
        fun() ->
            SessionPid = connect_session(<<"test-incoming-packet-too-large">>, #{
                transport => self()
            }, #{}),
            Publish = #{
                type => publish,
                topic => <<"test">>,
                payload => binary:copy(<<"x">>, 200),
                qos => 0
            },
            {ok, PublishMsg} = mqtt_packet_map:encode(5, Publish),
            {error, packet_too_large} = mqtt_sessions:incoming_data(SessionPid, PublishMsg),
            receive
                {mqtt_transport, SessionPid, disconnect} ->
                    ok
            after 1000 ->
                ct:fail(disconnect_timeout)
            end,
            mqtt_sessions_process:kill(SessionPid)
        end).

outgoing_packet_too_large_v5_test(_Config) ->
    SessionPid = connect_session(<<"test-outgoing-packet-too-large">>, #{
        transport => self()
    }, #{
        maximum_packet_size => 100
    }),
    Subscribe = #{
        type => subscribe,
        topics => [ <<"test-outgoing-packet-too-large">> ]
    },
    {ok, SubMsg} = mqtt_packet_map:encode(5, Subscribe),
    ok = mqtt_sessions:incoming_data(SessionPid, SubMsg),
    receive
        {mqtt_transport, SessionPid, SubAckMsgBin} when is_binary(SubAckMsgBin) ->
            {ok, {SubAck, <<>>}} = mqtt_packet_map:decode(SubAckMsgBin),
            #{
                type := suback,
                acks := [ {ok, 0} ]
            } = SubAck,
            ok
    after 1000 ->
        ct:fail(suback_timeout)
    end,
    Publish = #{
        type => publish,
        topic => <<"test-outgoing-packet-too-large">>,
        payload => binary:copy(<<"x">>, 200),
        qos => 0
    },
    ok = mqtt_sessions:publish(Publish, undefined),
    receive
        {mqtt_transport, SessionPid, disconnect} ->
            ok
    after 1000 ->
        ct:fail(disconnect_timeout)
    end,
    mqtt_sessions_process:kill(SessionPid).

incoming_messages_rate_limit_qos1_test(_Config) ->
    with_incoming_messages_limit(
        1,
        0,
        fun() ->
            SessionPid = connect_session(<<"test-incoming-messages-rate-limit-qos1">>, #{
                transport => self()
            }, #{}),
            Publish = #{
                type => publish,
                topic => <<"test">>,
                payload => <<"hello">>,
                qos => 1,
                packet_id => 10
            },
            {ok, PublishMsg} = mqtt_packet_map:encode(5, Publish),
            ok = mqtt_sessions:incoming_data(SessionPid, PublishMsg),
            receive
                {mqtt_transport, SessionPid, Ack1Bin} when is_binary(Ack1Bin) ->
                    {ok, {Ack1, <<>>}} = mqtt_packet_map:decode(Ack1Bin),
                    #{
                        type := puback,
                        packet_id := 10,
                        reason_code := ?MQTT_RC_SUCCESS
                    } = Ack1
            after 1000 ->
                ct:fail(puback_timeout)
            end,
            Publish2 = Publish#{ packet_id => 11 },
            {ok, PublishMsg2} = mqtt_packet_map:encode(5, Publish2),
            ok = mqtt_sessions:incoming_data(SessionPid, PublishMsg2),
            receive
                {mqtt_transport, SessionPid, Ack2Bin} when is_binary(Ack2Bin) ->
                    {ok, {Ack2, <<>>}} = mqtt_packet_map:decode(Ack2Bin),
                    #{
                        type := puback,
                        packet_id := 11,
                        reason_code := ?MQTT_RC_QUOTA_EXCEEDED
                    } = Ack2
            after 1000 ->
                ct:fail(puback_timeout)
            end,
            mqtt_sessions_process:kill(SessionPid)
        end).

incoming_messages_burst_limit_qos1_test(_Config) ->
    with_incoming_messages_limit(
        1,
        1,
        fun() ->
            SessionPid = connect_session(<<"test-incoming-messages-burst-limit-qos1">>, #{
                transport => self()
            }, #{}),
            Publish1 = #{
                type => publish,
                topic => <<"test">>,
                payload => <<"hello">>,
                qos => 1,
                packet_id => 20
            },
            {ok, PublishMsg1} = mqtt_packet_map:encode(5, Publish1),
            ok = mqtt_sessions:incoming_data(SessionPid, PublishMsg1),
            receive
                {mqtt_transport, SessionPid, Ack1Bin} when is_binary(Ack1Bin) ->
                    {ok, {Ack1, <<>>}} = mqtt_packet_map:decode(Ack1Bin),
                    #{
                        type := puback,
                        packet_id := 20,
                        reason_code := ?MQTT_RC_SUCCESS
                    } = Ack1
            after 1000 ->
                ct:fail(puback_timeout)
            end,
            Publish2 = Publish1#{ packet_id => 21 },
            {ok, PublishMsg2} = mqtt_packet_map:encode(5, Publish2),
            ok = mqtt_sessions:incoming_data(SessionPid, PublishMsg2),
            receive
                {mqtt_transport, SessionPid, Ack2Bin} when is_binary(Ack2Bin) ->
                    {ok, {Ack2, <<>>}} = mqtt_packet_map:decode(Ack2Bin),
                    #{
                        type := puback,
                        packet_id := 21,
                        reason_code := ?MQTT_RC_SUCCESS
                    } = Ack2
            after 1000 ->
                ct:fail(puback_timeout)
            end,
            Publish3 = Publish1#{ packet_id => 22 },
            {ok, PublishMsg3} = mqtt_packet_map:encode(5, Publish3),
            ok = mqtt_sessions:incoming_data(SessionPid, PublishMsg3),
            receive
                {mqtt_transport, SessionPid, Ack3Bin} when is_binary(Ack3Bin) ->
                    {ok, {Ack3, <<>>}} = mqtt_packet_map:decode(Ack3Bin),
                    #{
                        type := puback,
                        packet_id := 22,
                        reason_code := ?MQTT_RC_QUOTA_EXCEEDED
                    } = Ack3
            after 1000 ->
                ct:fail(puback_timeout)
            end,
            mqtt_sessions_process:kill(SessionPid)
        end).

incoming_messages_rate_limit_qos0_test(_Config) ->
    with_incoming_messages_limit(
        1,
        0,
        fun() ->
            SessionPid = connect_session(<<"test-incoming-messages-rate-limit-qos0">>, #{
                transport => self()
            }, #{}),
            Subscribe = #{
                type => subscribe,
                topics => [ <<"test-rate-limit-qos0">> ]
            },
            {ok, SubMsg} = mqtt_packet_map:encode(5, Subscribe),
            ok = mqtt_sessions:incoming_data(SessionPid, SubMsg),
            receive
                {mqtt_transport, SessionPid, SubAckMsgBin} when is_binary(SubAckMsgBin) ->
                    {ok, {SubAck, <<>>}} = mqtt_packet_map:decode(SubAckMsgBin),
                    #{
                        type := suback,
                        acks := [ {ok, 0} ]
                    } = SubAck
            after 1000 ->
                ct:fail(suback_timeout)
            end,
            Publish = #{
                type => publish,
                topic => <<"test-rate-limit-qos0">>,
                payload => <<"hello">>,
                qos => 0
            },
            {ok, PublishMsg} = mqtt_packet_map:encode(5, Publish),
            ok = mqtt_sessions:incoming_data(SessionPid, PublishMsg),
            receive
                {mqtt_transport, SessionPid, PubMsgBin} when is_binary(PubMsgBin) ->
                    {ok, {PubMsgReceived, <<>>}} = mqtt_packet_map:decode(PubMsgBin),
                    #{
                        type := publish,
                        payload := <<"hello">>
                    } = PubMsgReceived
            after 1000 ->
                ct:fail(publish_timeout)
            end,
            ok = mqtt_sessions:incoming_data(SessionPid, PublishMsg),
            receive
                {mqtt_transport, SessionPid, UnexpectedBin} when is_binary(UnexpectedBin) ->
                    {ok, {Unexpected, <<>>}} = mqtt_packet_map:decode(UnexpectedBin),
                    ct:fail({unexpected_message, Unexpected})
            after 200 ->
                ok
            end,
            mqtt_sessions_process:kill(SessionPid)
        end).

anonymous_websocket_disconnect_timeout_test(_Config) ->
    ClientId = <<"test-anon-websocket-timeout">>,
    SessionPid = connect_session(ClientId, #{
        transport => self(),
        context_prefs => #{
            origin => websocket,
            is_crawler => false
        }
    }, #{
        session_expiry_interval => 300
    }),
    ok = disconnect_session(SessionPid, 300),
    assert_disconnect_timer(SessionPid, 30),
    mqtt_sessions_process:kill(SessionPid).

crawler_websocket_disconnect_timeout_test(_Config) ->
    ClientId = <<"test-crawler-websocket-timeout">>,
    SessionPid = connect_session(ClientId, #{
        transport => self(),
        context_prefs => #{
            origin => websocket,
            is_crawler => true
        }
    }, #{
        session_expiry_interval => 300
    }),
    ok = disconnect_session(SessionPid, 300),
    assert_disconnect_timer(SessionPid, 10),
    mqtt_sessions_process:kill(SessionPid).

authenticated_websocket_disconnect_timeout_test(_Config) ->
    ClientId = <<"test-auth-websocket-timeout">>,
    SessionPid = connect_session(ClientId, #{
        transport => self(),
        context_prefs => #{
            origin => websocket
        }
    }, #{
        username => <<"user">>,
        password => <<"secret">>,
        session_expiry_interval => 120
    }),
    ok = disconnect_session(SessionPid, 120),
    assert_disconnect_timer(SessionPid, 120),
    mqtt_sessions_process:kill(SessionPid).

connect_session(ClientId, Options, ConnectProps) ->
    Connect = #{
        type => connect,
        protocol_name => <<"MQTT">>,
        protocol_version => 5,
        clean_start => true,
        client_id => ClientId,
        will_flag => false,
        username => maps:get(username, ConnectProps, <<>>),
        password => maps:get(password, ConnectProps, <<>>),
        properties => maps:without([username, password], ConnectProps)
    },
    {ok, ConnectMsg} = mqtt_packet_map:encode(5, Connect),
    {ok, {SessionPid, <<>>}} = mqtt_sessions:incoming_connect(ConnectMsg, Options),
    receive
        {mqtt_transport, SessionPid, MsgBin} when is_binary(MsgBin) ->
            {ok, {ConnAck, <<>>}} = mqtt_packet_map:decode(MsgBin),
            #{ type := connack, reason_code := 0 } = ConnAck,
            SessionPid
    after 1000 ->
        ct:fail(connect_timeout)
    end.

disconnect_session(SessionPid, SessionExpiryInterval) ->
    Disconnect = #{
        type => disconnect,
        reason_code => 0,
        properties => #{
            session_expiry_interval => SessionExpiryInterval
        }
    },
    {ok, DisconnectMsg} = mqtt_packet_map:encode(5, Disconnect),
    ok = mqtt_sessions:incoming_data(SessionPid, DisconnectMsg),
    receive
        {mqtt_transport, SessionPid, disconnect} ->
            ok
    after 1000 ->
        ct:fail(disconnect_timeout)
    end.

assert_disconnect_timer(SessionPid, ExpectedSeconds) ->
    WillPid = element(18, sys:get_state(SessionPid)),
    WillState = sys:get_state(WillPid),
    TimerRef = element(9, WillState),
    Remaining = erlang:read_timer(TimerRef),
    MinRemaining = (ExpectedSeconds * 1000) - 1000,
    MaxRemaining = ExpectedSeconds * 1000,
    true = is_integer(Remaining) andalso Remaining =< MaxRemaining andalso Remaining >= MinRemaining.

with_max_incoming_packet_size(MaxPacketSize, Fun) ->
    OldValue = application:get_env(mqtt_sessions, max_incoming_packet_size),
    application:set_env(mqtt_sessions, max_incoming_packet_size, MaxPacketSize),
    try
        Fun()
    after
        case OldValue of
            {ok, Value} ->
                application:set_env(mqtt_sessions, max_incoming_packet_size, Value);
            undefined ->
                application:unset_env(mqtt_sessions, max_incoming_packet_size)
        end
    end.

with_incoming_messages_limit(Rate, Burst, Fun) ->
    OldRate = application:get_env(mqtt_sessions, max_incoming_messages_rate),
    OldBurst = application:get_env(mqtt_sessions, max_incoming_messages_burst),
    application:set_env(mqtt_sessions, max_incoming_messages_rate, Rate),
    application:set_env(mqtt_sessions, max_incoming_messages_burst, Burst),
    try
        Fun()
    after
        case OldRate of
            {ok, ValueRate} ->
                application:set_env(mqtt_sessions, max_incoming_messages_rate, ValueRate);
            undefined ->
                application:unset_env(mqtt_sessions, max_incoming_messages_rate)
        end,
        case OldBurst of
            {ok, ValueBurst} ->
                application:set_env(mqtt_sessions, max_incoming_messages_burst, ValueBurst);
            undefined ->
                application:unset_env(mqtt_sessions, max_incoming_messages_burst)
        end
    end.
