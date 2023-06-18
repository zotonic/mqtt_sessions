-module(mqtt_sessions_protocol_SUITE).

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
        connect_disconnect_v5_test,
        connect_reconnect_v5_test,
        connect_reconnect_clean_v5_test,
        connect_username_test_v5
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

connect_username_test_v5(_Config) ->
    Connect = #{
        type => connect,
        protocol_name => <<"MQTT">>,
        protocol_version => 5,
        clean_start => true,
        client_id => <<"test1">>,
        will_flag => false,
        username => <<"hostname:user@domain.com">>,
        password => <<"123">>,
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
