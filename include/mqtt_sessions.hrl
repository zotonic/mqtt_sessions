%% @doc Common definitions for mqtt_sessions

% The default mqtt pool name.
-define(MQTT_SESSIONS_DEFAULT, '-mqtt-').

%% @doc Message sent (or passed) to a subscriber
-record(mqtt_msg, {
        pool :: atom(),
        topic :: list( binary() ),
        topic_bindings :: list( proplists:property() ),
        message :: mqtt_packet_map:mqtt_message(),
        publisher_context :: term(),
        subscriber_context :: term()
    }).
