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

-module(mqtt_sessions_incoming).

-export([
    incoming_connect/3,
    send_connack_error/3
]).


-include("../include/mqtt_sessions.hrl").
-include_lib("mqtt_packet_map/include/mqtt_packet_map.hrl").


%% @doc Handle an incoming connect message, the package MUST be a connect package.
-spec incoming_connect( Pool :: atom(), mqtt_packet_map:mqtt_packet(), mqtt_sessions:msg_options() ) ->
        {ok, mqtt_sessions:session_ref()}.
incoming_connect(Pool, #{ type := connect, client_id := <<>> } = Msg, Options) ->
    start_session(Pool, Msg, Options);
incoming_connect(Pool, #{ type := connect, client_id := ClientId } = Msg, Options) ->
    % Check client_id in connect message
    case mqtt_sessions_registry:find_session(Pool, ClientId) of
        {ok, SessionRef} ->
            ok = mqtt_sessions_process:incoming_connect(SessionRef, Msg, Options),
            {ok, SessionRef};
        {error, notfound} ->
            start_session(Pool, Msg, Options)
        % {error, _} = Error ->
        %     send_connack_error(?MQTT_RC_ERROR, Msg, Options),
        %     Error
    end.

%% @doc The session pool returned an error when trying to map the client-id
send_connack_error(ReasonCode, #{ protocol_version := PV }, Options) ->
    AckMsg = #{
        type => connack,
        reason_code => ReasonCode
    },
    AckMsgB = mqtt_packet_map:encode(PV, AckMsg),
    case maps:get(transport, Options, undefined) of
        undefined ->
            ok;
        Pid when is_pid(Pid) ->
            Pid ! {mqtt_transport, self(), AckMsgB},
            Pid ! {mqtt_transport, self(), disconnect};
        F when is_function(F) ->
            F(AckMsgB),
            F(disconnect)
    end.

-spec start_session( atom(), mqtt_packet_map:mqtt_packet(), mqtt_sessions:msg_options() ) -> {ok, pid()}.
start_session(Pool, #{ client_id := ClientId } = Msg, MsgOptions) ->
    SessionOptions = #{
        peer_ip => maps:get(peer_ip, MsgOptions, undefined),
        context_prefs => maps:get(context_prefs, MsgOptions, #{})
    },
    {ok, {Pid, _NewClientId}} = mqtt_sessions_process_sup:new_session(Pool, ClientId, SessionOptions),
    ok = mqtt_sessions_process:incoming_connect(Pid, Msg, MsgOptions),
    {ok, Pid}.
