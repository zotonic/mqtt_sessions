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
    incoming_message/4
]).


-include("../include/mqtt_sessions.hrl").
-include_lib("mqtt_packet_map/include/mqtt_packet_map.hrl").


%% @doc Handle an incoming message, if there is an session-ref then forward the message to there.
%%      If there is no session-ref then the package MUST be a connect package.
-spec incoming_message( Pool :: atom(), mqtt_sessions:opt_session_ref(), mqtt_packet_map:mqtt_packet(), mqtt_sessions:msg_options() ) ->
        {ok, mqtt_sessions:session_ref()} | {error, term()}.
incoming_message(Pool, undefined, #{ type := connect, client_id := <<>> } = Msg, MsgOptions) ->
    SessionOptions = #{
        peer_ip => maps:get(peer_ip, MsgOptions, undefined)
    },
    {ok, {Pid, _ClientId}} = mqtt_sessions_process_sup:new_session(Pool, SessionOptions),
    ok = mqtt_sessions_process:incoming_message(Pid, Msg, MsgOptions),
    {ok, Pid};
incoming_message(Pool, undefined, #{ type := connect, client_id := ClientId, clean_start := true } = Msg, Options) ->
    % a. Close existing client (if is running)
    % b. Continue as if no client id present
    % WARNING: we should use the client-id, but in our case we divert from the specs
    mqtt_sessions_registry:kill_session(Pool, ClientId),
    incoming_message(Pool, undefined, Msg#{ client_id => <<>> }, Options);
incoming_message(Pool, undefined, #{ type := connect, client_id := ClientId, clean_start := false } = Msg, Options) ->
    % Check client_id in connect message
    case mqtt_sessions_registry:find_session(Pool, ClientId) of
        {ok, SessionRef} ->
            ok = mqtt_sessions_process:incoming_message(SessionRef, Msg, Options),
            {ok, SessionRef};
        {error, _} = Error ->
            maybe_send_connack(Options, Error)
    end;
incoming_message(Pool, undefined, Msg, _Options) ->
    lager:error("MQTT msg for unknown session in ~p: ~p", [ Pool, Msg ]),
    {error, must_connect};
incoming_message(Pool, ClientId, Msg, Options) when is_binary(ClientId) ->
    case mqtt_sessions_registry:find_session(Pool, ClientId) of
        {ok, SessionRef} ->
            ok = mqtt_sessions_process:incoming_message(SessionRef, Msg, Options),
            {ok, SessionRef};
        {error, _} = Error ->
            Error
    end;
incoming_message(_Pool, SessionRef, Msg, Options) when is_pid(SessionRef) ->
    ok = mqtt_sessions_process:incoming_message(SessionRef, Msg, Options),
    {ok, SessionRef}.

%% @doc Refuse unknown client-ids, as we want to have full control over the client identifiers.
%%      This is not according to the specs, but we do it out of security concerns.
maybe_send_connack(Options, Error) ->
    case maps:get(transport, Options, undefined) of
        undefined ->
            Error;
        Pid when is_pid(Pid) ->
            Pid ! {reply, #{ type => connack, reason_code => ?MQTT_RC_CLIENT_ID_INVALID }},
            Pid ! {reply, disconnect},
            case self() of
                Pid -> {ok, undefined};
                _ -> Error
            end
    end.

