%% @doc MQTT sessions runtime ACL interface.
%% @author Marc Worrell <marc@worrell.nl>
%% @copyright 2018-2019 Marc Worrell

%% Copyright 2018-2019 Marc Worrell
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

-module(mqtt_sessions_runtime).

-export([
    vhost_pool/1,
    pool_default/0,

    new_user_context/3,
    connect/4,
    control_message/3,
    reauth/2,
    is_allowed/4,
    is_valid_message/3
    ]).

-type user_context() :: term().
-type topic() :: list( binary() ).

-callback vhost_pool( binary() ) -> {ok, atom()} | {error, term()}.
-callback pool_default() -> {ok, atom()} | {error, term()}.

-callback new_user_context( atom(), binary(), mqtt_sessions:session_options() ) -> term().
-callback connect( mqtt_packet_map:mqtt_packet(), boolean(), map(), user_context() ) -> {ok, mqtt_packet_map:mqtt_packet(), user_context()} | {error, term()}.
-callback reauth( mqtt_packet_map:mqtt_packet(), user_context() ) -> {ok, mqtt_packet_map:mqtt_packet(), user_context()} | {error, term()}.
-callback is_allowed( publish | subscribe, topic(), mqtt_packet_map:mqtt_packet(), user_context()) -> boolean().
-callback is_valid_message( mqtt_packet_map:mqtt_packet(), mqtt_sessions:msg_options(), user_context() ) -> boolean().
-callback control_message( topic(), mqtt_packet_map:mqtt_packet(), user_context() ) -> {ok, user_context()}.

-export_type([
    user_context/0,
    topic/0
]).

-define(none(A), (A =:= undefined orelse A =:= <<>>)).

-include_lib("mqtt_packet_map/include/mqtt_packet_map.hrl").
-include("../include/mqtt_sessions.hrl").

-spec vhost_pool( binary() ) -> {ok, atom()} | {error, term()}.
vhost_pool( _VHost ) ->
    pool_default().

-spec pool_default() -> {ok, atom()} | {error, term()}.
pool_default() ->
    {ok, ?MQTT_SESSIONS_DEFAULT}.


-spec new_user_context( atom(), binary(), mqtt_sessions:session_options() ) -> term().
new_user_context( Pool, ClientId, Options ) ->
    #{
        pool => Pool,
        client_id => ClientId,
        routing_id => maps:get(routing_id, Options, undefined),
        peer_ip => maps:get(peer_ip, Options, undefined),
        user => undefined
    }.

-spec control_message( topic(), mqtt_packet_map:mqtt_packet(), user_context() ) -> {ok, user_context()}.
control_message(_Topic, _Packet, UserContext) ->
    {ok, UserContext}.

-spec connect( mqtt_packet_map:mqtt_packet(), boolean(), mqtt_session:msg_options(), user_context()) -> {ok, mqtt_packet_map:mqtt_packet(), user_context()} | {error, term()}.
connect(#{ type := connect, username := U, password := P }, IsSessionPresent, Options, UserContext) when ?none(U), ?none(P) ->
    % Anonymous login - user must stay anonymous (regardless of IsSessionPresent)
    case maps:get(user, UserContext, undefined) of
        undefined ->
            ConnAck = #{
                type => connack,
                reason_code => ?MQTT_RC_SUCCESS,
                session_present => IsSessionPresent
            },
            {ok, ConnAck, UserContext#{
                user => undefined,
                peer_ip => maps:get(peer_ip, Options, undefined)
            }};
        _SomeUser ->
            ConnAck = #{
                type => connack,
                reason_code => ?MQTT_RC_NOT_AUTHORIZED
            },
            {ok, ConnAck, UserContext}
    end;
connect(#{ type := connect, username := U, password := P }, IsSessionPresent, Options, UserContext) when not ?none(U), not ?none(P) ->
    % User logs on using username/password
    case maps:get(user, UserContext, undefined) of
        undefined ->
            % ... check username/password
            % ... log on user on UserContext
            ConnAck = #{
                type => connack,
                reason_code => ?MQTT_RC_SUCCESS,
                session_present => IsSessionPresent
            },
            {ok, ConnAck, UserContext#{
                user => U,
                peer_ip => maps:get(peer_ip, Options, undefined)
            }};
        U when IsSessionPresent ->
            % ... check username/password
            ConnAck = #{
                type => connack,
                reason_code => ?MQTT_RC_SUCCESS,
                session_present => IsSessionPresent
            },
            {ok, ConnAck, UserContext#{
                peer_ip => maps:get(peer_ip, Options, undefined)
            }};
        _SomeUser ->
            ConnAck = #{
                type => connack,
                reason_code => ?MQTT_RC_NOT_AUTHORIZED
            },
            {ok, ConnAck, UserContext}
    end;
connect(#{ type := connect, properties := #{ authentication_method := _AuthMethod } = Props }, _IsSessionPresent, _Options, UserContext) ->
    % User logs on using extended authentication method
    _AuthData = maps:get(authentication_data, Props, undefined),
    % ... handle extended authentication handshake
    ConnAck = #{
        type => connack,
        reason_code => ?MQTT_RC_NOT_AUTHORIZED
    },
    {ok, ConnAck, UserContext};
connect(_Packet, _IsSessionPresent, _Options, UserContext) ->
    % Extended authentication
    ConnAck = #{
        type => connack,
        reason_code => ?MQTT_RC_NOT_AUTHORIZED
    },
    {ok, ConnAck, UserContext}.


%% @doc Re-authentication. This is called when the client requests a re-authentication (or replies in a AUTH re-authentication).
-spec reauth( mqtt_packet_map:mqtt_packet(), user_context()) -> {ok, mqtt_packet_map:mqtt_packet(), user_context()} | {error, term()}.
reauth(#{ type := auth }, _UserContext) ->
    {error, notsupported}.


-spec is_allowed( publish | subscribe, topic(), mqtt_packet_map:mqtt_packet(), user_context()) -> boolean().
is_allowed(publish, _Topic, _Packet, _UserContext) ->
    true;
is_allowed(subscribe, _Topic, _Packet, _UserContext) ->
    true.


%% @doc Check a message and its options before it is processed. Used for http connections with authentication cookies.
-spec is_valid_message( mqtt_packet_map:mqtt_packet(), mqtt_sessions:msg_options(), user_context() ) -> boolean().
is_valid_message(_Msg, _Options, _UserContext) ->
    true.

