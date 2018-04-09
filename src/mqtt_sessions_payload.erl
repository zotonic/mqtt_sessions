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

-module(mqtt_sessions_payload).

-export([
    decode/1,
    encode/1
]).


-spec encode( mqtt_packet_map:mqtt_packet() ) -> mqtt_packet_map:mqtt_packet().
encode(#{ type := publish } = Msg) ->
    Payload = maps:get(payload, Msg, <<>>),
    Props = maps:get(properties, Msg, #{}),
    ContentType = case maps:get(content_type, Props, undefined) of
        undefined -> guess_mime(Payload);
        CT when is_binary(CT) -> CT;
        CT when is_list(CT) -> list_to_binary(CT)
    end,
    Props1 = case ContentType of
        undefined -> maps:remove(content_type, Props);
        _ -> Props#{ content_type => ContentType }
    end,
    Msg#{
        payload => encode_payload(Payload, ContentType),
        properties => Props1
    };
encode(Msg) ->
    Msg.


-spec decode( mqtt_packet_map:mqtt_packet() ) -> mqtt_packet_map:mqtt_packet().
decode(#{ type := publish } = Msg) ->
    Payload = maps:get(payload, Msg, undefined),
    Props = maps:get(properties, Msg, #{}),
    ContentType = maps:get(content_type, Props, undefined),
    Msg#{ payload => decode_payload(Payload, ContentType) };
decode(Msg) ->
    Msg.

encode_payload(undefined, _) -> undefined;
encode_payload(N, <<"application/json">>) -> encode_json(N);
encode_payload(B, <<"binary/octet-stream">>) -> z_convert:to_binary(B);
encode_payload(N, <<"text/plain">>) -> z_convert:to_binary(N);
encode_payload(N, <<"text/x-integer">>) -> z_convert:to_binary(N);
encode_payload(N, <<"text/x-number">>) -> z_convert:to_binary(N);
encode_payload(DT, <<"text/x-datetime">>) -> to_binary(z_convert:to_isotime(DT));
encode_payload(T, _) -> to_binary(T).

decode_payload(undefined, _) -> undefined;
decode_payload(B, undefined) -> B;
decode_payload(<<>>, <<"application/json">>) -> #{};
decode_payload(B, <<"application/json">>) -> decode_json(B);
decode_payload(B, <<"binary/octet-stream">>) -> B;
decode_payload(B, <<"text/plain">>) -> B;
decode_payload(B, <<"text/x-integer">>) -> z_convert:to_integer(B);
decode_payload(B, <<"text/x-number">>) -> z_convert:to_float(B);
decode_payload(B, <<"text/x-datetime">>) -> z_convert:to_datetime(B);
decode_payload(B, _) -> B.


guess_mime(null) -> undefined;
guess_mime(undefined) -> undefined;
guess_mime(<<>>) -> undefined;
guess_mime(N) when is_integer(N) -> <<"text/x-integer">>;
guess_mime(N) when is_float(N) -> <<"text/x-number">>;
guess_mime(N) when is_binary(N) ->
    case is_utf8(N) of
        true -> <<"text/plain">>;
        false -> <<"binary/octet-stream">>
    end;
guess_mime(N) when is_boolean(N) -> <<"application/json">>;
guess_mime(N) when is_atom(N) -> <<"text/plain">>;
guess_mime(N) when is_map(N) -> <<"application/json">>;
guess_mime(N) when is_list(N) -> <<"application/json">>;
guess_mime({struct, L}) when is_list(L) -> <<"application/json">>;
guess_mime({{Y,M,D},{H,I,S}}) when
    is_integer(Y), is_integer(M), is_integer(D),
    is_integer(H), is_integer(I), is_integer(S),
    M >= 1, M =< 12, D >= 1, D =< 31 ->
    <<"text/x-datetime">>;
guess_mime(X) ->
    lager:info("MQTT payload unknown type for guess_mime: ~p", [X]),
    <<"binary/octet-stream">>.


is_utf8(<<>>) -> true;
is_utf8(<<_/utf8, R/binary>>) -> is_utf8(R);
is_utf8(_) -> false.


encode_json(undefined) -> undefined;
encode_json(null) -> undefined;
encode_json(true) -> <<"true">>;
encode_json(false) -> <<"false">>;
encode_json({struct, _} = MochiJson) -> jsx:encode(mochijson_to_map(MochiJson));
encode_json(Term) -> jsx:encode(Term).

decode_json(<<>>) -> undefined;
decode_json(<<"null">>) -> undefined;
decode_json(<<"true">>) -> true;
decode_json(<<"false">>) -> false;
decode_json(B) -> jsx:decode(B, [return_maps]).


mochijson_to_map({struct, L}) ->
    maps:from_list([ mochijson_to_map(V) || V <- L ]);
mochijson_to_map({K, V}) ->
    {K, mochijson_to_map(V)};
mochijson_to_map(V) ->
    V.


to_binary(N) when is_atom(N) -> erlang:atom_to_binary(N, utf8);
to_binary(N) when is_integer(N) -> erlang:integer_to_binary(N);
to_binary(N) when is_float(N) -> erlang:float_to_binary(N);
to_binary(L) when is_list(L) -> unicode:characters_to_binary(L, utf8);
to_binary(B) -> z_convert:to_binary(B).
