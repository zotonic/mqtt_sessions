%% @author Marc Worrell <marc@worrell.nl>
%% @copyright 2018-2026 Marc Worrell
%% @doc Helpers for MQTT packet sizing and validation.
%% @end

%% Copyright 2018-2026 Marc Worrell
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

-module(mqtt_sessions_packet).

-export([
    check_packet_size/2,
    packet_size/1
]).

-spec check_packet_size(Data, MaxPacketSize) -> ok | incomplete | {error, Reason} when
    Data :: binary(),
    MaxPacketSize :: pos_integer() | undefined,
    Reason :: malformed_packet | packet_too_large.
check_packet_size(_Data, undefined) ->
    ok;
check_packet_size(Data, MaxPacketSize) ->
    case packet_size(Data) of
        {ok, PacketSize} when PacketSize =< MaxPacketSize ->
            ok;
        {ok, _PacketSize} ->
            {error, packet_too_large};
        {error, _} = Error ->
            Error;
        incomplete ->
            incomplete
    end.

-spec packet_size(Data) -> {ok, PacketSize} | incomplete | {error, Reason} when
    Data :: binary(),
    PacketSize :: non_neg_integer(),
    Reason :: malformed_packet.
packet_size(<<_PacketType:8, Rest/binary>>) ->
    case remaining_length(Rest, 0, 1, 0) of
        {ok, RemainingLength, LengthBytes} ->
            {ok, 1 + LengthBytes + RemainingLength};
        {error, _} = Error ->
            Error;
        incomplete ->
            incomplete
    end;
packet_size(<<>>) ->
    incomplete.

-spec remaining_length(Rest, Value, Multiplier, Count) -> {ok, RemainingLength, LengthBytes} | incomplete | {error, Reason} when
    Rest :: binary(),
    Value :: non_neg_integer(),
    Multiplier :: pos_integer(),
    Count :: non_neg_integer(),
    RemainingLength :: non_neg_integer(),
    LengthBytes :: pos_integer(),
    Reason :: malformed_packet.
remaining_length(<<>>, _Value, _Multiplier, _Count) ->
    incomplete;
remaining_length(_Rest, _Value, _Multiplier, Count) when Count >= 4 ->
    % MQTT spec: Remaining Length MUST be encoded in at most 4 bytes
    {error, malformed_packet};
remaining_length(<<Byte:8, Rest/binary>>, Value, Multiplier, Count) ->
    Value1 = Value + ((Byte band 16#7f) * Multiplier),
    case Byte band 16#80 of
        16#80 ->
            remaining_length(Rest, Value1, Multiplier * 128, Count + 1);
        0 when Value1 > 268435455 ->
            % MQTT spec: Remaining Length MUST NOT exceed 268435455 (0x0FFFFFFF)
            {error, malformed_packet};
        0 ->
            {ok, Value1, Count + 1}
    end.
