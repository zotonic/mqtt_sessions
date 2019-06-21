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

-module(mqtt_sessions_retain).

-behaviour(gen_server).

-export([
    start_link/1,
    retain/3,
    lookup/2,
    cleanup/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
    ]).

-record(state, {
        pool :: atom(),
        topics :: ets:tab(),
        messages :: ets:tab()
    }).


% Default expiry interval for retained messages
-define(MESSAGE_EXPIRY_INTERVAL, 24*3600).

% Remove expired messages every minute
-define(PERIODIC_CLEANUP, 60000).


-spec start_link( atom() ) -> {ok, pid()} | {error, term()}.
start_link( Pool ) ->
    gen_server:start_link({local, name(Pool)}, ?MODULE, [Pool], []).

-spec retain( atom(), mqtt_packet_map:mqtt_message(), term() ) -> ok.
retain(Pool, #{ type := publish, topic := Topic } = Msg, PublisherContext) when is_list(Topic) ->
    case is_empty_payload(Msg) of
        true ->
            gen_server:call(name(Pool), {delete, Topic}, infinity);
        false ->
            gen_server:call(name(Pool), {retain, Msg, PublisherContext}, infinity)
    end.

-spec lookup( atom(), list(binary()) ) -> {ok, [ {mqtt_packet_map:mqtt_message(), term()} ]}.
lookup(Pool, TopicFilter) ->
    case is_wildcard(TopicFilter) of
        true ->
            Refs = ets:foldl(
                fun({MsgTopic, _QoS, _Expire, Ref}, Acc) ->
                    case match(MsgTopic, TopicFilter) of
                        true -> [ Ref | Acc ];
                        false -> Acc
                    end
                end,
                [],
                name_topics(Pool)),
            {ok, lookup_refs(Pool, Refs)};
        false ->
            case ets:lookup(name_topics(Pool), TopicFilter) of
                [{_Topic, _QoS, _Expire, Ref}] ->
                    {ok, lookup_refs(Pool, [Ref])};
                [] ->
                    {ok, []}
            end
    end.

lookup_refs(_Pool, []) ->
    [];
lookup_refs(Pool, Refs) ->
    MsgTab = name_messages(Pool),
    lists:foldl(
        fun(Ref, Acc) ->
            case ets:lookup(MsgTab, Ref) of
                [{_Ref, Msg, PublisherContext}] -> [ {Msg, PublisherContext} | Acc ];
                [] -> Acc
            end
        end,
        [],
        Refs).

-spec cleanup( atom() ) -> ok.
cleanup(Pool) ->
    gen_server:cast(name(Pool), cleanup).

% ---------------------------------------------------------------------------------------
% --------------------------- gen_server functions --------------------------------------
% ---------------------------------------------------------------------------------------

-spec init( [ atom() ]) -> {ok, #state{}}.
init([ Pool ]) ->
    erlang:send_after(?PERIODIC_CLEANUP, self(), cleanup),
    {ok, #state{
        pool = Pool,
        topics = ets:new(name_topics(Pool), [ set, protected, named_table ]),
        messages = ets:new(name_messages(Pool), [ set, protected, named_table ])
    }}.

handle_call({retain, #{ topic := Topic } = Msg, PublisherContext}, _From, #state{ topics = Topics, messages = Messages } = State) ->
    Ref = erlang:make_ref(),
    Props = maps:get(properties, Msg, #{}),
    ExpiryInterval = maps:get(message_expiry_interval, Props, ?MESSAGE_EXPIRY_INTERVAL),
    Expire = mqtt_sessions_timestamp:timestamp() + ExpiryInterval,
    QoS = maps:get(qos, Msg, 0),
    ets:insert(Messages, {Ref, Msg, PublisherContext}),
    ets:insert(Topics, {Topic, QoS, Expire, Ref}),
    {reply, ok, State};

handle_call({delete, Topic}, _From, #state{ topics = Topics, messages = Messages } = State) ->
    case ets:lookup(Topics, Topic) of
        [{_Topic, _QoS, _Expire, Ref}] ->
            ets:delete(Topics, Topic),
            ets:delete(Messages, Ref);
        [] ->
            ok
    end,
     {reply, ok, State};

handle_call(Cmd, _From, State) ->
    {stop, {unknown_cmd, Cmd}, State}.

handle_cast(cleanup, #state{ topics = Topics, messages = Messages } = State) ->
    do_cleanup(Topics, Messages),
    {noreply, State};

handle_cast(Cmd, State) ->
    {stop, {unknown_cmd, Cmd}, State}.

handle_info(cleanup, #state{ topics = Topics, messages = Messages } = State) ->
    do_cleanup(Topics, Messages),
    erlang:send_after(?PERIODIC_CLEANUP, self(), cleanup),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

code_change(_Vsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

% ---------------------------------------------------------------------------------------
% ----------------------------- support functions ---------------------------------------
% ---------------------------------------------------------------------------------------

% first([ A | _ ]) -> A;
% first(_) -> undefined.

% second([ _, B | _ ]) -> B;
% second(_) -> undefined.

is_empty_payload(#{ payload := undefined }) -> true;
is_empty_payload(#{ payload := <<>> }) -> true;
is_empty_payload(#{ payload := _}) -> false;
is_empty_payload(#{}) -> true.

do_cleanup(Topics, Messages) ->
    Now = mqtt_sessions_timestamp:timestamp(),
    TRefs = ets:foldl(
        fun({T, _QoS, Expire, Ref}, Acc) ->
            case Expire < Now of
                true -> [ {T,Ref} | Acc ];
                false -> Acc
            end
        end,
        [],
        Topics),
    lists:foreach(
        fun({Topic, Ref}) ->
            ets:delete(Topics, Topic),
            ets:delete(Messages, Ref)
        end,
        TRefs).


match([], []) -> true;
match(_, [ '#' ]) -> true;
match([ _ | Topic ], [ '+' | Filter ]) -> match(Topic, Filter);
match([ A | Topic ], [ A | Filter ]) -> match(Topic, Filter);
match(_, _) -> false.

is_wildcard([]) -> false;
is_wildcard([ '+' | _ ]) -> true;
is_wildcard([ '#' ]) -> true;
is_wildcard([ _ | T ]) -> is_wildcard(T).

-spec name( atom() ) -> atom().
name( Pool ) ->
    list_to_atom(atom_to_list(Pool) ++ "$retain").

-spec name_topics( atom() ) -> atom().
name_topics( Pool ) ->
    list_to_atom(atom_to_list(Pool) ++ "$retaintp").

-spec name_messages( atom() ) -> atom().
name_messages( Pool ) ->
    list_to_atom(atom_to_list(Pool) ++ "$retainms").
