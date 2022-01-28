%% @doc Sidejobs for handling topic subscriptions
%% @author Marc Worrell <marc@worrell.nl>
%% @copyright 2018-2022 Marc Worrell

%% Copyright 2018-2022 Marc Worrell
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

-module(mqtt_sessions_job).

-export([
    publish/5,
    publish_retained/6,
    publish_job/6,
    publish_retained_job/6,
    publish1/6
]).


-include_lib("kernel/include/logger.hrl").
-include_lib("router/include/router.hrl").
-include_lib("../include/mqtt_sessions.hrl").

-spec publish( atom(), mqtt_sessions:topic(), list(), mqtt_packet_map:mqtt_message(), term()) -> {ok, pid() | undefined} | {error, overload}.
publish(_Pool, _Topic, [], _Msg, _PublisherContext) ->
    {ok, undefined};
publish(Pool, Topic, Routes, Msg, PublisherContext) ->
    case sidejob_supervisor:spawn(
        ?MQTT_SESSIONS_JOBS,
        {?MODULE, publish_job, [ Pool, Topic, Routes, Msg, PublisherContext, self() ]})
    of
        {ok, _JobPid} = OK ->
            OK;
        {error, overload} ->
            ?LOG_DEBUG("MQTT sidejob overload, delaying job ~p ...", [ Topic ]),
            timer:sleep(100),
            sidejob_supervisor:spawn(
                    ?MQTT_SESSIONS_JOBS,
                    {?MODULE, publish_job, [ Pool, Topic, Routes, Msg, PublisherContext, self() ]})
        % publish(Pool, Topic, Routes, Msg, PublisherContext)
    end.

-spec publish_retained( atom(), mqtt_sessions:topic(), list(), mqtt_sessions:callback(), map(), term()) -> ok | {error, overload}.
publish_retained(_Pool, _TopicFilter, [], _Subscriber, _Options, _SubscriberContext) ->
    ok;
publish_retained(Pool, TopicFilter, Ms, Subscriber, Options, SubscriberContext) ->
    case sidejob_supervisor:spawn(
        ?MQTT_SESSIONS_JOBS,
        {?MODULE, publish_retained_job, [ Pool, TopicFilter, Ms, Subscriber, Options, SubscriberContext ]})
    of
        {ok, _JobPid} ->
            ok;
        {error, overload} ->
            ?LOG_DEBUG("MQTT sidejob overload, delaying retained job ~p ...", [ TopicFilter ]),
            timer:sleep(100),
            sidejob_supervisor:spawn(
                    ?MQTT_SESSIONS_JOBS,
                    {?MODULE, publish_retained_job, [ Pool, TopicFilter, Ms, Subscriber, Options, SubscriberContext ]})
            % publish_retained(Pool, TopicFilter, Ms, Subscriber, Options, SubscriberContext)
    end.


publish_job(Pool, Topic, Routes, Msg, PublisherContext, PublishedPid) ->
    lists:foreach(
        fun(Route) ->
            publish1(Pool, Topic, Route, Msg, PublisherContext, PublishedPid)
        end,
        Routes).

-spec publish_retained_job( atom(), mqtt_sessions:topic(), list(), mqtt_sessions:callback(), map(), term()) -> ok.
publish_retained_job(Pool, TopicFilter, Ms, Subscriber, Options, SubscriberContext) ->
    Runtime = mqtt_sessions:runtime(),
    lists:foreach(
        fun({#{ topic := Topic } = Msg, PublisherContext}) ->
            case Runtime:is_allowed(subscribe, Topic, Msg, SubscriberContext) of
                true ->
                    Bound = bind(Topic, TopicFilter),
                    Dest = {Subscriber, undefined, Options},
                    publish1(Pool, Topic, #route{ bound_args = Bound, destination = Dest }, Msg, PublisherContext, none);
                false ->
                    ok
            end
        end,
        Ms).


publish1(Pool, Topic, #route{ bound_args = Bound, destination = Dest }, Msg, PublisherContext, PublisherPid) ->
    case is_no_local(Dest, PublisherPid) of
        true ->
            ok;
        false ->
            {Callback, _OwnerPid, Options} = Dest,
            Msg1 = case maps:get(retain, Msg, false) of
                true ->
                    case maps:get(retain_as_published, Msg, false) of
                        false -> Msg#{ retain => false };
                        true -> Msg
                    end;
                false -> Msg
            end,
            MqttMsg = Options#{
                type => publish,
                pool => Pool,
                topic => Topic,
                topic_bindings => Bound,
                message => Msg1,
                publisher_context => PublisherContext
            },
            callback(Callback, MqttMsg)
    end.


callback({io, format, A}, MqttMsg) ->
    erlang:apply(io, format, A ++ [ [ MqttMsg ] ]);
callback({M, F, A} , MqttMsg) ->
    erlang:apply(M, F, A ++ [ MqttMsg ]);
callback(Pid, MqttMsg) when is_pid(Pid) ->
    Pid ! {mqtt_msg, MqttMsg}.

is_no_local(_, none) -> false;
is_no_local({_Callback, OwnerPid, #{ no_local := true }}, OwnerPid) -> true;
is_no_local(_Destination, _Pid) -> false.

%% Bind variables from the match to the path
bind(Path, Match) ->
    bind(Path, Match, []).

bind([], [], Acc) ->
    lists:reverse(Acc);
bind(P, [ '#' ], Acc) ->
    lists:reverse([{'#', P}|Acc]);
bind([H|Path], [ '+' | Match ], Acc) ->
    bind(Path, Match, [H|Acc]);
bind([_|Path], [ _ | Match ], Acc) ->
    bind(Path, Match, Acc).
