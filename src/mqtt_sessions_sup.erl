%% @doc Main supervisor, starts registry and default pool.
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

-module(mqtt_sessions_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    init/1
    ]).

-include("mqtt_sessions.hrl").


-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([ atom() ]) -> {ok, {supervisor:sup_flags(), list( supervisor:child_spec() )}}.
init([]) ->
    {ok, {
        #{
            strategy => one_for_all,
            intensity => 5,
            period => 10
        },
        [
            #{
                id => mqtt_sessions_registry,
                start => {mqtt_sessions_registry, start_link, []},
                type => worker
            },
            #{
                id => mqtt_sessions_pool_sup,
                start => {mqtt_sessions_pool_sup, start_link, [ ?MQTT_SESSIONS_DEFAULT ]},
                type => supervisor
            }
        ]}}.

