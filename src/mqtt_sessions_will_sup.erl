%% @doc Supervisor for watchdog processes that process wills
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

-module(mqtt_sessions_will_sup).

-export([
    start/2,
    start_link/1,
    init/1
]).

-spec start( atom(), pid() ) -> {ok, pid()}.
start(Pool, SessionPid) ->
    supervisor:start_child(name(Pool), [ SessionPid ]).


-spec start_link( atom() ) -> {ok, pid()} | {error, term()}.
start_link( Pool ) ->
    supervisor:start_link({local, name(Pool)}, ?MODULE, [ Pool ]).

-spec init([ atom() ]) -> {ok, {supervisor:sup_flags(), list( supervisor:child_spec() )}}.
init([ Pool ]) ->
    {ok, {
        #{
            strategy => simple_one_for_one,
            intensity => 5,
            period => 10
        },
        [
            #{
                id => session_process,
                start => {mqtt_sessions_will, start_link, [ Pool ]},
                type => worker,
                restart => temporary
            }
        ]}}.

-spec name( atom() ) -> atom().
name( Pool ) ->
    list_to_atom(atom_to_list(Pool) ++ "$willsup").
