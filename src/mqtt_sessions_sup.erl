%% @doc Main supervisor, starts registry and default pool.

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

