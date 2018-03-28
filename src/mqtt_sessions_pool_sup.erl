%% @doc Start a supervisor with a session registry and a topic tree

-module(mqtt_sessions_pool_sup).

-behaviour(supervisor).

-export([
    start_link/1,
    init/1,
    name/1
    ]).

-spec start_link( Pool :: atom() ) -> {ok, pid()} | {error, term()}.
start_link(Pool) ->
    supervisor:start_link({local, name(Pool)}, ?MODULE, [ Pool ]).

-spec init([ atom() ]) -> {ok, {supervisor:sup_flags(), list( supervisor:child_spec() )}}.
init([Pool]) ->
    {ok, {
        #{
            strategy => one_for_all,
            intensity => 5,
            period => 10
        },
        [
            #{
                id => router,
                start => {mqtt_sessions_router, start_link, [ Pool ]},
                type => worker
            },
            #{
                id => mqtt_sessions_retain,
                start => {mqtt_sessions_retain, start_link, [ Pool ]},
                type => worker
            },
            #{
                id => mqtt_sessions_will_sup,
                start => {mqtt_sessions_will_sup, start_link, [ Pool ]},
                type => supervisor
            },
            #{
                id => mqtt_sessions_process_sup,
                start => {mqtt_sessions_process_sup, start_link, [ Pool ]},
                type => supervisor
            }
        ]}}.

-spec name( atom() ) -> atom().
name( Pool ) ->
    list_to_atom(atom_to_list(Pool) ++ "$poolsup").
