%% @doc Supervisor for watchdog processes that process wills

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
