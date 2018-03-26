%% @doc Simple one-for-one supervisor for all mqtt_sessions_process workers.

-module(mqtt_sessions_process_sup).

-behaviour(supervisor).

-export([
    new_session/1,
    start_link/1,
    init/1,
    name/1
    ]).


-spec new_session( atom() ) -> {ok, binary()}.
new_session(Pool) ->
    ClientId = mqtt_sessions_registry:client_id(Pool),
    {ok, Pid} = supervisor:start_child(name(Pool), [ ClientId ]),
    {ok, {Pid, ClientId}}.

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
                start => {mqtt_sessions_process, start_link, [ Pool ]},
                type => worker,
                restart => temporary
            }
        ]}}.

-spec name( atom() ) -> atom().
name( Pool ) ->
    list_to_atom(atom_to_list(Pool) ++ "$sesssup").
