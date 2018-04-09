%% @doc Registry for all the sessions in a namespace
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


-module(mqtt_sessions_registry).

-behaviour(gen_server).

-export([
    start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
    ]).

-export([
    kill_session/2,
    find_session/2,
    whois/1,
    register/3,
    client_id/1
    ]).

-define(CLIENT_ID_LEN, 20).

-define(NAME_TO_PID, mqttreg2pid).
-define(PID_TO_NAME, mqttreg2name).

-record(state, {}).

%% @doc Force a session to close, no DISCONNECT to be sent.
-spec kill_session( atom(), binary() ) -> ok.
kill_session( Pool, ClientId ) ->
    case find_session(Pool, ClientId) of
        {ok, Pid} ->
            mqtt_sessions_process:kill(Pid);
        {error, notfound} ->
            ok
    end.


%% @doc Find the session process for the given pool name and client.
-spec find_session( atom(), binary() | pid() ) -> {ok, pid()} | {error, notfound}.
find_session( _Pool, Pid ) when is_pid(Pid) ->
    case erlang:is_process_alive(Pid) of
        true -> {ok, Pid};
        false -> {error, notfound}
    end;
find_session( Pool, ClientId ) ->
    case ets:lookup(?NAME_TO_PID, {Pool, ClientId}) of
        [{_, Pid}] -> {ok, Pid};
        [] -> {error, notfound}
    end.


%% @doc Find the name and pool for the session process
-spec whois( pid() ) -> {ok, {atom(), binary()}} | {error, notfound}.
whois( Pid ) ->
    case ets:lookup(?PID_TO_NAME, Pid) of
        [{_, {Pool, ClientId}}] -> {ok, {Pool, ClientId}};
        [] -> {error, notfound}
    end.

%% @doc Generate a new (random) ClientID of 20 bytes.
%%      A ClientID may be 1 to 23 utf8 encoded bytes.
-spec client_id( atom() ) -> binary().
client_id( _Pool ) ->
    make_any_char_id(?CLIENT_ID_LEN).


%% @doc Register a session with the registry, called by the session.
-spec register( atom(), binary(), pid() ) -> ok | {error, duplicate}.
register( Pool, ClientId, Pid ) ->
    case find_session(Pool, ClientId) of
        {ok, Pid} ->
            ok;
        {error, notfound} ->
            gen_server:call(?MODULE, {register, Pool, ClientId, Pid}, infinity)
    end.

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


% ---------------------------------------------------------------------------------------
% --------------------------- gen_server functions --------------------------------------
% ---------------------------------------------------------------------------------------

-spec init([]) -> {ok, #state{}}.
init([]) ->
    ets:new(?NAME_TO_PID, [ named_table, set, {keypos, 1}, {read_concurrency, true} ]),
    ets:new(?PID_TO_NAME, [ named_table, set, {keypos, 1}, {read_concurrency, true} ]),
    {ok, #state{}}.


handle_call({register, Pool, ClientId, Pid}, _From, State) ->
    Reply = case ets:lookup(?NAME_TO_PID, {Pool, ClientId}) of
        [{_, Pid}] ->
            ok;
        [{_, _Pid}] ->
            {error, duplicate};
        [] ->
            ets:insert(?NAME_TO_PID, {{Pool, ClientId}, Pid}),
            ets:insert(?PID_TO_NAME, {Pid, {Pool, ClientId}}),
            erlang:monitor(process, Pid),
            ok
    end,
    {reply, Reply, State}.

handle_cast(Cmd, State) ->
    {stop, {unknown_command, Cmd}, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason}, State) ->
    case ets:lookup(?PID_TO_NAME, Pid) of
        [{Pid, Name}] ->
            ets:delete(?PID_TO_NAME, Pid),
            ets:delete(?NAME_TO_PID, Name);
        [] ->
            ok
    end,
    {noreply, State}.

code_change(_Vsn, State, _Extra) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

% ---------------------------------------------------------------------------------------
% ---------------------------- Internal functions ---------------------------------------
% ---------------------------------------------------------------------------------------

%% @doc Generate a random key consisting of numbers, upper- and lowercase characters.
-spec make_any_char_id(Length::integer()) -> binary().
make_any_char_id(Len) ->
    << << case N of
              C when C < 26 -> C  + $a;
              C when C < 52 -> C - 26 + $A;
              C -> C - 52 + $0
          end >>
      || N <- random_list(62, Len)
    >>.

random_list(Radix, Length) ->
    N = (radix_bits(Radix) * Length + 7) div 8,
    Val = bin2int(rand_bytes(N)),
    int2list(Val, Radix, Length, []).

int2list(_, _, 0, Acc) ->
    Acc;
int2list(Val, Radix, Length, Acc) ->
    int2list(Val div Radix, Radix, Length-1, [ Val rem Radix | Acc]).

bin2int(Bin) ->
    lists:foldl(fun(N, Acc) -> Acc * 256 + N end, 0, binary_to_list(Bin)).

-spec radix_bits(1..64) -> pos_integer().
radix_bits(N) when N =< 16 -> 4;
radix_bits(N) when N =< 26 -> 5;
radix_bits(_N) -> 6.

-spec rand_bytes(integer()) -> binary().
rand_bytes(N) when N > 0 ->
    try
        crypto:strong_rand_bytes(N)
    catch
        error:low_entropy ->
            list_to_binary([ rand:uniform(256) || _X <- lists:seq(1, N) ])
    end.
