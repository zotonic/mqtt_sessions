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

-module(mqtt_sessions_timestamp).

-export([
    timestamp/0
    ]).

% Constant value of calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}})
-define(SECS_1970, 62167219200).

%% @doc Calculate the current UNIX timestamp (seconds since Jan 1, 1970)
timestamp() ->
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()) - ?SECS_1970.
