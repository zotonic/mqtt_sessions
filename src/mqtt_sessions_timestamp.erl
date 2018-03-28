-module(mqtt_sessions_timestamp).

-export([
    timestamp/0
    ]).

% Constant value of calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}})
-define(SECS_1970, 62167219200).

%% @doc Calculate the current UNIX timestamp (seconds since Jan 1, 1970)
timestamp() ->
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()) - ?SECS_1970.
