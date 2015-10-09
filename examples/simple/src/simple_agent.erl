-module(simple_agent).
-include_lib("agency/include/agency.hrl").

-behavior(agent).
-export([agency/1,
         home/1,
         handle_message/4]).

agency(_) ->
    agency:spec(simple_agency, ?MODULE).

home(#agent{mod=?MODULE, account=Account}) ->
    filename:join([var, url:esc(node()), ?MODULE, Account]).

handle_message(_Message, _Node, _IsNew, State) ->
    State.
