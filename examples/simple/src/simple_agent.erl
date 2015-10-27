-module(simple_agent).
-include_lib("agency/include/agency.hrl").

-behavior(agent).
-export([spec/2,
         home/1,
         handle_message/4]).

spec(Agency, Account) ->
    #agent{mod=?MODULE, agency=Agency, account=Account}.

home(#agent{account=Account}) ->
    filename:join([var, url:esc(node()), ?MODULE, Account]).

handle_message(_Message, _Node, _IsNew, State) ->
    State.
