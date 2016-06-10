-module(simple_agent).
-include_lib("agency/include/agency.hrl").

%% convenience
-export([connect/1,
         connect/2,
         patch/2,
         patch/3,
         spec/1]).

-behavior(agent).
-export([spec/2,
         home/1,
         handle_message/4]).

connect(Name) ->
    connect(Name, #{}).

connect(Name, Ctx) ->
    agent:connect({simple_agency:spec(), {name, ?MODULE, Name}}, Name, Ctx).

patch(Name, Message) ->
    loom:ok(patch(Name, Message, #{})).

patch(Name, Message, Ctx) ->
    loom:patch({simple_agency:spec(), {name, ?MODULE, Name}}, Message, Ctx).

spec(AgentId) ->
    spec(simple_agency:spec(), AgentId).

spec(Agency, AgentId) ->
    #agent{mod=?MODULE, agency=Agency, id=AgentId}.

home(#agent{id=AgentId}) ->
    filename:join([var, url:esc(node()), ?MODULE, AgentId]).

handle_message(_Message, _Node, _IsNew, State) ->
    State.
