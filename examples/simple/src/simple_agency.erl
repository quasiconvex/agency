-module(simple_agency).
-include_lib("agency/include/agency.hrl").

-export([a/0, b/0, b2/0, c/0, d/0, e/0, f/0, f/3]).
-export([nd/1, fwd/2, ag/1, ags/1, nms/1, rep/1]).
-export([spec/0, call/1, state/0, tasks/0]).

-behavior(manager).
-export([home/1,
         task_completed/4,
         task_continued/5,
         out_of_resources/2]).

a() ->
    loom:seed(spec()).

b() ->
    {ok, _} = rep(3),
    {ok, _} = manager:change_pool(spec(), {'+', [nd("rst"), nd("uvw"), nd("xyz")]}),
    {ok, _} = manager:obtain(spec(), simple_agent, <<"jared">>),
    {ok, _} = manager:obtain(spec(), simple_agent, <<"jeff">>),
    {ok, _} = manager:obtain(spec(), simple_agent, <<"thomas">>),
    {ok, _} = rep(2).

b2() ->
    {ok, _} = manager:obtain(simple_agency:spec(), simple_agent, <<"jared">>),
    {ok, _} = rep(3).

c() ->
    {ok, _} = manager:alias(spec(), simple_agent, <<"jared">>, <<"account">>),
    {ok, _} = manager:alias(spec(), simple_agent, <<"jeff">>, <<"account">>).

d() ->
    {ok, _} = manager:unalias(spec(), simple_agent, <<"jared">>, <<"account">>),
    {ok, _} = manager:unalias(spec(), simple_agent, <<"jeff">>, <<"account">>).

e() ->
    a(), b(), c(), d().

f() ->
    f(1, 1, 1).

f(A, B, C) ->
    manager:inform_pool(spec(), #{nd("rst") => A, nd("uvw") => B, nd("xyz") => C}).

nd(Name) ->
    util:atom(Name ++ "@" ++ util:ok(inet:gethostname())).

fwd(Name, Message) ->
    manager:relay(spec(), {name, simple_agent, Name}, Message).

ag(AgentId) ->
    simple_agent:spec(spec(), AgentId).

ags(AgentId) ->
    loom:patch(ag(AgentId), get_state).

nms(AgentId) ->
    util:get(ags(AgentId), names).

rep(N) ->
    manager:set_replication_factor(spec(), simple_agent, N).

spec() ->
    manager:spec(?MODULE).

call(Message) ->
    loom:call(spec(), Message).

state() ->
    loom:call(spec(), get_state).

tasks() ->
    util:get(state(), tasks).

%% manager

home(#manager{mod=?MODULE}) ->
    filename:join([var, url:esc(node()), ?MODULE]).

task_completed(_Message, _Node, _Result, State) ->
    State.

task_continued(Name, Reason, {N, T}, _Arg, _State) ->
    Secs = time:timer_elapsed(T) / 1000,
    io:format("[~B @ ~.3fs] ~256p: ~256p~n", [N, Secs, Name, Reason]).

out_of_resources(Reason, State) ->
    io:format("[~256p] out of resources: ~256p~n", [util:get(State, spec), Reason]),
    State.
