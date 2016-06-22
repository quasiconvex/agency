-module(simple_agency).
-include_lib("agency/include/agency.hrl").

-export([a/0, b/0, b2/0, c/0, d/0, e/0, f/0, f/3, v/0]).
-export([nd/1, fwd/2, ag/1, ags/1, nms/1, rep/1]).
-export([spec/0, patch/1, state/0, subs/0, tasks/0]).

-behavior(manager).
-export([find/2,
         home/1,
         task_completed/4,
         task_continued/5,
         out_of_resources/2]).

a() ->
    loom:seed(spec()).

b() ->
    rep(3),
    manager:change_pool(spec(), {'+', [nd("rst"), nd("uvw"), nd("xyz")]}),
    manager:obtain({spec(), {name, simple_agent, <<"jared">>}}),
    manager:obtain({spec(), {name, simple_agent, <<"jeff">>}}),
    manager:obtain({spec(), {name, simple_agent, <<"thomas">>}}),
    rep(2).

b2() ->
    manager:obtain({spec(), {name, simple_agent, <<"jared">>}}),
    manager:change_pool(spec(), {'+', [nd("rst")]}),
    rep(1).

c() ->
    manager:alias(spec(), simple_agent, <<"jared">>, <<"account">>),
    manager:alias(spec(), simple_agent, <<"jeff">>, <<"account">>).

d() ->
    manager:unalias(spec(), simple_agent, <<"jared">>, <<"account">>),
    manager:unalias(spec(), simple_agent, <<"jeff">>, <<"account">>).

e() ->
    a(), b(), c(), d().

f() ->
    f(1, 1, 1).

f(A, B, C) ->
    manager:inform_pool(spec(), #{nd("rst") => A, nd("uvw") => B, nd("xyz") => C}).

v() ->
    manager:vacate({spec(), {name, simple_agent, <<"thomas">>}}).

nd(Name) ->
    util:atom(Name ++ "@" ++ util:ok(inet:gethostname())).

fwd(Name, Message) ->
    loom:patch({spec(), {name, simple_agent, Name}}, Message).

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

patch(Message) ->
    loom:patch(spec(), Message).

state() ->
    patch(get_state).

subs() ->
    State = state(),
    SubNames = util:lookup(State, [sub, names]),
    SubSites = util:lookup(State, [sub, sites]),
    util:fold(
      fun ({SubType, BySubName}, Acc) ->
              util:fold(
                fun({SubName, Ids}, A) ->
                        case util:first(element(1, Ids)) of
                            undefined ->
                                A;
                            SubId ->
                                A#{SubName => {SubId, erloom_chain:value(SubSites, [SubType, SubId])}}
                        end
                end, Acc, BySubName)
      end, #{}, SubNames).

tasks() ->
    util:get(state(), tasks).

%% manager

find(#manager{} = Spec, Ctx) ->
    {ok, {Spec, [nd("rst")]}, Ctx}.

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
