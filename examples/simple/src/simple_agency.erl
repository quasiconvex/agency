-module(simple_agency).
-include_lib("agency/include/agency.hrl").

-export([a/0, b/0, c/0, d/0, e/0]).
-export([nd/1, ags/1, ids/1, spa/1]).
-export([spec/0, call/1, state/0, tasks/0]).

-behavior(agency).
-export([home/1,
         task_completed/4,
         task_continued/5]).

a() ->
    loom:seed(spec()).

b() ->
    {ok, _} = agency:set_sites_per_account(spec(), 3),
    {ok, _} = agency:change_sites(spec(), {'+', [nd("rst"), nd("uvw"), nd("xyz")]}),
    {ok, _} = agency:resolve(spec(), <<"jared">>),
    {ok, _} = agency:resolve(spec(), <<"jeff">>),
    {ok, _} = agency:resolve(spec(), <<"thomas">>),
    {ok, _} = agency:set_sites_per_account(spec(), 2).

c() ->
    {ok, _} = agency:link_identity(spec(), <<"jared">>, <<"account">>),
    {ok, _} = agency:link_identity(spec(), <<"jeff">>, <<"account">>).

d() ->
    {ok, _} = agency:unlink_identity(spec(), <<"jared">>, <<"account">>),
    {ok, _} = agency:unlink_identity(spec(), <<"jeff">>, <<"account">>).

e() ->
    a(), b(), c(), d().

nd(Name) ->
    util:atom(Name ++ "@" ++ util:ok(inet:gethostname())).

ags(Account) ->
    agency:patch(agent:spec(simple_agent, Account), get_state).

ids(Account) ->
    util:get(ags(Account), identities).

spa(N) ->
    agency:set_sites_per_account(spec(), N).

spec() ->
    agency:spec(?MODULE, simple_agent).

call(Message) ->
    loom:call(spec(), Message).

state() ->
    loom:call(spec(), get_state).

tasks() ->
    util:get(state(), tasks).

%% agency

home(#agency{mod=?MODULE}) ->
    filename:join([var, url:esc(node()), ?MODULE]).

task_completed(_Message, _Node, _Result, State) ->
    State.

task_continued(Name, Reason, {N, T}, _Arg, _State) ->
    Secs = time:timer_elapsed(T) / 1000,
    io:format("[~B @ ~.3fs] ~256p: ~256p~n", [N, Secs, Name, Reason]).
