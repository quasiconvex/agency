-module(manager).
-include("agency.hrl").

%% 1. Manage a pool of nodes, and a set of typed looms (subordinates)
%%    Ensure each subordinate is replicated on a configurable number of distinct nodes
%%    When a subordinate is created:
%%     allocate the least utilized nodes in the pool to it
%%    When the pool is modified, or the # nodes per subordinate changes:
%%     modify allocations for all affected subordinates
%%    Whenever allocations are changed, ensure they are enacted
%%    Every managed loom has zero or more external names, and an internal id
%%    The subordinate intrinsically knows its id, ensure it also knows its names

%% 2. Subordinates may also be managers themselves
%%    Manage other managers by controlling their pools
%%    A subordinate may request more resources for its pool:
%%      if possible, give it more resources
%%      otherwise, request more resources from our own boss, or complain loudly

%% 3. Track the utilization levels of all nodes in the pool
%%    This information is used when allocations are made
%%    Track both measured and desired utilization
%%    When nodes fill up, they may request us to relocate some of our subordinates

%% XXX High level known issues:
%%  - cycling rep factor (e.g. from 1 to 3 to 1) rapidly can confuse a sub
%%    this isn't really a supported use case but:
%%     possibly screws up its syncing (probably due to inaccurate peers)
%%     maybe caused by moving across non-overlapping set of nodes (hypothesis, not tested)
%%  - realloc semantics are wrong (see do_adjust_subs)
%%  - vacating subs is not completely safe (see do_remove_sub)

%% behavior
-export_type([sub_type/0,
              sub_name/0,
              sub_id/0,
              subish/0,
              which/0]).

-type sub_type() :: module().
-type sub_name() :: binary().
-type sub_id() :: binary().
-type subish() :: {name, sub_type(), sub_name()} |
                  {id, sub_type(), sub_id()}.
-type which() :: {#manager{}, subish()}.

-callback wants_relocate(node(), loom:state()) -> loom:state().
-callback wants_resources(list(), term(), loom:state()) -> loom:state().
-callback out_of_resources(term(), loom:state()) -> loom:state().

-optional_callbacks([wants_relocate/2,
                     wants_resources/3,
                     out_of_resources/2]).

%% client
-export([spec/1,
         spec/2,
         spec/4,
         id/1]).

-export([lookup/1,
         lookup/2,
         obtain/1,
         obtain/2,
         vacate/1,
         vacate/2,
         ids/1,
         ids/2,
         sites/1,
         sites/2]).

-export([alias/4,
         alias/5,
         unalias/4,
         unalias/5]).

%% admin
-export([lookup_pool/1,
         lookup_pool/2,
         change_pool/2,
         change_pool/3,
         inform_pool/2,
         inform_pool/3,
         report_pool/1,
         report_pool/2,
         get_replication_factor/2,
         get_replication_factor/3,
         set_replication_factor/3,
         set_replication_factor/4]).

%% admin / subordinate
-export([find_cache/4,
         find_cache/6,
         find_sub/3,
         request_relocate/2,
         request_relocate/3,
         request_resources/3,
         request_resources/4]).

%% tasks
-export([do_control_sites/2,
         do_assign_pool/2,
         do_adjust_subs/2,
         do_transfer_name/2,
         do_remove_sub/2]).

%% loom
-behavior(loom).
-export([vsn/1,
         find/2,
         home/1,
         opts/1,
         proc/1,
         keep/1,
         init/1,
         waken/1,
         verify_message/2,
         write_through/3,
         handle_idle/1,
         handle_info/2,
         handle_message/4,
         command_called/4,
         motion_decided/4,
         task_completed/4,
         task_continued/5,
         check_node/3,
         needs_upgrade/2]).

-export([callback/3,
         callback/4]).

%% manager impl helpers
-export([iter_subordinates/2,
         list_sub_names/1,
         list_sub_names/2]).

%% client

spec(Mod) ->
    spec(Mod, Mod).

spec(Mod, Name) ->
    spec(Mod, Name, undefined, undefined).

spec(Mod, Name, Boss, Id) ->
    #manager{mod=Mod, reg_name=Name, boss=Boss, id=Id}.

id(#{spec := Spec}) ->
    id(Spec);
id(#manager{id=Id}) ->
    Id.

lookup(Which) ->
    loom:ok(lookup(Which, #{})).

lookup({Manager, Subish}, Ctx) ->
    loom:patch(Manager, #{
                 type => command,
                 verb => lookup,
                 what => Subish
                }, Ctx).

obtain(Which) ->
    loom:ok(obtain(Which, #{})).

obtain({Manager, Subish}, Ctx) ->
    loom:patch(Manager, #{
                 type => command,
                 verb => obtain,
                 what => Subish
                }, Ctx).

vacate(Which) ->
    loom:ok(vacate(Which, #{})).

vacate({Manager, Subish}, Ctx) ->
    loom:patch(Manager, #{
                 type => command,
                 verb => vacate,
                 what => Subish
                }, Ctx).

ids(Which) ->
    loom:ok(ids(Which, #{})).

ids({Manager, {name, SubType, SubName}}, Ctx) ->
    loom:patch(Manager, #{
                 type => command,
                 kind => chain,
                 verb => lookup,
                 path => [sub, names, SubType, SubName]
                }, Ctx).

sites(Which) ->
    loom:ok(sites(Which, #{})).

sites({Manager, {id, SubType, SubId}}, Ctx) ->
    loom:patch(Manager, #{
                 type => command,
                 kind => chain,
                 verb => lookup,
                 path => [sub, sites, SubType, SubId]
                }, Ctx).

alias(Manager, SubType, SubName, SubId) ->
    loom:ok(alias(Manager, SubType, SubName, SubId, #{})).

alias(Manager, SubType, SubName, SubId, Ctx) when is_binary(SubName), is_binary(SubId) ->
    loom:patch(Manager, #{
                type => tether,
                verb => accrue,
                path => [sub, names, SubType, SubName],
                value =>
                    case util:get(Ctx, weak) of
                        true ->
                            {init, SubId};
                        _ ->
                            {push, SubId}
                    end
               }, Ctx).

unalias(Manager, SubType, SubName, SubId) ->
    loom:ok(unalias(Manager, SubType, SubName, SubId, #{})).

unalias(Manager, SubType, SubName, SubId, Ctx) when is_binary(SubName), is_binary(SubId) ->
    loom:patch(Manager, #{
                 type => tether,
                 verb => accrue,
                 path => [sub, names, SubType, SubName],
                 value => {drop, SubId}
                }, Ctx).

%% admin

lookup_pool(Manager) ->
    loom:ok(lookup_pool(Manager, #{})).

lookup_pool(Manager, Ctx) ->
    loom:patch(Manager, #{
                 type => command,
                 kind => chain,
                 verb => lookup,
                 path => [pool]
                }, Ctx).

change_pool(Manager, Change) ->
    loom:ok(change_pool(Manager, Change, #{})).

change_pool(Manager, Change, Ctx) ->
    loom:patch(Manager, #{
                 type => tether,
                 verb => accrue,
                 path => [pool],
                 value => Change
                }, Ctx).

inform_pool(Manager, Info) ->
    loom:ok(inform_pool(Manager, Info, #{})).

inform_pool(Manager, Info, Ctx) ->
    loom:patch(Manager, #{
                 type => measurement,
                 kind => pool,
                 value => Info
                }, Ctx).

report_pool(Manager) ->
    loom:ok(report_pool(Manager, #{})).

report_pool(Manager, Ctx) ->
    loom:patch(Manager, #{
                 type => command,
                 verb => lookup,
                 path => [pool_info]
                }, Ctx).

get_replication_factor(Manager, SubType) ->
    loom:ok(get_replication_factor(Manager, SubType, #{})).

get_replication_factor(Manager, SubType, Ctx) ->
    loom:patch(Manager, #{
                 type => command,
                 kind => chain,
                 verb => lookup,
                 path => [replication, SubType]
                }, Ctx).

set_replication_factor(Manager, SubType, K) ->
    loom:ok(set_replication_factor(Manager, SubType, K, #{})).

set_replication_factor(Manager, SubType, K, Ctx) ->
    loom:patch(Manager, #{
                 type => tether,
                 kind => chain,
                 verb => accrue,
                 path => [replication, SubType],
                 value => {'=', K}
                }, Ctx).

%% 'find_*' helps subordinates to implement their 'find' callbacks

find_cache(Which, Semantics, CachePath, Ctx) ->
    Check = fun (C, _) -> util:lookup(C, CachePath) end,
    Cache = fun (C, _, V) -> util:modify(C, CachePath, V) end,
    Purge = fun (C, _, _) -> util:remove(C, CachePath) end,
    find_cache(Which, Semantics, Check, Cache, Purge, Ctx).

find_cache(Which, Semantics, Check, Cache, Purge, Ctx) ->
    %% we're going to use a cached value, or look it up and cache the value
    %% if the delivery fails though, we want to purge the cache either way
    case Check(Ctx, Which) of
        undefined ->
            case find_sub(Which, Semantics, Ctx) of
                {ok, Found, Ctx1} ->
                    {ok, Found, Cache(Ctx1, Which, Found), find_purger(Which, Purge, Found)};
                {error, Reason, Ctx1} ->
                    {error, Reason, Ctx1}
            end;
        Cached ->
            {ok, Cached, Ctx, find_purger(Which, Purge, Cached)}
    end.

find_purger(Which, Purge, Value) ->
    %% create a find follow-up function to purge the cache if delivery failed
    fun (_, _, {error, delivery, Ctx}) ->
            {error, delivery, Purge(Ctx, Which, Value)};
        (_, _, Delivery) ->
            Delivery
    end.

find_sub(Which, get_or_create, Ctx) ->
    case obtain(Which, Ctx) of
        {ok, {_, Found}, Ctx1} ->
            {ok, Found, Ctx1};
        {error, Reason, Ctx1} ->
            {error, Reason, Ctx1}
    end;
find_sub(Which, get_if_exists, Ctx) ->
    case lookup(Which, Ctx) of
        {ok, {_, undefined}, Ctx1} ->
            {error, not_found, Ctx1};
        {ok, {_, Found}, Ctx1} ->
            {ok, Found, Ctx1};
        {error, Reason, Ctx1} ->
            {error, Reason, Ctx1}
    end.

%% 'request_relocate' requests that the manager move some subordinates off a node
%% the manager may accept the request and honor it later, or not at all

request_relocate(Manager, AwayFrom) ->
    loom:ok(request_relocate(Manager, AwayFrom, #{})).

request_relocate(Manager, AwayFrom, Ctx) ->
    loom:patch(Manager, #{
                 type => request,
                 kind => relocate,
                 value => AwayFrom
                }, Ctx).

%% 'request_resources' is for subordinate managers to request a bigger pool
%% in theory this same channel could be used to request that resources be revoked
%% the manager may accept the request and honor it later, or not at all
%% one should always just cope with the resources they have, the best they can
%% having more resources is a nice-to-have rather than a strict requirement

request_resources(Manager, Path, HaveWant) ->
    loom:ok(request_resources(Manager, Path, HaveWant, #{})).

request_resources(Manager, Path, HaveWant, Ctx) ->
    loom:patch(Manager, #{
                 type => request,
                 kind => resources,
                 path => Path,
                 value => HaveWant
                }, Ctx).

%% manage subordinate

subordinate_spec(#{spec := Manager}, Sub) ->
    subordinate_spec(Manager, Sub);
subordinate_spec(Manager, [SubType, SubId]) ->
    SubType:spec(Manager, SubId).

subordinate_is_manager(Manager, Sub) ->
    case subordinate_spec(Manager, Sub) of
        #manager{} ->
            true;
        _ ->
            false
    end.

make_subordinate_id(_SubType, SubName) when is_binary(SubName) ->
    util:bin([base64url:encode(SubName), "-", time:stamp(time:unow(), tai64)]).

which_callback({Manager = #manager{}, {name, SubType, _}}) ->
    SubType:spec(Manager, undefined);
which_callback({Manager = #manager{}, {id, SubType, SubId}}) ->
    SubType:spec(Manager, SubId).

%% loom

vsn(Spec) ->
    maps:merge(callback(Spec, {vsn, 1}, [Spec], #{}), #{}).

find(Which = {#manager{}, _}, Ctx) ->
    loom:callback(which_callback(Which), {find, 2}, [Which, Ctx]);
find(Spec, Ctx) ->
    callback(Spec, {find, 2}, [Spec, Ctx], {ok, {Spec, [node()]}, Ctx}).

proc(#manager{reg_name=Name} = Spec) ->
    callback(Spec, {proc, 1}, [Spec], fun () -> erloom_registry:proc(Name, Spec) end).

home(Spec) ->
    callback(Spec, {home, 1}, [Spec]).

opts(Spec) ->
    Defaults = #{
      alloc_batch_limit => 1000,
      default_replication => 1
     },
    maps:merge(Defaults, callback(Spec, {opts, 1}, [Spec], #{})).

keep(State) ->
    %% NB: sub has {names, sites, pools}
    Builtins = maps:with([pool,
                          replication,
                          utilization], State),
    maps:merge(Builtins, callback(State, {keep, 1}, [State], #{})).

init(State) ->
    State1 = State#{
               pool => util:get(State, pool, {[], undefined}),
               replication => util:get(State, replication, #{}),
               utilization => util:get(State, utilization, #{}),
               sub => jfdb:open(loom:path(sub, State))
              },
    callback(State1, {init, 1}, [State1], State1).

waken(State) ->
    callback(State, {waken, 1}, [State], State).

verify_message(Message, State) ->
    callback(State, {verify_message, 2}, [Message, State], {ok, Message, State}).

write_through(#{type := command, verb := lookup}, _, _State) ->
    {0, infinity};
write_through(#{type := command, verb := obtain}, _, _State) ->
    {0, infinity};
write_through(#{type := measurement}, _, _State) ->
    {0, infinity};
write_through(Message, N, State) ->
    callback(State, {write_through, 3}, [Message, N, State], {1, infinity}).

handle_idle(State) ->
    callback(State, {handle_idle, 1}, [State], fun () -> loom:sleep(State) end).

handle_info(Info, State) ->
    Untrap = fun () -> proc:untrap(Info, State) end,
    callback(State, {handle_info, 2}, [Info, State], Untrap).

handle_message(#{type := measurement, kind := pool, value := Info}, _, true, State) ->
    %% measurements should be sent to every node of the manager, they are not logged
    flood_detect(pool_measured(Info, State));

handle_message(#{type := request, kind := relocate, value := AwayFrom}, _, true, State) ->
    %% NB: these happen only when the message is received (on the node it was received)
    %%     its up to the subordinate to keep nagging as long as it still wants anything
    wants_relocate(AwayFrom, State);
handle_message(#{type := request, kind := resources, path := Path, value := HaveWant}, _, true, State) ->
    wants_resources(Path, HaveWant, State);

handle_message(Message, Node, IsNew, State) ->
    callback(State, {handle_message, 4}, [Message, Node, IsNew, State], State).

command_called(#{verb := accrue, path := [pool]}, Node, true, State) ->
    %% make sure all affected subordinates are updated to reflect the change
    %% we need to check both if their sites OR pools are affected by the change
    %% NB: if we inverted ids per node, we wouldn't have to check *all* subordinates
    %%     either way this is just a pool-sized factor in a time-space tradeoff
    Former = erloom_chain:value(State, [former], []),
    Latter = erloom_chain:value(State, [pool], []),
    Task = {fun ?MODULE:do_adjust_subs/2, {all, {pool, util:diff(Former, Latter)}}},
    loom:suture_task(adjust, Node, Task, cache_pool_info(State));

command_called(#{verb := accrue, path := [replication, SubType]}, Node, true, State) ->
    %% make sure all affected subordinates are updated to reflect the change
    %% we only need to check if the sites are affected, make sure we have the right number
    Task = {fun ?MODULE:do_adjust_subs/2, {{type, SubType}, replication}},
    loom:suture_task(adjust, Node, Task, State);

command_called(#{verb := lookup, what := {name, SubType, SubName}}, _, _, State)
  when is_binary(SubName) ->
    case erloom_chain:value(State, [sub, names, SubType, SubName]) of
        A when A =:= undefined; A =:= [] ->
            State#{response => {ok, {false, undefined}}};
        [SubId|_] ->
            respond_spec_sites(SubType, SubId, State)
    end;

command_called(#{verb := obtain, what := {name, SubType, SubName}}, _, _, State)
  when is_binary(SubName) ->
    case erloom_chain:value(State, [sub, names, SubType, SubName]) of
        A when A =:= undefined; A =:= [] ->
            %% information will be propagated through the chain motion itself
            %% we dont need yet another emit, unless we want to choose the value later
            SubId = make_subordinate_id(SubType, SubName),
            Message = #{verb => accrue, path => [sub, names, SubType, SubName], value => {init, SubId}},
            loom:wait(loom:make_tether(Message, State));
        [SubId|_] ->
            respond_spec_sites(SubType, SubId, State)
    end;

command_called(#{verb := vacate, what := {name, SubType, SubName}}, Node, DidChange, State)
  when is_binary(SubName) ->
    case erloom_chain:value(State, [sub, names, SubType, SubName]) of
        A when A =:= undefined; A =:= [] ->
            State#{response => {ok, {false, undefined}}};
        [SubId|_] ->
            %% treat the same as if called directly on the id, since remove task unaliases later
            command_called(#{verb => vacate, what => {id, SubType, SubId}}, Node, DidChange, State)
    end;

command_called(#{verb := accrue, path := [sub, names|Sub_]}, Node, true, State) ->
    %% either a new name has been created or an alias was created or destroyed
    %% subordinates must reflect the new ownership
    Transfer = {util:first(erloom_chain:value(State, [former], [])),
                util:first(erloom_chain:value(State, [sub, names|Sub_], []))},
    Task = {fun ?MODULE:do_transfer_name/2, {Sub_, Transfer}},
    loom:wait(loom:suture_task({name, Sub_}, Node, Task, State));

command_called(#{verb := lookup, what := {id, SubType, SubId}}, _, _, State)
  when is_binary(SubId) ->
    respond_spec_sites(SubType, SubId, State);

command_called(#{verb := obtain, what := {id, SubType, SubId}}, _, _, State)
  when is_binary(SubId) ->
    case erloom_chain:value(State, [sub, sites, SubType, SubId]) of
        undefined ->
            %% get permission to change first, then choose the value and emit
            Message = #{verb => create, path => [sub, sites, SubType, SubId], value => []},
            loom:wait(loom:make_tether(Message, State));
        Sites ->
            respond_spec_sites(SubType, SubId, Sites, State)
    end;

command_called(#{verb := vacate, what := {id, SubType, SubId}}, _, _, State)
  when is_binary(SubId) ->
    case erloom_chain:value(State, [sub, sites, SubType, SubId]) of
        undefined ->
            State#{response => {ok, {false, undefined}}};
        _Sites ->
            %% to vacate we must vote to remove both the sites & the pool for sub
            %% NB: we are lucky the batch returns {true, undefined}, which is correct here
            %%     otherwise we could add a handler for the batch message in motion_decided
            %%     that handler could wait for the remove task to finish, and return the same
            Message = #{
              kind => batch,
              value => [#{
                           type => command,
                           verb => remove,
                           path => [sub, sites, SubType, SubId]
                         },
                        #{
                           type => command,
                           verb => remove,
                           path => [sub, pools, SubType, SubId]
                         }]
             },
            loom:wait(loom:make_tether(Message, State))
    end;

command_called(#{verb := create, path := [sub, sites|Sub]}, Node, true, State) when Node =:= node() ->
    %% subordinate created: choose sites, emit a command to communicate the result
    %% we suture because we depend on many messages (votes), not just this one
    Nodes = choose_nodes([sub, sites|Sub], State),
    Boiler = #{type => command, kind => chain, verb => accrue, value => {'=', Nodes}},
    State1 = loom:suture_yarn(Boiler#{path => [sub, sites|Sub]}, State),
    State2 =
        case subordinate_is_manager(State1, Sub) of
            true ->
                %% if its a manager, also choose an initial pool
                %% NB: for now this is always the same as sites, but could be independent
                loom:suture_yarn(Boiler#{path => [sub, pools|Sub]}, State1);
            false ->
                State1
        end,
    loom:wait(State2); %% NB: sub_sites task will return

command_called(#{verb := accrue, path := [sub, sites|Sub]}, Node, true, State) ->
    %% time to actually change the sites for the subordinate, reflect changes in stats
    %% NB: does not return, which is fine as it is only used from create / batch updates
    Former = erloom_chain:value(State, [former], []),
    Latter = erloom_chain:value(State, [sub, sites|Sub], []),
    Task = {fun ?MODULE:do_control_sites/2, {Sub, {Former, Latter}}},
    State1 = pool_allocated([sub, sites|Sub], util:diff(Former, Latter), State),
    loom:wait(loom:stitch_task({sub, Sub}, Node, Task, State1));

command_called(#{verb := accrue, path := [sub, pools|Sub]}, Node, true, State) ->
    %% time to actually change the pool for the subordinate, reflect changes in stats
    %% NB: does not return, which is fine as it is only used from create / batch updates
    %%     the task can return as it is sure to run after the 'do_control_sites' on create
    Former = erloom_chain:value(State, [former], []),
    Latter = erloom_chain:value(State, [sub, pools|Sub], []),
    Task = {fun ?MODULE:do_assign_pool/2, {Sub, {Former, Latter}}},
    State1 = pool_allocated([sub, pools|Sub], util:diff(Former, Latter), State),
    loom:wait(loom:stitch_task({sub, Sub}, Node, Task, State1));

command_called(#{verb := remove, path := [sub, sites|Sub]}, Node, true, State) ->
    %% time to actually get rid of the sub, run the task and reflect changes in stats
    Former = erloom_chain:value(State, [former], []),
    Task = {fun ?MODULE:do_remove_sub/2, {Sub, Former}},
    State1 = pool_allocated([sub, sites|Sub], {[], Former}, State),
    loom:wait(loom:stitch_task({sub, Sub}, Node, Task, State1));

command_called(#{verb := remove, path := [sub, pools|Sub]}, _, true, State) ->
    %% just reflect changes in stats, everything else handled when the sites are removed
    Former = erloom_chain:value(State, [former], []),
    State1 = pool_allocated([sub, pools|Sub], {[], Former}, State),
    loom:wait(State1); %% NB: should've been called in vacate batch, task will return

command_called(Command, Node, DidChange, State) ->
    callback(State, {command_called, 4}, [Command, Node, DidChange, State], State).

motion_decided(Motion, Mover, Decision, State) ->
    callback(State, {motion_decided, 4}, [Motion, Mover, Decision, State], State).

task_completed(Message, Node, Result, State) ->
    callback(State, {task_completed, 4}, [Message, Node, Result, State], State).

task_continued(Name, Reason, Clock, Arg, State) ->
    callback(State, {task_continued, 5}, [Name, Reason, Clock, Arg, State], ok).

check_node(Node, Unanswered, State) ->
    callback(State, {check_node, 3}, [Node, Unanswered, State], State).

needs_upgrade(Vsn, State) ->
    callback(State, {needs_upgrade, 2}, [Vsn, State], State).

callback(#{spec := Spec}, FA, Args) ->
    callback(Spec, FA, Args);
callback(#manager{mod=Mod}, FA, Args) ->
    loom:callback(Mod, FA, Args).

callback(#{spec := Spec}, FA, Args, Default) ->
    callback(Spec, FA, Args, Default);
callback(#manager{mod=Mod}, FA, Args, Default) ->
    loom:callback(Mod, FA, Args, Default).

%% manager callbacks

wants_relocate(AwayFrom, State) ->
    %% called by the manager when asked to relocate off of a node
    %% if there are no better options, we may request more resources from our boss, if we have one
    Default = fun () ->
                      %% the default is to try to move some subordinates away from the node
                      Task = {fun ?MODULE:do_adjust_subs/2, {all, {relocate, AwayFrom}}},
                      loom:stitch_task(adjust, node(), Task, State)
              end,
    callback(State, {wants_relocate, 2}, Default).

wants_resources(Path = [sub, pools|_], HaveWant = {Have, Want}, State) ->
    %% called by the manager when a subordinate is running out of pooled resources
    %% (or in theory also when the subordinate has too many resources)
    %% we may in turn request more resources from our boss, if we have one
    Default = fun () ->
                      %% by default we try to honor the request
                      SubPool = erloom_chain:value(State, Path, []),
                      case length(SubPool) of
                          Have ->
                              %% we agree with how big the current size of the subpool is
                              case realloc_nodes(Path, SubPool, Want, State) of
                                  {[], _} when Want > Have ->
                                      %% they wanted more but we have no more to give
                                      maybe_request_resources({wants, Path, HaveWant}, State);
                                  Diff ->
                                      %% we should be able to honor this request, move to change the pool
                                      %% NB: there is some competition with the adjustment task
                                      %%     however worst case we might retry a batch (i think)
                                      Change = #{
                                        verb => accrue,
                                        path => Path,
                                        value => diff_op(Diff)
                                       },
                                      loom:make_tether(Change, State)
                              end;
                          _ ->
                              %% we disagree about the size of the subpool, ignore the request
                              State
                      end
              end,
    callback(State, {wants_resources, 3}, [Path, HaveWant, State], Default);
wants_resources(Path, HaveWant, State) ->
    callback(State, {wants_resources, 3}, [Path, HaveWant, State], State).

out_of_resources(Reason, State) ->
    callback(State, {out_of_resources, 2}, [Reason, State], State).

maybe_request_resources(Reason, State = #{spec := Spec}) ->
    case Spec of
        #manager{boss=undefined} ->
            %% no boss: just complain to the callback
            out_of_resources(Reason, State);
        #manager{boss=Boss, id=SubId, mod=SubType} ->
            %% we have a boss: ask to expand our pool (by 1 for now)
            Have = length(erloom_chain:value(State, [pool], [])),
            ok = request_resources(Boss, [sub, pools, SubType, SubId], {Have, Have + 1}),
            State
    end.

flood_detect(State) ->
    case pool_info(State) of
        PoolInfo when map_size(PoolInfo) =:= 0 ->
            %% an empty pool is never flooded (as opposed to always)
            State;
        PoolInfo ->
            %% check if the pool is flooding using a dumb heuristic
            HasOverflow =
                util:roll(fun ({_, {Total, _}}, _) ->
                              case Total < 0.8 of
                                  true ->
                                      {stop, false};
                                  false ->
                                      {continue, true}
                              end
                      end, false, PoolInfo),
            case HasOverflow of
                true ->
                    maybe_request_resources({pool, flooding}, State);
                false ->
                    State
            end
    end.

cache_pool_info(State = #{utilization := U}) ->
    %% when the pool changes, or utilization changes, we must update the pool_info
    State#{
      pool_info =>
          lists:foldl(fun (Node, Acc) ->
                              Acc#{Node => util:get(U, Node, {0, {0, 0}})}
                      end, #{}, erloom_chain:value(State, [pool]))
     }.

pool_info(#{pool_info := PoolInfo}) ->
    PoolInfo;
pool_info(_State) ->
    #{}.

pool_allocated([sub, sites|_], {Add, Rem}, State = #{utilization := U}) ->
    U1 = lists:foldl(u9n_accruer(sites, {'+', 1}), U, Add),
    U2 = lists:foldl(u9n_accruer(sites, {'-', 1}), U1, Rem),
    cache_pool_info(State#{utilization => U2});
pool_allocated([sub, pools|_], {Add, Rem}, State = #{utilization := U}) ->
    U1 = lists:foldl(u9n_accruer(pools, {'+', 1}), U, Add),
    U2 = lists:foldl(u9n_accruer(pools, {'-', 1}), U1, Rem),
    cache_pool_info(State#{utilization => U2}).

pool_measured(Info, State = #{utilization := U}) ->
    U1 = util:fold(u9n_modifier(total), U, Info),
    cache_pool_info(State#{utilization => U1}).

u9n_modifier(Field) ->
    fun ({Node, V}, U) -> accrue_u9n(U, Node, Field, {'=', V}) end.

u9n_accruer(Field, Op) ->
    fun (Node, U) -> accrue_u9n(U, Node, Field, Op) end.

accrue_u9n(Utilization, Node, Field, Op) ->
    util:modify(Utilization, [Node],
                fun (undefined) when Field =:= sites ->
                        {0, {util:op(0, Op), 0}};
                    (undefined) when Field =:= pools ->
                        {0, {0, util:op(0, Op)}};
                    (undefined) when Field =:= total ->
                        {util:op(0, Op), {0, 0}};
                    ({T, {S, P}}) when Field =:= sites ->
                        {T, {util:op(S, Op), P}};
                    ({T, {S, P}}) when Field =:= pools ->
                        {T, {S, util:op(P, Op)}};
                    ({T, {S, P}}) when Field =:= total ->
                        {util:op(T, Op), {S, P}}
                end).

%% common helpers

respond_spec_sites(SubType, SubId, State) ->
    Sites = erloom_chain:value(State, [sub, sites, SubType, SubId]),
    respond_spec_sites(SubType, SubId, Sites, State).

respond_spec_sites(_, _, undefined, State) ->
    State#{response => {ok, {false, undefined}}};
respond_spec_sites(SubType, SubId, Sites, State) ->
    SubSpec = subordinate_spec(State, [SubType, SubId]),
    State#{response => {ok, {false, {SubSpec, Sites}}}}.

%% manage resources

diff_op({Add, []}) ->
    {'+', Add};
diff_op({[], Rem}) ->
    {'-', Rem};
diff_op({Add, Rem}) ->
    [{'+', Add}, {'-', Rem}].

iter_subordinates(all, State) ->
    %% XXX: would be nice to push the fold to db
    %%      if only NIFs supported calling erlang functions
    util:fold(
      fun ({SubType, BySubId}, Acc) ->
              util:fold(
                fun ({SubId, _}, A) ->
                        [{util:atom(SubType), SubId}|A]
                end, Acc, BySubId)
      end, [], util:lookup(State, [sub, sites], []));
iter_subordinates({type, SubType}, State) ->
    %% XXX: see above
    util:fold(
      fun ({SubId, _}, Acc) ->
              [{SubType, SubId}|Acc]
      end, [], util:lookup(State, [sub, sites, SubType], [])).

list_sub_names(State) ->
    list_sub_names(fun (_) -> true end, State).

list_sub_names(FilterType, State) ->
    %% XXX: see above
    util:fold(
      fun ({SubTypeBin, BySubName}, Acc) ->
              SubType = util:atom(SubTypeBin),
              case FilterType(SubType) of
                  true ->
                      util:fold(
                        fun ({SubName, {[_|_], _}}, A) ->
                                [{SubType, SubName}|A];
                            ({SubName, {[_|_], _, _}}, A) ->
                                [{SubType, SubName}|A];
                            ({_, _}, A) ->
                                A
                        end, Acc, BySubName);
                  false ->
                      Acc
              end
      end, [], util:lookup(State, [sub, names], [])).

ranked_nodes(PoolInfo) ->
    util:vals(lists:keysort(1, util:index(PoolInfo))).

choose_nodes(Path, State) ->
    choose_nodes(Path, erloom_chain:value(State, Path), State).

choose_nodes(Path, Existing, State) ->
    util:edit(Existing, realloc_nodes(Path, Existing, State)).

realloc_nodes(Path = [sub, sites, SubType, _], Existing, State) ->
    %% the number of sites we want is always determined by the replication factor
    K = erloom_chain:value(State, [replication, SubType]),
    realloc_nodes(Path, Existing, K, State);
realloc_nodes(Path = [sub, pools|_], Existing, State) ->
    %% when we reallocate a sub_pool, we aim to keep it the same size
    realloc_nodes(Path, Existing, length(Existing), State).

realloc_nodes(Path, Existing, undefined, State) ->
    %% if K is undefined, fallback to the default replication factor
    realloc_nodes(Path, Existing, util:lookup(State, [opts, default_replication]), State);
realloc_nodes(Path, Existing, K, State) ->
    %% get the pool info, then determine any nodes which are gone thanks to a pool change
    PoolInfo = pool_info(State),
    Gone = util:except(Existing, PoolInfo),
    realloc_nodes(Path, Existing, length(Existing) - length(Gone), K, PoolInfo, Gone).

realloc_nodes(_, Existing, R, K, PoolInfo, Gone) when R < K ->
    %% choose nodes to add: less utilized is better
    %% NB: we may return less than desired
    Prefs = ranked_nodes(util:except(PoolInfo, Existing)),
    {lists:sublist(Prefs, K - R), Gone};
realloc_nodes(_, Existing, R, K, PoolInfo, Gone) when R > K ->
    %% choose nodes to drop: more utilized is better
    Prefs = lists:reverse(ranked_nodes(util:select(PoolInfo, Existing))),
    {[], Gone ++ lists:sublist(Prefs, R - K)};
realloc_nodes(_, _, _, _, _, Gone) ->
    %% number remaining == number desired, only drop whats gone (generally empty)
    {[], Gone}.

maybe_realloc({relocate, AwayFrom}, Path, Acc) ->
    %% a relocate potentially touches both subordinate sites and pools
    potential_realloc(Path, AwayFrom, Acc);
maybe_realloc(_, Path = [sub, sites|_], Acc) ->
    %% any trigger can cause a potential realloc of sites
    potential_realloc(Path, Acc);
maybe_realloc({pool, {_, Rem}}, Path = [sub, pools|_], Acc) when Rem =/= [] ->
    %% only a pool shrinkage triggers potential realloc of subordinate pools
    potential_realloc(Path, Acc);
maybe_realloc(_Trigger, _Path, Acc) ->
    Acc.

potential_realloc(Path, {_, State} = Acc) ->
    Existing = erloom_chain:value(State, Path, []),
    Diff = realloc_nodes(Path, Existing, State),
    accumulate_allocation(Path, Diff, Acc).

potential_realloc(Path, Without, {_, State} = Acc) ->
    %% potential realloc for a relocate is speculative and thus more complicated
    Existing = erloom_chain:value(Path, State, []),
    case util:has(Existing, Without) of
        true ->
            %% flip a coin (we only want to move some of the subordinates)
            case util:random(2) of
                1 ->
                    %% heads: pretend we didn't already have the node and realloc
                    K = length(Existing),
                    Existing1 = util:except(Existing, [Without]),
                    case realloc_nodes(Path, Existing1, K, State) of
                        {[Without], []} ->
                            %% we produced the same result: do nothing
                            %% NB: we elsewhere realize if we are out of resources
                            Acc;
                        Diff ->
                            %% we produced a different result, take it
                            accumulate_allocation(Path, Diff, Acc)
                    end;
                2 ->
                    %% tails: skip
                    Acc
            end;
        false ->
            %% not using the node: nothing to do
            Acc
    end.

accumulate_allocation(_, {[], []}, Acc) ->
    Acc;
accumulate_allocation(Path, Diff, {Allocs, State}) ->
    %% add the allocation if its a change, update state to reflect what we are allocating
    %% NB: changes in state won't be persisted, just used for temporary bookkeeping
    %%     however the changes in allocations will be mirrored when they are applied
    {[{Path, diff_op(Diff)}|Allocs], pool_allocated(Path, Diff, State)}.

attempt_pushing_allocations(Allocs, {Retry, State}) ->
    %% attempt to push a whole bunch of allocations at once to avoid overhead
    %% NB: this has other ramifications for efficiency (e.g. if one of the paths is locked)
    %%     however the loom can theoretically optimize some of these cases (TODO)
    Batch =
        util:fold(fun ({Path, Op}, Acc) ->
                          [#{verb => accrue, path => Path, value => Op}|Acc]
                  end, [], Allocs),
    case loom:call(State, #{type => tether, kind => batch, value => Batch}) of
        {ok, _} ->
            %% the batch succeeded, nothing remaining to do
            {Retry, State};
        _ ->
            %% the batch failed (or we didn't hear back), we'll have to try again
            {Allocs ++ Retry, State}
    end.

do_adjust_subs({Which, Trigger}, State) ->
    %% make sure that each subordinate has sensible allocations when params change
    %% this task should be globally queued, so that only one works at a time
    BMax = util:lookup(State, [opts, alloc_batch_limit]),
    Iter = iter_subordinates(Which, State),
    do_adjust_subs(initial, Which, Trigger, Iter, BMax, State);
do_adjust_subs({Which, Trigger, Iter, BMax}, State) ->
    do_adjust_subs(retry, Which, Trigger, Iter, BMax, State).

do_adjust_subs(Phase, Which, Trigger, Iter, BMax, State) ->
    %% XXX: the realloc operation MUST have the following semantics (but does not currently):
    %%       if the value at path is undefined - take no effect, that is, leave it undefined
    %%       that is, realloc is predicated on existence of sub, if sub does not exist: do nothing
    %%      this is how we avoid race conditions with removal of subs (i.e. delete always takes precedence)
    %%      right now we would allocate new nodes for a deleted sub if we started realloc beforehand
    %%       it's not dangerous but it wastes some resources and violates the meaning of removal
    {Retry, _} =
        util:chunk(
          fun (Chunk, {R, S}) when Phase =:= initial ->
                  %% initial attempt, actually figure out what we wan't to allocate
                  {Allocs, S1} =
                      util:fold(
                        fun ({SubType, SubId}, A) ->
                                A1 = maybe_realloc(Trigger, [sub, sites, SubType, SubId], A),
                                __ = maybe_realloc(Trigger, [sub, pools, SubType, SubId], A1)
                        end, {[], S}, Chunk),
                  attempt_pushing_allocations(Allocs, {R, S1});
              (Allocs, {R, S}) ->
                  %% retried attempt, just push the allocations that failed before
                  attempt_pushing_allocations(Allocs, {R, S})
          end, {[], State}, Iter, BMax),
    case Retry of
        [_|_] ->
            {retry, {Which, Trigger, Iter, BMax}, {10, seconds}, unfinished};
        [] ->
            {done, ok}
    end.

%% actually start / stop sites
%% this is a generic task for controlling another loom given a spec

do_control_sites({Sub, {[], [_|_] = New}}, State) ->
    %% we can stop as soon as the loom acknowledges the message
    %% the start message will keep trying to seed until it succeeds
    %% since the root conf is symbolic, we can safely try seeding different nodes when we don't get a reply
    SubSpec = subordinate_spec(State, Sub),
    case loom:deliver(New, SubSpec, #{type => start, seed => New}, #{}) of
        {error, delivery, #{dstat := #{spec := SubSpec}}} ->
            {retry, {10, seconds}, {undeliverable, {seed, SubSpec}}};
        {ok, already_exists, _} ->
            {done, {ok, {false, {SubSpec, New}}}};
        {ok, ok, _} ->
            {done, {ok, {true, {SubSpec, New}}}}
    end;
do_control_sites({Sub, {[_|_] = Old, New}}, State) ->
    %% we can stop as soon as the loom accepts responsibility for the change
    %% once the motion passes it will take effect, eventually
    %% we rely on the old nodes to make the transition
    SubSpec = subordinate_spec(State, Sub),
    case loom:deliver(Old, SubSpec, #{type => command, verb => lookup, path => [elect]}, #{}) of
        {error, delivery, _} ->
            {retry, {10, seconds}, {undeliverable, {start, SubSpec}}};
        {ok, {ok, {_, Elect = #{current := Current}}}, #{dstat := #{node := Probed}}} ->
            %% check if the current conf is what we want already
            case util:get(Elect, Current) of
                {_, #{value := {'=', New}}, decided} ->
                    %% decided means accepted if its still there
                    {done, {ok, {false, {SubSpec, New}}}};
                {_, #{value := {'=', New}}, _} ->
                    %% wait to see if it passes
                    {retry, {10, seconds}, {wait, {Probed, control, pending}}};
                {_, #{}, _} ->
                    %% has another value, we ought to set it
                    case loom:deliver(Old, SubSpec, #{type => move, kind => conf, value => {'=', New}}, #{}) of
                        {ok, {ok, _}, _} ->
                            {done, {ok, {true, {SubSpec, New}}}};
                        {ok, {retry, stopped}, _} ->
                            {done, {ok, {true, {SubSpec, New}}}};
                        {ok, {retry, waiting}, _} ->
                            {done, {ok, {true, {SubSpec, New}}}};
                        {_, Response, #{dstat := DStat}} ->
                            %% either the motion is pending or it failed, either way try again
                            {retry, {10, seconds}, {wait, {Response, control, DStat}}}
                    end
            end;
        {ok, {retry, stopped}, _} ->
            %% if an old node is stopped, the group config must have already changed
            %% either our change was successful, or someone else interfered
            %% in either case we are done, if someone else is interfering, its their problem
            {done, {ok, {true, {SubSpec, New}}}};
        {ok, {retry, waiting}, _} ->
            %% its possible the node was stopped a long time ago and wiped itself
            %% its also possible the node was never started, but not unless someone interfered
            %% treat the same as a stop, if someone interfered, not our problem
            {done, {ok, {true, {SubSpec, New}}}}
    end;
do_control_sites({Sub, _}, State) ->
    %% there are no old nodes and no new ones: weird, but someone must have asked us to do it
    %% controlling them is easy: nothing to do
    SubSpec = subordinate_spec(State, Sub),
    {done, {ok, {false, {SubSpec, []}}}}.

%% pool management (of subordinates)
%% NB: we only transfer the diff, so nobody else better mess with the subordinate's pool

do_assign_pool({_, {Same, Same}}, _State) ->
    {done, same};
do_assign_pool({Sub, {Old, New}}, State) ->
    %% probe the path first to prevent duplicating requests
    %% then wait, modify, or retry as needed
    SubSpec = subordinate_spec(State, Sub),
    Sites = erloom_chain:value(State, [sub, sites|Sub], []),
    Probe = #{
      type => command,
      kind => probe,
      verb => lookup,
      path => [pool]
     },
    case loom:deliver(Sites, SubSpec, Probe, #{}) of
        {error, Reason, _} ->
            %% subordinate could not be contacted
            {retry, {10, seconds}, {unreachable, {probe, SubSpec}, Reason}};
        {ok, {wait, Reason}, #{dstat := #{node := Node}}} ->
            %% a change is pending
            {retry, {10, seconds}, {wait, {Node, {assign, deliver}, Reason}}};
        {ok, {ok, {_, New}}, _} ->
            %% the value has already been set, all done
            {done, ok};
        {ok, {ok, _}, #{dstat := #{node := Node}}} ->
            %% the value is something else, try sending
            Accrue = #{
              type => tether,
              verb => accrue,
              path => [pool],
              value => {edit, util:diff(Old, New)}
             },
            case loom:rpc(Node, SubSpec, Accrue) of
                {ok, {_, New}} ->
                    {done, ok};
                Response ->
                    {retry, {10, seconds}, {wait, {Node, {assign, rpc}, Response}}}
            end
    end.

%% name management
%% ensure that names are transferred between the subordinates to reflect alias changes

do_transfer_name({_, {Same, Same}}, _) ->
    %% do nothing if there is no change
    {done, {false, Same}};
do_transfer_name({[SubType, SubName], {undefined, New}}, State) ->
    %% just give control to the new subordinate, since there is no old one
    %% use obtain because this is how a sub gets initially 'created'
    %% NB: internal only - normally, bad things can happen if obtaining by id
    do_micromanage(State, {id, SubType, New}, obtain, [names, SubName], true);
do_transfer_name({[SubType, SubName], {Old, undefined}}, State) ->
    %% just revoke the control from the old subordinate, since there is no new one
    %% use lookup for the old sub, because it may have been removed if it doesn't exist
    do_micromanage(State, {id, SubType, Old}, lookup, [names, SubName], false);
do_transfer_name({[SubType, SubName], {Old, New}}, State) ->
    %% transfer control of the name to the new subordinate from the old one, if any
    %% the new subordinate relieves the old one of duty, and copies anything it needs
    %% during the transition period they might both think they own the name, which is fine
    %% the agency only points to one of them (who knows how long it takes for them to find out)
    %% NB: return the new {SubSpec, Sites}, not the old
    case do_micromanage(State, {id, SubType, New}, obtain, [names, SubName], true) of
        {done, Return} ->
            do_micromanage(State, {id, SubType, Old}, lookup, [names, SubName], false, Return);
        Other ->
            Other
    end.

do_micromanage(Manager, Subish, Mechanism, Path, Value) ->
    do_micromanage(Manager, Subish, Mechanism, Path, Value, undefined).

do_micromanage(#{spec := Spec}, {id, SubType, SubId}, obtain, Path, Value, Return) ->
    case obtain({Spec, {id, SubType, SubId}}, #{}) of
        {ok, {_, {SubSpec, Sites}}, _} ->
            do_micromanage(Spec, SubSpec, Sites, Path, Value, Return);
        {error, Reason, _} ->
            %% we couldn't talk to the manager, should only happen under heavy load
            {retry, {10, seconds}, {unreachable, {obtain, Spec}, Reason}}
    end;
do_micromanage(#{spec := Spec}, {id, SubType, SubId}, lookup, Path, Value, Return) ->
    case lookup({Spec, {id, SubType, SubId}}, #{}) of
        {ok, {false, undefined}, _} ->
            {done, Return};
        {ok, {_, {SubSpec, SubSites}}, _} ->
            do_micromanage(Spec, SubSpec, SubSites, Path, Value, Return);
        {error, Reason, _} ->
            %% we couldn't talk to the manager, should only happen under heavy load
            {retry, {10, seconds}, {unreachable, {lookup, Spec}, Reason}}
    end;

do_micromanage(_, SubSpec, Sites, Path, Value, Return) ->
    %% probe the path first to prevent duplicating requests
    %% then wait, modify, or retry as needed
    %% NB: always returns the {SubSpec, Sites} as needed by create
    Probe = #{
      type => command,
      kind => probe,
      verb => lookup,
      path => Path
     },
    case loom:deliver(Sites, SubSpec, Probe, #{}) of
        {error, Reason, _} ->
            %% subordinate could not be contacted
            {retry, {10, seconds}, {unreachable, {probe, SubSpec}, Reason}};
        {ok, {wait, Reason}, #{dstat := #{node := Node}}} ->
            %% a change is pending
            {retry, {10, seconds}, {wait, {Node, {micro, deliver}, Reason}}};
        {ok, {ok, {_, Value}}, _} ->
            %% the value has already been set, all done
            {done, util:def(Return, {ok, {true, {SubSpec, Sites}}})};
        {ok, {ok, _}, #{dstat := #{node := Node}}} ->
            %% the value is something else, try sending
            Modify = #{
              type => tether,
              verb => modify,
              path => Path,
              value => Value
             },
            case loom:rpc(Node, SubSpec, Modify) of
                {ok, {_, Value}} ->
                    {done, util:def(Return, {ok, {true, {SubSpec, Sites}}})};
                Response ->
                    {retry, {10, seconds}, {wait, {Node, {micro, rpc}, Response}}}
            end
    end.

do_remove_sub({Sub = [SubType, SubId], Sites}, State) ->
    %% Removal is fairly complicated, especially due to potential race conditions
    %% To do it correctly we must (in order):
    %%  1. Make the sub catatonic: it should no longer accept new names (aliasing)
    %%      XXX: we don't currently do this, but to be safe we MUST
    %%  2. Drop aliases: remove all names which are associated with the sub, even old ones
    %%  3. Free: stop the sub, it should in turn free all its resources when stopped
    %%      XXX: currently managers don't free their resources when stopped, they should
    SubSpec = subordinate_spec(State, Sub),
    case loom:deliver(Sites, SubSpec, #{type => command, verb => lookup, path => [names]}, #{}) of
        {error, Reason, _} ->
            %% subordinate could not be contacted
            {retry, {10, seconds}, {unreachable, {names, Sub}, Reason}};
        {ok, {retry, stopped}, _} ->
            %% subordinate stopped already, strange but possible
            {done, ok};
        {ok, {retry, waiting}, _} ->
            %% subordinate never started (or more likely stopped & wiped)
            %% treat the same as stopped, given what we know
            {done, ok};
        {ok, {ok, {_, []}}, _} ->
            %% exit early if we never had any names
            do_control_sites({Sub, {Sites, []}}, State);
        {ok, {ok, {_, Names}}, _} ->
            %% get rid of all names, since even old ones may reference us in their stack
            %% try to unalias them all at once
            Batch = #{
              type => tether,
              kind => batch,
              value =>
                  [#{
                      verb => accrue,
                      path => [sub, names, SubType, Name],
                      value => {except, [SubId]}
                    } || {Name, _} <- Names]
             },
            case loom:call(State, Batch) of
                {ok, _} ->
                    %% stop the sub
                    do_control_sites({Sub, {Sites, []}}, State);
                _ ->
                    %% couldn't kill the aliases, retry
                    {retry, {10, seconds}, inalienable}
            end
    end.
