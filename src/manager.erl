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
         spec/4]).

-export([lookup/1,
         lookup/2,
         obtain/1,
         obtain/2,
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

%% generic
-export([patch/2,
         patch/3,
         relay/3,
         relay/4]).

%% tasks
-export([do_control_sites/2,
         do_assign_pool/2,
         do_adjust_subs/2,
         do_transfer_name/2]).

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

%% client

spec(Mod) ->
    spec(Mod, Mod).

spec(Mod, Name) ->
    spec(Mod, Name, undefined, undefined).

spec(Mod, Name, Boss, Id) ->
    #manager{mod=Mod, reg_name=Name, boss=Boss, id=Id}.

lookup(Which) ->
    lookup(Which, #{}).

lookup({Manager, Subish}, Ctx) ->
    patch(Manager, #{
            type => command,
            verb => lookup,
            what => Subish
           }, Ctx).

obtain(Which) ->
    obtain(Which, #{}).

obtain({Manager, Subish}, Ctx) ->
    patch(Manager, #{
            type => command,
            verb => obtain,
            what => Subish
           }, Ctx).

ids(Which) ->
    ids(Which, #{}).

ids({Manager, {name, SubType, SubName}}, Ctx) ->
    patch(Manager, #{
            type => command,
            kind => chain,
            verb => lookup,
            path => [sub_names, SubType, SubName]
           }, Ctx).

sites(Which) ->
    sites(Which, #{}).

sites({Manager, {id, SubType, SubId}}, Ctx) ->
    patch(Manager, #{
            type => command,
            kind => chain,
            verb => lookup,
            path => [sub_sites, SubType, SubId]
           }, Ctx).

alias(Manager, SubType, SubName, SubId) ->
    alias(Manager, SubType, SubName, SubId, #{}).

alias(Manager, SubType, SubName, SubId, Ctx) when is_binary(SubName), is_binary(SubId) ->
    patch(Manager, #{
            type => tether,
            verb => accrue,
            path => [sub_names, SubType, SubName],
            value => {push, SubId}
           }, Ctx).

unalias(Manager, SubType, SubName, SubId) ->
    unalias(Manager, SubType, SubName, SubId, #{}).

unalias(Manager, SubType, SubName, SubId, Ctx) when is_binary(SubName), is_binary(SubId) ->
    patch(Manager, #{
            type => tether,
            verb => accrue,
            path => [sub_names, SubType, SubName],
            value => {drop, SubId}
           }, Ctx).

%% admin

lookup_pool(Manager) ->
    lookup_pool(Manager, #{}).

lookup_pool(Manager, Ctx) ->
    patch(Manager, #{
            type => command,
            kind => chain,
            verb => lookup,
            path => pool
           }, Ctx).

change_pool(Manager, Change) ->
    change_pool(Manager, Change, #{}).

change_pool(Manager, Change, Ctx) ->
    patch(Manager, #{
            type => tether,
            verb => accrue,
            path => pool,
            value => Change
           }, Ctx).

inform_pool(Manager, Info) ->
    inform_pool(Manager, Info, #{}).

inform_pool(Manager, Info, Ctx) ->
    patch(Manager, #{
            type => measurement,
            kind => pool,
            value => Info
           }, Ctx).

report_pool(Manager) ->
    report_pool(Manager, #{}).

report_pool(Manager, Ctx) ->
    patch(Manager, #{
            type => command,
            verb => lookup,
            path => pool_info
           }, Ctx).

get_replication_factor(Manager, SubType) ->
    get_replication_factor(Manager, SubType, #{}).

get_replication_factor(Manager, SubType, Ctx) ->
    patch(Manager, #{
            type => command,
            kind => chain,
            verb => lookup,
            path => [replication, SubType]
           }, Ctx).

set_replication_factor(Manager, SubType, K) ->
    set_replication_factor(Manager, SubType, K, #{}).

set_replication_factor(Manager, SubType, K, Ctx) ->
    patch(Manager, #{
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
    request_relocate(Manager, AwayFrom, #{}).

request_relocate(Manager, AwayFrom, Ctx) ->
    patch(Manager, #{
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
    request_resources(Manager, Path, HaveWant, #{}).

request_resources(Manager, Path, HaveWant, Ctx) ->
    patch(Manager, #{
            type => request,
            kind => resources,
            path => Path,
            value => HaveWant
           }, Ctx).

%% 'patch' dispatches a message to a manager (but unwraps value by default)

patch(Manager, Message) ->
    patch(Manager, Message, #{}).

patch(Manager, Message, Ctx) ->
    loom:patch(Manager, Message, Ctx).

%% 'relay' dispatches a message to a subordinate of manager by name or id (or spec)

relay(Manager, Subish, Message) ->
    relay(Manager, Subish, Message, #{}).
relay(#{spec := Spec}, Subish, Message, Ctx) ->
    relay(Spec, Subish, Message, Ctx);
relay(Manager, Subish, Message, Ctx) ->
    loom:dispatch({Manager, Subish}, Message, Ctx).

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
    %% XXX some of these should be lmdbs or dets tables or something
    Builtins = maps:with([pool,
                          replication,
                          utilization,
                          sub_names,
                          sub_sites,
                          sub_pools], State),
    maps:merge(Builtins, callback(State, {keep, 1}, [State], #{})).

init(State) ->
    %% XXX could load dbs, etc
    State1 = State#{
               pool => util:get(State, pool, {[], undefined}),
               replication => util:get(State, replication, #{}),
               utilization => util:get(State, utilization, #{}),
               sub_names => util:get(State, sub_names, #{}),
               sub_sites => util:get(State, sub_sites, #{}),
               sub_pools => util:get(State, sub_pools, #{})
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

command_called(#{verb := accrue, path := pool}, Node, true, State) ->
    %% make sure all affected subordinates are updated to reflect the change
    %% we need to check both if their sites OR pools are affected by the change
    %% NB: if we inverted ids per node, we wouldn't have to check *all* subordinates
    %%     either way this is just a pool-sized factor in a time-space tradeoff
    Former = erloom_chain:value(State, former, []),
    Latter = erloom_chain:value(State, pool, []),
    Task = {fun ?MODULE:do_adjust_subs/2, {all, {pool, util:diff(Former, Latter)}}},
    loom:suture_task(adjust, Node, Task, cache_pool_info(State));

command_called(#{verb := accrue, path := [replication, SubType]}, Node, true, State) ->
    %% make sure all affected subordinates are updated to reflect the change
    %% we only need to check if the sites are affected, make sure we have the right number
    Task = {fun ?MODULE:do_adjust_subs/2, {{type, SubType}, replication}},
    loom:suture_task(adjust, Node, Task, State);

command_called(#{verb := lookup, what := {name, SubType, SubName}}, _, _, State) ->
    case erloom_chain:value(State, [sub_names, SubType, SubName]) of
        A when A =:= undefined; A =:= [] ->
            State#{resonse => {ok, {false, undefined}}};
        [SubId|_] ->
            SubSpec = subordinate_spec(State, [SubType, SubId]),
            Sites = erloom_chain:value(State, [sub_sites, SubType, SubId], []),
            State#{response => {ok, {false, {SubSpec, Sites}}}}
    end;

command_called(#{verb := obtain, what := {name, SubType, SubName}}, _, _, State)
  when is_binary(SubName) ->
    case erloom_chain:value(State, [sub_names, SubType, SubName]) of
        A when A =:= undefined; A =:= [] ->
            %% information will be propagated through the chain motion itself
            %% we dont need yet another emit, unless we want to choose the value later
            SubId = make_subordinate_id(SubType, SubName),
            Message = #{verb => create, path => [sub_names, SubType, SubName], value => [SubId]},
            loom:no_reply(loom:make_tether(Message, State));
        [SubId|_] ->
            SubSpec = subordinate_spec(State, [SubType, SubId]),
            Sites = erloom_chain:value(State, [sub_sites, SubType, SubId], []),
            State#{response => {ok, {false, {SubSpec, Sites}}}}
    end;

command_called(#{verb := create, path := [sub_names|Sub_], value := [Initial]}, Node, true, State) ->
    %% a new name has been created, just transfer to the initial owner
    Task = {fun ?MODULE:do_transfer_name/2, {Sub_, {undefined, Initial}}},
    loom:no_reply(loom:suture_task({name, Sub_}, Node, Task, State));

command_called(#{verb := accrue, path := [sub_names|Sub_], value := Change}, Node, true, State) ->
    %% an alias was created or destroyed, subordinates must reflect new ownership
    Transfer =
        case {Change, erloom_chain:value(State, [sub_names|Sub_])} of
            {{push, New}, [New]} ->
                {undefined, New};
            {{push, New}, [New, Old|_]} ->
                {Old, New};
            {{drop, Old}, []} ->
                {Old, undefined};
            {{drop, Old}, [New|_]} ->
                {Old, New}
        end,
    Task = {fun ?MODULE:do_transfer_name/2, {Sub_, Transfer}},
    loom:no_reply(loom:suture_task({name, Sub_}, Node, Task, State));

command_called(#{verb := lookup, what := {id, SubType, SubId}}, _, _, State) ->
    SubSpec = subordinate_spec(State, [SubType, SubId]),
    Sites = erloom_chain:value(State, [sub_sites, SubType, SubId], []),
    State#{response => {ok, {false, {SubSpec, Sites}}}};

command_called(#{verb := obtain, what := {id, SubType, SubId}}, _, _, State)
  when is_binary(SubId) ->
    case erloom_chain:value(State, [sub_sites, SubType, SubId]) of
        undefined ->
            %% get permission to change first, then choose the value and emit
            Message = #{verb => create, path => [sub_sites, SubType, SubId], value => []},
            loom:no_reply(loom:make_tether(Message, State));
        Sites ->
            SubSpec = subordinate_spec(State, [SubType, SubId]),
            State#{response => {ok, {false, {SubSpec, Sites}}}}
    end;

command_called(#{verb := create, path := [sub_sites|Sub]}, Node, true, State) when Node =:= node() ->
    %% subordinate created: choose sites, emit a command to communicate the result
    %% we suture because we depend on many messages (votes), not just this one
    Nodes = choose_nodes([sub_sites|Sub], State),
    Boiler = #{type => command, kind => chain, verb => accrue, value => {'=', Nodes}},
    State1 = loom:suture_yarn(Boiler#{path => [sub_sites|Sub]}, State),
    State2 =
        case subordinate_is_manager(State1, Sub) of
            true ->
                %% if its a manager, also choose an initial pool
                %% NB: for now this is always the same as sites, but could be independent
                loom:suture_yarn(Boiler#{path => [sub_pools|Sub]}, State1);
            false ->
                State1
        end,
    loom:no_reply(State2); %% NB: sub_sites task will return

command_called(#{verb := accrue, path := [sub_sites|Sub]}, Node, true, State) ->
    %% time to actually change the sites for the subordinate, reflect changes in stats
    %% NB: does not return, which is fine as it is only used from create / batch updates
    Former = erloom_chain:value(State, former, []),
    Latter = erloom_chain:value(State, [sub_sites|Sub], []),
    Task = {fun ?MODULE:do_control_sites/2, {Sub, {Former, Latter}}},
    State1 = pool_allocated([sub_sites|Sub], util:diff(Former, Latter), State),
    loom:no_reply(loom:stitch_task({sub, Sub}, Node, Task, State1));

command_called(#{verb := accrue, path := [sub_pools|Sub]}, Node, true, State) ->
    %% time to actually change the pool for the subordinate, reflect changes in stats
    %% NB: does not return, which is fine as it is only used from create / batch updates
    %%     the task can return as it is sure to run after the 'do_control_sites' on create
    Former = erloom_chain:value(State, former, []),
    Latter = erloom_chain:value(State, [sub_pools|Sub], []),
    Task = {fun ?MODULE:do_assign_pool/2, {Sub, {Former, Latter}}},
    State1 = pool_allocated([sub_pools|Sub], util:diff(Former, Latter), State),
    loom:no_reply(loom:stitch_task({sub, Sub}, Node, Task, State1));

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

wants_resources(Path = [sub_pools|_], HaveWant = {Have, Want}, State) ->
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
            Have = length(erloom_chain:value(State, pool, [])),
            ok = request_resources(Boss, [sub_pools, SubType, SubId], {Have, Have + 1}),
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
                      end, #{}, erloom_chain:value(State, pool))
     }.

pool_info(#{pool_info := PoolInfo}) ->
    PoolInfo;
pool_info(_State) ->
    #{}.

pool_allocated([sub_sites|_], {Add, Rem}, State = #{utilization := U}) ->
    U1 = lists:foldl(u9n_accruer(sites, {'+', 1}), U, Add),
    U2 = lists:foldl(u9n_accruer(sites, {'-', 1}), U1, Rem),
    cache_pool_info(State#{utilization => U2});
pool_allocated([sub_pools|_], {Add, Rem}, State = #{utilization := U}) ->
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
    util:modify(Utilization, Node,
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

%% manage resources

diff_op({Add, []}) ->
    {'+', Add};
diff_op({[], Rem}) ->
    {'-', Rem};
diff_op({Add, Rem}) ->
    [{'+', Add}, {'-', Rem}].

iter_subordinates(all, State) ->
    %% XXX: would be nice to return a lazy iterator when we add lmdb support
    util:fold(
      fun ({SubType, BySubId}, Acc) ->
              util:fold(
                fun ({SubId, _}, A) ->
                        [{SubType, SubId}|A]
                end, Acc, BySubId)
      end, [], util:lookup(State, [sub_sites], []));
iter_subordinates({type, SubType}, State) ->
    util:fold(
      fun ({SubId, _}, Acc) ->
              [{SubType, SubId}|Acc]
      end, [], util:lookup(State, [sub_sites, SubType], [])).

ranked_nodes(PoolInfo) ->
    util:vals(lists:keysort(1, util:index(PoolInfo, []))).

choose_nodes(Path, State) ->
    choose_nodes(Path, erloom_chain:value(State, Path), State).

choose_nodes(Path, Existing, State) ->
    util:edit(Existing, realloc_nodes(Path, Existing, State)).

realloc_nodes(Path = [sub_sites, SubType, _], Existing, State) ->
    %% the number of sites we want is always determined by the replication factor
    K = erloom_chain:value(State, [replication, SubType]),
    realloc_nodes(Path, Existing, K, State);
realloc_nodes(Path = [sub_pools|_], Existing, State) ->
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
maybe_realloc(_, Path = [sub_sites|_], Acc) ->
    %% any trigger can cause a potential realloc of sites
    potential_realloc(Path, Acc);
maybe_realloc({pool, {_, Rem}}, Path = [sub_pools|_], Acc) when Rem =/= [] ->
    %% only a pool shrinkage triggers potential realloc of subordinate pools
    potential_realloc(Path, Acc);
maybe_realloc(_Trigger, _Path, Acc) ->
    Acc.

potential_realloc(Path, {_, State} = Acc) ->
    Existing = erloom_chain:value(Path, State, []),
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
    {Retry, _} =
        util:chunk(
          fun (Chunk, {R, S}) when Phase =:= initial ->
                  %% initial attempt, actually figure out what we wan't to allocate
                  {Allocs, S1} =
                      util:fold(
                        fun ({SubType, SubId}, A) ->
                                A1 = maybe_realloc(Trigger, [sub_sites, SubType, SubId], A),
                                __ = maybe_realloc(Trigger, [sub_pools, SubType, SubId], A1)
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
    case loom:deliver(New, SubSpec, #{type => start, seed => New}) of
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
    %% we check the new nodes to see if the value is set, and submit to the old nodes if it is not
    SubSpec = subordinate_spec(State, Sub),
    case loom:deliver(New, SubSpec, #{type => command, verb => lookup, path => elect}) of
        {error, deliver, _} ->
            {retry, {10, seconds}, {undeliverable, {start, SubSpec}}};
        {ok, {ok, {_, Elect = #{current := Current}}}, #{dstat := #{node := Probed}}} ->
            %% check if the current conf is what we want already
            case util:get(Elect, Current) of
                {_, #{value := {'=', New}}, decided} ->
                    %% decided means accepted if its still there
                    {done, {ok, {false, {SubSpec, New}}}};
                {_, #{value := {'=', New}}, _} ->
                    %% wait to see if it passes
                    {retry, {10, seconds}, {wait, {Probed, pending}}};
                {_, #{}, _} ->
                    %% has another value, we ought to set it
                    case loom:deliver(Old, SubSpec, #{type => move, kind => conf, value => {'=', New}}) of
                        {ok, {ok, _}, _} ->
                            {done, {ok, {true, {SubSpec, New}}}};
                        {ok, {retry, stopped}, _} ->
                            %% if an old node is stopped, the group config must have already changed
                            %% either our change was successful, or someone else interfered
                            %% in either case we are done, if someone else is interfering, its their problem
                            {done, {ok, {true, {SubSpec, New}}}};
                        {_, Response, #{dstat := #{node := Node}}} ->
                            %% either the motion is pending or it failed, either way try again
                            {retry, {10, seconds}, {wait, {Node, Response}}}
                    end
            end
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
    Sites = erloom_chain:value(State, [sub_sites|Sub], []),
    Probe = #{
      type => command,
      kind => probe,
      verb => lookup,
      path => pool
     },
    case loom:deliver(Sites, SubSpec, Probe) of
        {error, Reason, _} ->
            %% subordinate could not be contacted
            {retry, {10, seconds}, {unreachable, {probe, SubSpec}, Reason}};
        {ok, {wait, Reason}, #{dstat := #{node := Node}}} ->
            %% a change is pending
            {retry, {10, seconds}, {wait, {Node, Reason}}};
        {ok, {ok, {_, New}}, _} ->
            %% the value has already been set, all done
            {done, ok};
        {ok, {ok, _}, #{dstat := #{node := Node}}} ->
            %% the value is something else, try sending
            Accrue = #{
              type => tether,
              verb => accrue,
              path => pool,
              value => {edit, util:diff(Old, New)}
             },
            case loom:rpc(Node, SubSpec, Accrue) of
                {ok, {_, New}} ->
                    {done, ok};
                Response ->
                    {retry, {10, seconds}, {wait, {Node, Response}}}
            end
    end.

%% name management
%% ensure that names are transferred between the subordinates to reflect alias changes

do_transfer_name({[SubType, SubName], {undefined, New}}, State) ->
    %% just give control to the new subordinate, since there is no old one
    do_micromanage(State, {id, SubType, New}, [names, SubName], true);
do_transfer_name({[SubType, SubName], {Old, undefined}}, State) ->
    %% just revoke the control from the old subordinate, since there is no new one
    do_micromanage(State, {id, SubType, Old}, [names, SubName], false);
do_transfer_name({[SubType, SubName], {Old, New}}, State) ->
    %% transfer control of the name to the new subordinate from the old one, if any
    %% the new subordinate relieves the old one of duty, and copies anything it needs
    %% during the transition period they might both think they own the name, which is fine
    %% the agency only points to one of them (who knows how long it takes for them to find out)
    %% NB: return the new {SubSpec, Sites}, not the old
    case do_micromanage(State, {id, SubType, New}, [names, SubName], true) of
        {done, Return} ->
            do_micromanage(State, {id, SubType, Old}, [names, SubName], false, Return);
        Other ->
            Other
    end.

do_micromanage(Manager, Subish, Path, Value) ->
    do_micromanage(Manager, Subish, Path, Value, undefined).

do_micromanage(Manager, Subish, Path, Value, Return) ->
    %% probe the path first to prevent duplicating requests
    %% then wait, modify, or retry as needed
    %% NB: always returns the {SubSpec, Sites} as needed by create
    Probe = #{
      type => command,
      kind => probe,
      verb => lookup,
      path => Path
     },
    case relay(Manager, Subish, Probe) of
        {error, Reason, _} ->
            %% subordinate could not be contacted
            {retry, {10, seconds}, {unreachable, {probe, Subish}, Reason}};
        {ok, {wait, Reason}, #{dstat := #{node := Node}}} ->
            %% a change is pending
            {retry, {10, seconds}, {wait, {Node, Reason}}};
        {ok, {ok, {_, Value}}, #{dstat := #{spec := SubSpec, nodes := Sites}}} ->
            %% the value has already been set, all done
            {done, util:def(Return, {ok, {false, {SubSpec, Sites}}})};
        {ok, {ok, _}, #{dstat := #{spec := SubSpec, node := Node, nodes := Sites}}} ->
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
                    {retry, {10, seconds}, {wait, {Node, Response}}}
            end
    end.
