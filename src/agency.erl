-module(agency).
-include("agency.hrl").

%% The agency maps identities (a.k.a. 'names') to accounts managed by agents.
%% The agency keeps a list of N nodes (a.k.a. 'sites') where it can place agents.
%% The agency ensures each account is represented by K agents at K distinct sites.
%% Nodes can be added / removed from the cluster, K can be increased / decreased.
%% However, 1 <= K <= N is the intended usage.

%% api
-export([spec/3,
         agents/2,
         agents/3,
         connect/2,
         connect/3]).

-export([obtain_identity/2,
         obtain_identity/3,
         obtain_account/2,
         obtain_account/3,
         link_identity/3,
         link_identity/4,
         unlink_identity/3,
         unlink_identity/4]).

-export([change_sites/2,
         change_sites/3,
         lookup_sites/1,
         lookup_sites/2,
         set_sites_per_account/2,
         set_sites_per_account/3,
         get_sites_per_account/1,
         get_sites_per_account/2,
         check_placement/1,
         check_placement/2]).

%% loom
-behavior(loom).
-export([home/1,
         proc/1,
         keep/1,
         init/1,
         write_through/3,
         handle_message/4,
         motion_decided/4]).

%% api

spec(Name, Home, AgentMod) ->
    #agency{name=Name, home=Home, agent_mod=AgentMod}.

agents(Spec, Identity) ->
    agents(Spec, Identity, []).

agents(Spec, Identity, Opts) ->
    case obtain_identity(Spec, Identity, Opts) of
        {ok, [Account|_]} ->
            case obtain_account(Spec, Account, Opts) of
                {ok, Nodes} ->
                    {ok, {Account, Nodes}};
                {error, Reason} ->
                    {error, Reason}
            end;
        {ok, []} ->
            {error, no_account};
        {error, Reason} ->
            {error, Reason}
    end.

connect(Spec, Which) ->
    connect(Spec, Which, []).

connect(Spec, Identity, Opts) when is_binary(Identity) ->
    case agents(Spec, Identity, Opts) of
        {ok, Agents} ->
            connect(Spec, Agents, Opts);
        {error, Reason} ->
            {error, Reason}
    end;
connect(_, {_, []}, _) ->
    {error, failed};
connect(Spec, {Account, Nodes}, Opts) when is_list(Nodes) ->
    Node = util:random(Nodes),
    case connect(Spec, {Account, Node}, Opts) of
        {ok, Node} ->
            {ok, Node};
        _ ->
            connect(Spec, {Account, util:delete(Nodes, Node)}, Opts)
    end;
connect(Spec, {Account, Node}, Opts) ->
    rpc(Node, agent_spec(Spec, Account), #{type => conn, from => self()}, Opts).

obtain_identity(Spec, Identity) ->
    obtain_identity(Spec, Identity, []).

obtain_identity(Spec, Identity, Opts) ->
    call(Spec, #{
           type => command,
           verb => obtain,
           path => [identities, Identity]
          }, Opts).

obtain_account(Spec, Account) ->
    obtain_account(Spec, Account, []).

obtain_account(Spec, Account, Opts) ->
    call(Spec, #{
           type => command,
           verb => obtain,
           path => [accounts, Account]
          }, Opts).

link_identity(Spec, Identity, Account) ->
    link_identity(Spec, Identity, Account, []).

link_identity(Spec, Identity, Account, Opts) ->
    call(Spec, #{
           type => tether,
           verb => accrue,
           path => [identities, Identity],
           value => {cons, Account}
          }, Opts).

unlink_identity(Spec, Identity, Account) ->
    unlink_identity(Spec, Identity, Account, []).

unlink_identity(Spec, Identity, Account, Opts) ->
    call(Spec, #{
           type => tether,
           verb => accrue,
           path => [identities, Identity],
           value => {tail, Account}
          }, Opts).

change_sites(Spec, Change) ->
    change_sites(Spec, Change, []).

change_sites(Spec, Change, Opts) ->
    call(Spec, #{
           type => tether,
           verb => accrue,
           path => sites,
           value => Change
          }, Opts).

lookup_sites(Spec) ->
    lookup_sites(Spec, []).

lookup_sites(Spec, Opts) ->
    call(Spec, #{
           type => command,
           kind => chain,
           verb => lookup,
           path => sites
          }, Opts).

set_sites_per_account(Spec, K) ->
    set_sites_per_account(Spec, K, []).

set_sites_per_account(Spec, K, Opts) ->
    call(Spec, #{
           type => tether,
           kind => chain,
           verb => accrue,
           path => sites_per_account,
           value => {'=', K}
          }, Opts).

get_sites_per_account(Spec) ->
    get_sites_per_account(Spec, []).

get_sites_per_account(Spec, Opts) ->
    call(Spec, #{
           type => command,
           kind => chain,
           verb => lookup,
           path => sites_per_account
          }, Opts).

check_placement(Spec) ->
    check_placement(Spec, []).

check_placement(Spec, Opts) ->
    call(Spec, #{
           type => command,
           verb => lookup,
           path => placement
          }, Opts).

call(Spec, Message, Opts) ->
    loom:call(Spec, Message, util:get(Opts, timeout, 5000)).

rpc(Node, Spec, Message, Opts) ->
    loom:rpc(Node, Spec, Message, util:get(Opts, timeout, 30000)).

%% loom

home(#agency{home=Home}) when is_function(Home) ->
    Home();
home(#agency{home=Home}) ->
    Home.

proc(#agency{name=Name} = Spec) ->
    erloom_registry:proc(Name, Spec).

keep(State) ->
    %% XXX these should be lmdbs or dets tables or something
    maps:with([sites, placement, identities, accounts], State).

init(State) ->
    %% XXX could load dbs, etc
    State#{
      sites => util:get(State, sites, {[], undefined}),
      placement => util:get(State, placement, #{}),
      identities => util:get(State, identities, #{}),
      accounts => util:get(State, accounts, #{})
     }.

write_through(#{type := command, verb := lookup}, _, _State) ->
    {0, infinity};
write_through(#{type := command, verb := obtain}, _, _State) ->
    {0, infinity};
write_through(_, _, _State) ->
    {1, infinity}.

handle_message(#{verb := obtain, path := [identities, Identity]}, _, true, State) ->
    case erloom_chain:value(State, [identities, Identity]) of
        undefined ->
            %% information will be propagated through the chain motion itself
            %% we dont need yet another emit, unless we want to choose the value later
            Account = create_account(Identity),
            Message = #{
              verb => accrue,
              path => [identities, Identity],
              value => {'=', [Account]}
             },
            loom:maybe_chain(Message, State);
        Account ->
            loom:maybe_reply({ok, Account}, State)
    end;

handle_message(#{verb := obtain, path := [accounts, Account]}, _, true, State) ->
    case erloom_chain:value(State, [accounts, Account]) of
        undefined ->
            %% get permission to change first, then choose the value and emit
            loom:maybe_chain(#{verb => create, path => [accounts, Account]}, State);
        Nodes ->
            loom:maybe_reply({ok, Nodes}, State)
    end;

handle_message(#{verb := accrue, path := [accounts, Account]}, Node, _, State) ->
    %% we receive the change command (i.e. create motion already passed, or ensure running)
    %% update the sites count: increment every added node, decrement every removed node
    Former = util:get(State, former),
    Latter = erloom_chain:lookup(State, [accounts, Account]),
    Task = {fun do_control_sites/2, {Account, Former, Latter}},
    State1 = update_placement(util:diff(util:def(element(1, Former), []),
                                        util:def(element(1, Latter), [])), State),
    State2 = loom:stitch_task({ctrl, Account}, Node, Task, State1),
    loom:maybe_reply(State2);

handle_message(_Message, _Node, _IsNew, State) ->
    loom:maybe_reply(State).

motion_decided(#{verb := accrue, path := sites}, Mover, {true, _}, State) ->
    %% make sure all accounts are updated to reflect the change, if needed
    %% NB: if we kept accounts per node, we wouldn't have to check *all* accounts
    Task = {fun do_ensure_sites/2, {all}},
    State1 =
        util:modify(State, placement,
                    fun (Placement) ->
                            util:update(util:mapped(sites(State), 0), Placement)
                    end),
    loom:stitch_task({change, sites}, Mover, Task, State1);

motion_decided(#{verb := accrue, path := sites_per_account}, Mover, {true, _}, State) ->
    %% make sure all accounts are updated to reflect the change, if needed
    Task = {fun do_ensure_sites/2, {all}},
    loom:stitch_task({change, sites}, Mover, Task, State);

motion_decided(#{verb := create, path := [accounts, Account]}, Mover, {true, _}, State) when Mover =:= node() ->
    %% choose nodes for the account, emit a command to communicate the result
    %% we suture because we depend on many messages (votes), not just this one
    Nodes = choose_sites(Account, State),
    Command = #{
      type => command,
      kind => chain,
      verb => accrue,
      path => [accounts, Account],
      value => {'=', Nodes}
     },
    loom:suture_yarn(Command, State#{response => {ok, Nodes}});

motion_decided(_Motion, _Mover, _Decision, State) ->
    State.

%% helpers

agent_spec(#{spec := Spec}, Account) ->
    agent_spec(Spec, Account);
agent_spec(#agency{agent_mod=AgentMod}, Account) ->
    agent:spec(AgentMod, Account).

create_account(Identity) when is_binary(Identity) ->
    util:bin([base64url:encode(Identity), "-", time:stamp(time:unow(), tai64)]).

sites(State) ->
    erloom_chain:value(State, sites).

sites_per_account(State) ->
    erloom_chain:value(State, sites_per_account, 1).

select_placement(State = #{placement := Placement}) ->
    util:select(Placement, sites(State)).

update_placement({Added, Removed}, State = #{placement := Placement}) ->
    Placement1 = util:reduce(fun util:increment/2, Placement, Added),
    Placement2 = util:reduce(fun util:decrement/2, Placement1, Removed),
    State#{placement => Placement2}.

choose_sites(Account, State) ->
    choose_sites(Account, [], State).

choose_sites(Account, Existing, State) ->
    choose_sites(Account, Existing, sites_per_account(State), State).

choose_sites(Account, Existing, K, State) ->
    choose_sites(Account, Existing, length(Existing), K, select_placement(State)).

choose_sites(_, Existing, E, K, Placement) when E < K ->
    %% choose nodes to add
    %% rank the sites by size (the smaller the better)
    %% NB: we may return less than K
    Prefs = maps:fold(fun (Node, Size, Acc) ->
                              [{Size, Node}|Acc]
                      end, [], util:except(Placement, Existing)),
    lists:sublist(util:vals(lists:keysort(1, Prefs)), K - E) ++ Existing;
choose_sites(_, Existing, E, K, Placement) when E > K ->
    %% choose nodes to drop
    %% rank the sites by size (the bigger the better)
    Prefs = maps:fold(fun (Node, Size, Acc) ->
                              [{-Size, Node}|Acc]
                      end, [], util:select(Placement, Existing)),
    Existing -- lists:sublist(util:vals(lists:keysort(1, Prefs)), E - K);
choose_sites(_, Existing, _, _, _) ->
    %% nothing to add or drop
    Existing.

do_ensure_sites({all}, State) ->
    %% go through every account, check that it has the right number of sites
    %% this task should be globally queued, so that only one works at a time
    %% noone else is supposed to modify the account sites, except during creation
    %% therefore we don't bother to vote on changes
    %% to remove accounts, we need to keep a separate list and handle them here
    K = sites_per_account(State),
    _ = util:fold(
          fun ({Account, {Nodes, _Version}}, S) ->
                  %% if we need to choose sites, send a command back to the listener
                  case choose_sites(Account, Nodes, K, S) of
                      Nodes ->
                          S;
                      Nodes1 ->
                          Command = #{
                            type => command,
                            kind => chain,
                            verb => accrue,
                            path => [accounts, Account],
                            value => {'=', Nodes1}
                           },
                          {ok, _} = loom:call(S, Command, 10000),
                          update_placement(util:diff(Nodes, Nodes1), S)
                  end;
              ({_, _}, S) ->
                  %% skip locked accounts
                  %% XXX: we must keep retrying until we are sure about all accounts
                  S
          end, State, util:get(State, accounts)),
    {done, ok}.

do_control_sites({Account, {Old, _}, {[First|_] = New, _}}, State) when Old =:= undefined; Old =:= [] ->
    %% we can stop as soon as the agent acknowledges the message
    %% the start message will keep trying to seed until it succeeds
    %% we always seed the same node on retry, which guarantees its idempotence
    Start = #{type => start, seed => New},
    case loom:rpc(First, agent_spec(State, Account), Start) of
        ok ->
            {done, ok};
        _ ->
            {retry, {60, seconds}}
    end;
do_control_sites({Account, {[_|_] = Old, _}, {New, _}}, State) ->
    %% we can stop as soon as the agent accepts responsibility for the change
    %% once the motion passes it will take effect, eventually
    Delegate = util:random(Old),
    Start = #{type => move, kind => conf, value => {'=', New}},
    case loom:rpc(Delegate, agent_spec(State, Account), Start) of
        {ok, _} ->
            {done, ok};
        {error, stopped} ->
            %% if an old node is stopped, the group config must have already changed
            %% either our change was successful, or someone else interfered
            %% in either case we are done, if someone else is interfering, its their problem
            {done, stopped};
        _ ->
            {retry, {60, seconds}}
    end;
do_control_sites({_, _, _}, _) ->
    %% there are no old nodes and no new ones: weird, but someone must have asked us to do it
    %% controlling them is easy: nothing to do
    {done, empty}.
