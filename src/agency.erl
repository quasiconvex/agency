-module(agency).
-include("agency.hrl").

%% The agency maps identities (a.k.a. 'names') to accounts managed by agents.
%% The agency keeps a list of N nodes (a.k.a. 'sites') where it can place agents.
%% The agency ensures each account is represented by K agents at K distinct sites.
%% Nodes can be added / removed from the cluster, K can be increased / decreased.
%% However, 1 <= K <= N is the intended usage.

%% api
-export([spec/2,
         agent/2,
         connect/2,
         connect/3,
         resolve/2,
         resolve/3]).

%% atomic
-export([obtain_identity/2,
         obtain_identity/3,
         obtain_account/2,
         obtain_account/3,
         link_identity/3,
         link_identity/4,
         unlink_identity/3,
         unlink_identity/4]).

%% admin
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

%% raw
-export([patch/2,
         patch/3,
         relay/3,
         relay/4]).

%% tasks
-export([micromanage/4,
         micromanage/5,
         do_transfer_identity/2,
         do_control_sites/2,
         do_ensure_sites/2]).

%% loom
-behavior(loom).
-export([home/1,
         proc/1,
         keep/1,
         init/1,
         write_through/3,
         handle_message/4,
         motion_decided/4,
         task_completed/4,
         task_continued/5]).

-export([callback/3,
         callback/4]).

-callback home(#agency{}) -> loom:home().

%% api

spec(Mod, AgentMod) ->
    #agency{mod=Mod, agent_mod=AgentMod}.

agent(#{spec := Agency}, Account) ->
    agent(Agency, Account);
agent(#agency{agent_mod=AgentMod}, Account) ->
    agent:spec(AgentMod, Account).

connect(Agency, Identity) ->
    connect(Agency, Identity, []).

connect(Agency, Identity, Opts) ->
    %% if the identity actually maps to a different account than we thought, its ok
    %% we can still connect to the old account:
    %%  when the agents for the new account take over, they must notify the old agents
    %%  eventually the old account will have a message indicating it was unlinked
    %%  when the agent is in an unlinked state, it refuses / closes client connections
    %%  when the client sees this, it forgets cached info, and tries to reconnect
    Message =
        util:update(#{
                       type => conn,
                       from => self(),
                       as => Identity
                     }, util:select(Opts, [since])),
    case relay(Agency, {identity, Identity}, Message, Opts) of
        {Node, Spec, ok} ->
            {ok, {Node, Spec}};
        {_, _, Error} ->
            Error
    end.

resolve(Agency, Identity) ->
    resolve(Agency, Identity, []).

resolve(Agency, Identity, Opts) ->
    case obtain_identity(Agency, Identity, Opts) of
        {ok, [Account|_]} ->
            case obtain_account(Agency, Account, Opts) of
                {ok, Nodes} ->
                    {ok, {Account, Nodes}};
                {error, Reason} ->
                    {error, {account, Account, Reason}}
            end;
        {ok, []} ->
            {error, {identity, Identity, no_account}};
        {error, Reason} ->
            {error, {identity, Identity, Reason}}
    end.

obtain_identity(Agency, Identity) ->
    obtain_identity(Agency, Identity, []).

obtain_identity(Agency, Identity, Opts) when is_binary(Identity) ->
    patch(Agency, #{
            type => command,
            verb => obtain,
            path => [identities, Identity]
           }, Opts).

obtain_account(Agency, Account) ->
    obtain_account(Agency, Account, []).

obtain_account(Agency, Account, Opts) when is_binary(Account) ->
    patch(Agency, #{
            type => command,
            verb => obtain,
            path => [accounts, Account]
           }, Opts).

link_identity(Agency, Identity, Account) ->
    link_identity(Agency, Identity, Account, []).

link_identity(Agency, Identity, Account, Opts) when is_binary(Identity), is_binary(Account) ->
    patch(Agency, #{
            type => tether,
            verb => accrue,
            path => [identities, Identity],
            value => {push, Account}
           }, Opts).

unlink_identity(Agency, Identity, Account) ->
    unlink_identity(Agency, Identity, Account, []).

unlink_identity(Agency, Identity, Account, Opts) when is_binary(Identity), is_binary(Account) ->
    patch(Agency, #{
            type => tether,
            verb => accrue,
            path => [identities, Identity],
            value => {drop, Account}
           }, Opts).

change_sites(Agency, Change) ->
    change_sites(Agency, Change, []).

change_sites(Agency, Change, Opts) ->
    patch(Agency, #{
            type => tether,
            verb => accrue,
            path => sites,
            value => Change
           }, Opts).

lookup_sites(Agency) ->
    lookup_sites(Agency, []).

lookup_sites(Agency, Opts) ->
    patch(Agency, #{
            type => command,
            kind => chain,
            verb => lookup,
            path => sites
           }, Opts).

set_sites_per_account(Agency, K) ->
    set_sites_per_account(Agency, K, []).

set_sites_per_account(Agency, K, Opts) ->
    patch(Agency, #{
            type => tether,
            kind => chain,
            verb => accrue,
            path => sites_per_account,
            value => {'=', K}
           }, Opts).

get_sites_per_account(Agency) ->
    get_sites_per_account(Agency, []).

get_sites_per_account(Agency, Opts) ->
    patch(Agency, #{
            type => command,
            kind => chain,
            verb => lookup,
            path => sites_per_account
           }, Opts).

check_placement(Agency) ->
    check_placement(Agency, []).

check_placement(Agency, Opts) ->
    patch(Agency, #{
            type => command,
            verb => lookup,
            path => placement
           }, Opts).

patch(Agency, Message) ->
    patch(Agency, Message, []).

patch(Agency, Message, Opts) ->
    loom:dispatch(Agency, Message, util:ifndef(Opts, wrapped, false)).

relay(Agency, Agentish, Message) ->
    relay(Agency, Agentish, Message, []).

relay(Agency, {identity, Identity}, Message, Opts) ->
    case obtain_identity(Agency, Identity, #{wrapped => true}) of
        {Node, _, {ok, [Account|_]}} ->
            relay(Agency, {account, Account}, Message, util:set(Opts, pref, Node));
        {Node, Agency, {error, Reason}} ->
            {Node, Agency, {error, {identity, Identity, Reason}}}
    end;
relay(Agency, {account, Account}, Message, Opts) ->
    %% ensure that a delegate from the account eventually accomplishes some task
    case obtain_account(Agency, Account, #{wrapped => true}) of
        {_, _, {ok, [_|_] = Nodes}} ->
            loom:deliver(Nodes, agent(Agency, Account), Message, Opts);
        {Node, Agency, {ok, []}} ->
            %% probably either K = 0 or N = 0
            {Node, Agency, {error, {account, Account, no_sites}}};
        {Node, Agency, {error, Reason}} ->
            {Node, Agency, {error, {account, Account, Reason}}}
    end;
relay(_, Agent, Message, Opts) ->
    loom:dispatch(Agent, Message, Opts).

%% task api

micromanage(Agency, Agentish, Path, Value) ->
    micromanage(Agency, Agentish, Path, Value, []).

micromanage(Agency, Agentish, Path, Value, Opts) ->
    %% probe the path first to prevent duplicating requests
    %% then wait, modify, or retry as needed
    Probe = #{
      type => command,
      kind => probe,
      verb => lookup,
      path => Path
     },
    case relay(Agency, Agentish, Probe, Opts) of
        {undefined, Agent, _} ->
            %% agent could not be contacted
            {retry, {10, seconds}, {undeliverable, {probe, Agent}}};
        {Node, _, {wait, Reason}} ->
            %% a change is pending
            {retry, {10, seconds}, {wait, {Node, Reason}}};
        {_, _, {ok, Value}} ->
            %% the value has already been set, all done
            {done, ok};
        {Node, Agent, {ok, _}} ->
            %% the value hasn't even begun to change, try sending
            Modify = #{
              type => tether,
              verb => modify,
              path => Path,
              value => Value
             },
            case loom:rpc(Node, Agent, Modify) of
                {ok, Value} ->
                    {done, ok};
                Response ->
                    {retry, {10, seconds}, {wait, {Node, Response}}}
            end
    end.

%% loom

proc(#agency{mod=Mod} = Spec) ->
    callback(Spec, {proc, 1}, [Spec], fun () -> erloom_registry:proc(Mod, Spec) end).

home(Spec) ->
    callback(Spec, {home, 1}, [Spec]).

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
        A when A =:= undefined; A =:= [] ->
            %% information will be propagated through the chain motion itself
            %% we dont need yet another emit, unless we want to choose the value later
            Account = create_account(Identity),
            Message = #{
              verb => accrue,
              path => [identities, Identity],
              value => {push, Account}
             },
            loom:make_tether(Message, State);
        Account ->
            State#{response => {ok, Account}}
    end;

handle_message(#{verb := obtain, path := [accounts, Account]}, _, true, State) ->
    case erloom_chain:value(State, [accounts, Account]) of
        undefined ->
            %% get permission to change first, then choose the value and emit
            loom:make_tether(#{verb => create, path => [accounts, Account]}, State);
        Nodes ->
            State#{response => {ok, Nodes}}
    end;

handle_message(#{verb := accrue, path := [accounts, Account]}, Node, _, State) ->
    %% we receive the change command (i.e. create motion already passed, or ensure running)
    %% update the sites count: increment every added node, decrement every removed node
    Former = util:get(State, former),
    Latter = erloom_chain:lookup(State, [accounts, Account]),
    Task = {fun ?MODULE:do_control_sites/2, {Account, Former, Latter}},
    State1 = update_placement(util:diff(util:def(element(1, Former), []),
                                        util:def(element(1, Latter), [])), State),
    loom:stitch_task({sites, Account}, Node, Task, State1);

handle_message(_Message, _Node, _IsNew, State) ->
    State.

motion_decided(#{verb := accrue, path := sites}, Mover, {true, _}, State) ->
    %% make sure all accounts are updated to reflect the change, if needed
    %% NB: if we kept accounts per node, we wouldn't have to check *all* accounts
    Task = {fun ?MODULE:do_ensure_sites/2, {all}},
    State1 =
        util:modify(State, placement,
                    fun (Placement) ->
                            util:update(util:mapped(sites(State), 0), Placement)
                    end),
    loom:suture_task({change, sites}, Mover, Task, State1);

motion_decided(#{verb := accrue, path := sites_per_account}, Mover, {true, _}, State) ->
    %% make sure all accounts are updated to reflect the change, if needed
    Task = {fun ?MODULE:do_ensure_sites/2, {all}},
    loom:suture_task({change, sites}, Mover, Task, State);

motion_decided(#{verb := accrue, path := [identities, Identity], value := Change}, Node, _, State) ->
    %% an identity was linked / unlinked to an account
    Transfer =
        case {Change, erloom_chain:value(State, [identities, Identity])} of
            {{push, New}, [New]} ->
                {undefined, New};
            {{push, New}, [New, Old|_]} ->
                {Old, New};
            {{drop, Old}, []} ->
                {Old, undefined};
            {{drop, Old}, [New|_]} ->
                {Old, New}
        end,
    Task = {fun ?MODULE:do_transfer_identity/2, {Identity, Transfer}},
    loom:suture_task({ident, Identity}, Node, Task, State);

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

task_completed(Message, Node, Result, State) ->
    callback(State, {task_completed, 4}, [Message, Node, Result, State], State).

task_continued(Name, Reason, Clock, Arg, State) ->
    callback(State, {task_continued, 5}, [Name, Reason, Clock, Arg, State], ok).

callback(#{spec := Spec}, FA, Args) ->
    callback(Spec, FA, Args);
callback(#agency{mod=Mod}, FA, Args) ->
    loom:callback(Mod, FA, Args).

callback(#{spec := Spec}, FA, Args, Default) ->
    callback(Spec, FA, Args, Default);
callback(#agency{mod=Mod}, FA, Args, Default) ->
    loom:callback(Mod, FA, Args, Default).

%% helpers

create_account(Identity) when is_binary(Identity) ->
    util:bin([base64url:encode(Identity), "-", time:stamp(time:unow(), tai64)]).

%% identity management

do_transfer_identity({Identity, {Old, undefined}}, State) ->
    %% just revoke the control from the old account, since there is no new one
    micromanage(State, {account, Old}, [identities, Identity], false);
do_transfer_identity({Identity, {undefined, New}}, State) ->
    %% just give control to the new account, since there is no old one
    micromanage(State, {account, New}, [identities, Identity], true);
do_transfer_identity({Identity, {Old, New}}, State) ->
    %% transfer control of the identity to the new account from the old one, if any
    %% the new account relieves the old one of duty, and copies anything it needs
    %% during the transition period they might both think they own the identity, which is fine
    %% the agency only points to one of them (who knows how long it takes for them to find out)
    case micromanage(State, {account, New}, [identities, Identity], true) of
        {done, _} ->
            micromanage(State, {account, Old}, [identities, Identity], false);
        Other ->
            Other
    end.

%% sites and placement

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
    Remaining = util:select(Existing, sites(State)),
    Placement = select_placement(State),
    choose_sites(Account, Remaining, length(Remaining), K, Placement).

choose_sites(_, Remaining, R, K, Placement) when R < K ->
    %% choose nodes to add
    %% rank the sites by size (the smaller the better)
    %% NB: we may return less than K
    Prefs = maps:fold(fun (Node, Size, Acc) ->
                              [{Size, Node}|Acc]
                      end, [], util:except(Placement, Remaining)),
    lists:sublist(util:vals(lists:keysort(1, Prefs)), K - R) ++ Remaining;
choose_sites(_, Remaining, R, K, Placement) when R > K ->
    %% choose nodes to drop
    %% rank the sites by size (the bigger the better)
    Prefs = maps:fold(fun (Node, Size, Acc) ->
                              [{-Size, Node}|Acc]
                      end, [], util:select(Placement, Remaining)),
    Remaining -- lists:sublist(util:vals(lists:keysort(1, Prefs)), R - K);
choose_sites(_, Remaining, _, _, _) ->
    %% nothing to add or drop
    Remaining.

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
                          {ok, _} = loom:call(S, Command),
                          update_placement(util:diff(Nodes, Nodes1), S)
                  end;
              ({_, _}, S) ->
                  %% skip locked accounts
                  %% XXX: we must keep retrying until we are sure about all accounts
                  S
          end, State, util:get(State, accounts)),
    {done, all}.

do_control_sites({Account, {Old, _}, {[_|_] = New, _}}, State) when Old =:= undefined; Old =:= [] ->
    %% we can stop as soon as the agent acknowledges the message
    %% the start message will keep trying to seed until it succeeds
    %% since the root conf is symbolic, we can safely try seeding different nodes when we don't get a reply
    case loom:deliver(New, agent(State, Account), #{type => start, seed => New}) of
        {undefined, Agent, _} ->
            {retry, {10, seconds}, {undeliverable, {seed, Agent}}};
        {_, _, ok} ->
            {done, ok}
    end;
do_control_sites({Account, {[_|_] = Old, _}, {New, _}}, State) ->
    %% we can stop as soon as the agent accepts responsibility for the change
    %% once the motion passes it will take effect, eventually
    %% we check the new nodes to see if the value is set, and submit to the old nodes if it is not
    case loom:deliver(New, agent(State, Account), #{type => command, verb => lookup, path => elect}) of
        {undefined, Agent, _} ->
            {retry, {10, seconds}, {undeliverable, {start, Agent}}};
        {Probed, Agent, {ok, Elect = #{current := Current}}} ->
            %% check if the current conf is what we want already
            case util:get(Elect, Current) of
                {_, #{value := {'=', New}}, decided} ->
                    %% decided means accepted if its still there
                    {done, ok};
                {_, #{value := {'=', New}}, _} ->
                    %% wait to see if it passes
                    {retry, {10, seconds}, {wait, {Probed, pending}}};
                {_, #{}, _} ->
                    %% has another value, we ought to set it
                    case loom:deliver(Old, Agent, #{type => move, kind => conf, value => {'=', New}}) of
                        {_, _, {ok, _}} ->
                            {done, ok};
                        {_, _, {retry, stopped}} ->
                            %% if an old node is stopped, the group config must have already changed
                            %% either our change was successful, or someone else interfered
                            %% in either case we are done, if someone else is interfering, its their problem
                            {done, stopped};
                        {Node, _, Response} ->
                            %% either the motion is pending or it failed, either way try again
                            {retry, {10, seconds}, {wait, {Node, Response}}}
                    end
            end
    end;
do_control_sites({_, _, _}, _) ->
    %% there are no old nodes and no new ones: weird, but someone must have asked us to do it
    %% controlling them is easy: nothing to do
    {done, empty}.
