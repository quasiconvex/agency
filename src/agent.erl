-module(agent).
-include("agency.hrl").

-export_type([identity/0,
              account/0,
              state/0,
              conn/0]).

-type identity() :: manager:sub_name().
-type account() :: manager:sub_id().

-type state() :: #{ %% + loom:state()
             roles => [atom()],
             clients => #{}
            }.

-type conn() :: #{ %% + loom:message()
            type => conn,
            from => pid(),
            as => identity(),
            since => {binary(), erloom:edge()}
}.

-callback spec(#manager{}, account()) -> #agent{}.
-callback home(#agent{}) -> loom:home().
-callback is_observable(loom:message(), identity(), loom:state()) -> boolean().

-optional_callbacks([is_observable/3]).

%% agent
-export([agency/1,
         connect/3,
         connect/4]).

%% loom
-behavior(loom).
-export([find/1,
         home/1,
         opts/1,
         keep/1,
         init/1,
         verify_message/2,
         write_through/3,
         handle_info/2,
         handle_message/4,
         motion_decided/4]).

-export([callback/3,
         callback/4]).

%% agent

agency(#agent{agency=Agency}) ->
    Agency.

connect(Agency, AgentMod, Identity) ->
    connect(Agency, AgentMod, Identity, []).

connect(Agency, AgentMod, Identity, Opts) ->
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
    case manager:relay(Agency, {name, AgentMod, Identity}, Message, Opts) of
        {Node, Spec, ok} ->
            {ok, {Node, Spec}};
        {_, _, Error} ->
            Error
    end.

is_observable(Message, Identity, State) ->
    Default =
        case Message of
            #{path := [user|_]} ->
                true;
            _ ->
                false
        end,
    callback(State, {is_observable, 3}, [Message, Identity, State], Default).

%% loom

find(Spec = #agent{mod=AgentMod, account=Account}) ->
    case manager:obtain_sites(agency(Spec), AgentMod, Account, []) of
        {ok, {_, Nodes}} ->
            Nodes;
        {error, _} ->
            []
    end.

home(Spec) ->
    callback(Spec, {home, 1}, [Spec]).

opts(Spec) ->
    Defaults = #{idle_timeout => time:timeout({5, minutes})},
    maps:merge(Defaults, callback(Spec, {opts, 1}, [Spec], #{})).

keep(State) ->
    Builtins = maps:with([roles], State),
    maps:merge(Builtins, callback(State, {keep, 1}, [State], #{})).

init(State) ->
    State#{
      roles => util:get(State, roles, [])
     }.

verify_message(Message = #{scope := Scope}, State) ->
    %% if the message is accepted, we assume we are supposed to handle it
    %% handlers must match on the scope if they want to limit access using roles
    case util:has(util:get(State, roles, []), Scope) of
        true ->
            callback(State, {verify_message, 2}, [Message, State], {ok, Message, State});
        false ->
            {error, not_authorized, State}
    end;
verify_message(Message, State) ->
    callback(State, {verify_message, 2}, [Message, State], {ok, Message, State}).

write_through(Message = #{verb := lookup}, N, State) ->
    callback(State, {write_through, 3}, [Message, N, State], {0, infinity});
write_through(Message, N, State) ->
    callback(State, {write_through, 3}, [Message, N, State], {1, infinity}).

handle_info(Info = {'EXIT', From, _}, State) ->
    case util:exists(State, [clients, From]) of
        true ->
            State1 = util:remove(State, [clients, From]),
            callback(State1, {handle_info, 2}, [Info, State1], State1);
        false ->
            Untrap = fun () -> proc:untrap(Info, State) end,
            callback(State, {handle_info, 2}, [Info, State], Untrap)
    end;
handle_info(Info, State) ->
    callback(State, {handle_info, 2}, [Info, State], State).

handle_builtin(#{type := conn, from := From, as := Identity}, _, true, State) ->
    case erloom_chain:value(State, [names, Identity]) of
        true ->
            link(From),
            State1 = util:modify(State, [clients, From], Identity),
            State2 = loom:maybe_reply(ok, State1),
            catchup_client(From, Identity, util:get(State2, since), State2);
        _ ->
            %% we refuse conns for identities we dont manage
            State#{response => {error, {names, Identity}}}
    end;

handle_builtin(_Message, _Node, _IsNew, State) ->
    State.

handle_message(Message, Node, IsNew, State) ->
    State1 = handle_builtin(Message, Node, IsNew, State),
    State2 = forward_clients(Message, State1),
    callback(State2, {handle_message, 4}, [Message, Node, IsNew, State2], State2).

motion_builtin(#{verb := modify, path := [names, Identity], value := false}, _, {true, _}, State) ->
    %% close conns when we stop managing an identity
    close_clients(Identity, State);
motion_builtin(_Motion, _Mover, _Decision, State) ->
    State.

motion_decided(Motion, Mover, Decision, State) ->
    State1 = motion_builtin(Motion, Mover, Decision, State),
    callback(State, {motion_decided, 4}, [Motion, Mover, Decision, State1], State1).

callback(#{spec := Spec}, FA, Args) ->
    callback(Spec, FA, Args);
callback(#agent{mod=Mod}, FA, Args) ->
    loom:callback(Mod, FA, Args).

callback(#{spec := Spec}, FA, Args, Default) ->
    callback(Spec, FA, Args, Default);
callback(#agent{mod=Mod}, FA, Args, Default) ->
    loom:callback(Mod, FA, Args, Default).

%% helpers

catchup_client(Client, Identity, Since, State = #{point := Point}) ->
    %% XXX we should really batch these into limited size chunks
    %%     and wait for ack (or timeout and disconnect) on each chunk
    Missing =
        erloom_logs:fold(
          fun (Message, Locus, Acc) ->
                  case is_observable(Message, Identity, State) of
                      true ->
                          [{Message, Locus}|Acc];
                      false ->
                          Acc
                  end
          end, [], {Since, Point}, State),
    Client ! {catchup, lists:reverse(Missing), Point},
    State.

forward_clients(Message, State = #{locus := Locus}) ->
    maps:fold(
      fun (Client, Identity, S) ->
              case is_observable(Message, Identity, S) of
                  true ->
                      Client ! {forward, [{Message, Locus}]},
                      S;
                  false ->
                      S
              end
      end, State, util:get(State, clients, #{})).

close_clients(Identity, State) ->
    maps:fold(
      fun (Client, I, S) when I =:= Identity ->
              unlink(Client),
              Client ! {closed, Identity},
              util:remove(S, [clients, Identity]);
          (_, _, S) ->
              S
      end, State, util:get(State, clients, #{})).
