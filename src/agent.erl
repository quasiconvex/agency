-module(agent).
-include("agency.hrl").

%% agency
-export([spec/2,
         agency/1]).

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

-export_type([state/0,
              conn/0]).

-type state() :: #{ %% + loom:state()
             roles => [atom()],
             clients => #{}
            }.

-type conn() :: #{ %% + loom:message()
            type => conn,
            from => pid(),
            as => binary(),
            since => {binary(), erloom:edge()}
}.

-callback agency(#agent{}) -> loom:spec().
-callback home(#agent{}) -> loom:home().

%% agency

spec(Mod, Account) ->
    #agent{mod=Mod, account=Account}.

agency(Spec) ->
    callback(Spec, {agency, 1}, [Spec]).

%% loom

find(Spec = #agent{account=Account}) ->
    case agency:obtain_account(agency(Spec), Account) of
        {ok, Nodes} ->
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
    Builtins = maps:with([identities, roles], State),
    maps:merge(Builtins, callback(State, {keep, 1}, [State], #{})).

init(State) ->
    State#{
      identities => util:get(State, identities, []),
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
    %% XXX refuse conns for identities we dont manage, close conns when we lose an identity
    link(From),
    State1 = util:modify(State, [clients, From], Identity),
    State2 = catchup_client(From, util:get(State1, since), State1),
    State2#{response => ok};

handle_builtin(_Message, _Node, _IsNew, State) ->
    State.

handle_message(Message, Node, IsNew, State) ->
    State1 = handle_builtin(Message, Node, IsNew, State),
    State2 = forward_clients(Message, State1),
    callback(State2, {handle_message, 4}, [Message, Node, IsNew, State2], State2).

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

catchup_client(Client, Since, State = #{point := Point}) ->
    erloom_logs:fold(
      fun (Message, Locus, S) ->
              %% XXX is this a message we want to send?
              Client ! {catchup, Message, Locus},
              S
      end, State, {Since, Point}, State).

forward_clients(Message, State = #{locus := Locus}) ->
    maps:fold(
      fun (Client, _Identity, S) ->
              %% XXX is this a message we want to send?
              Client ! {forward, Message, Locus},
              S
      end, State, util:get(State, clients, #{})).
