-module(agent).
-include("agency.hrl").

%% agency
-export([spec/2]).

%% loom
-behavior(loom).
-export([home/1,
         write_through/3,
         handle_message/4,
         motion_decided/4]).

-export([callback/3,
         callback/4]).

-callback home(#agent{}) -> loom:home().

%% agency

spec(Mod, Account) ->
    #agent{mod=Mod, account=Account}.

%% loom

home(Spec) ->
    callback(Spec, {home, 1}, [Spec]).

write_through(Message, N, State) ->
    callback(State, {write_through, 3}, [Message, N, State], {1, infinity}).

handle_builtin(#{type := conn, from := From}, Node, true, State) ->
    io:format("xxx ~p trying to connect to ~p~n", [From, Node]),
    State#{response => {ok, Node}};

handle_builtin(_Message, _Node, _IsNew, State) ->
    State.

handle_message(Message, Node, IsNew, State) ->
    State1 = handle_builtin(Message, Node, IsNew, State),
    Respond = fun () -> loom:maybe_reply(State1) end,
    callback(State, {handle_message, 4}, [Message, Node, IsNew, State1], Respond).

motion_decided(Motion, Mover, Decision, State) ->
    callback(State, {motion_decided, 4}, [Motion, Mover, Decision, State], State).

callback(#{spec := Spec}, FA, Args) ->
    callback(Spec, FA, Args);
callback(#agent{mod=Mod}, FA, Args) ->
    loom:callback(Mod, FA, Args).

callback(#{spec := Spec}, FA, Args, Default) ->
    callback(Spec, FA, Args, Default);
callback(#agent{mod=Mod}, FA, Args, Default) ->
    loom:callback(Mod, FA, Args, Default).
