-module(agent).
-include("agency.hrl").

-export_type([state/0,
              conn/0]).

-type state() :: #{ %% + loom:state()
             roles => [atom()],
             tokens => #{},
             clients => #{}
            }.

-type conn() :: #{ %% + loom:message()
            type => conn,
            from => pid(),
            as => manager:sub_name(),
            since => {binary(), erloom:edge()},
            token => binary()
}.

-callback spec(#manager{}, manager:sub_id()) -> #agent{}.
-callback home(#agent{}) -> loom:home().
-callback client_filter(loom:message(), manager:sub_name(), loom:state()) ->
    loom:message() | undefined.

-optional_callbacks([client_filter/3]).

%% agent
-export([agency/1,
         id/1,
         which/1,
         connect/2,
         connect/3,
         patch/2,
         patch/3]).

%% agent mod helpers
-export([is_valid_token/2,
         create_token/3,
         forget_token/2,
         purge_tokens/1]).

%% loom
-behavior(loom).
-export([vsn/1,
         find/2,
         home/1,
         opts/1,
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

%% agent

agency(#agent{agency=Agency}) ->
    Agency.

id(#{spec := Spec}) ->
    id(Spec);
id(#agent{id=AgentId}) ->
    AgentId.

which(Specish = {#manager{}, _}) ->
    Specish;
which(Spec = #agent{mod=AgentMod, id=AgentId}) ->
    %% convert a spec to something the manager can handle
    {agency(Spec), {id, AgentMod, AgentId}}.

connect(Specish, As) ->
    connect(Specish, As, #{}).

connect(Specish, As, Ctx) ->
    %% if the name actually maps to a different id than we thought (cached), its ok
    %% we can still connect to the old id:
    %%  when the agents for the new id take over, they must notify the old agents
    %%  eventually the old id will have a message indicating it was unlinked
    %%  when the agent is in an unlinked state, it refuses / closes client connections
    %%  when the client sees this, it forgets cached info, and tries to reconnect
    Message =
        util:update(#{
                       type => conn,
                       from => self(),
                       as => As,
                       at => time:unix()
                     }, util:select(Ctx, [since, token])),
    patch(Specish, Message, Ctx).

patch(Specish, Message) ->
    patch(Specish, Message, #{}).

patch(Specish, Message, Ctx) ->
    loom:patch(Specish, Message, Ctx).

client_filter(#{mute := true}, _, _) ->
    undefined;
client_filter(Message, For, State) ->
    Default =
        case Message of
            #{type := client} ->
                Message;
            _ ->
                undefined
        end,
    callback(State, {client_filter, 3}, [Message, For, State], Default).

%% loom

vsn(Spec) ->
    maps:merge(callback(Spec, {vsn, 1}, [Spec], #{}), #{}).

find(Specish, Ctx) ->
    manager:find_cache(which(Specish), get_or_create, agent, Ctx).

home(Spec) ->
    callback(Spec, {home, 1}, [Spec]).

opts(Spec) ->
    Defaults = #{idle_timeout => time:timeout({5, minutes})},
    maps:merge(Defaults, callback(Spec, {opts, 1}, [Spec], #{})).

keep(State) ->
    Builtins = maps:with([roles, tokens], State),
    maps:merge(Builtins, callback(State, {keep, 1}, [State], #{})).

init(State) ->
    State#{
      roles => util:get(State, roles, []),
      tokens => util:get(State, tokens, #{})
     }.

waken(State) ->
    State1 = purge_tokens(State),
    callback(State1, {waken, 1}, [State1], State1).

verify_message(Message = #{type := conn, token := Token}, State) ->
    case is_valid_token(State, Token) of
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

handle_idle(State) ->
    %% default is not to sleep as long as any clients are connected
    Default =
        fun () ->
                case util:get(State, clients) of
                    C when map_size(C) > 0 ->
                        loom:save(State);
                    _ ->
                        loom:sleep(State)
                end
        end,
    callback(State, {handle_idle, 1}, [State], Default).

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

handle_builtin(#{type := conn, from := From, as := Name} = Message, _, true, State) ->
    case erloom_chain:value(State, [names, Name]) of
        true ->
            link(From),
            State1 = util:modify(State, [clients, From], Name),
            State2 = loom:maybe_reply(ok, State1),
            catchup_client(From, Name, util:get(Message, since), State2);
        _ ->
            %% we refuse conns for names we dont manage
            State#{response => {error, {name, Name}}}
    end;

handle_builtin(_Message, _Node, _IsNew, State) ->
    State.

handle_message(Message, Node, IsNew, State) ->
    State1 = handle_builtin(Message, Node, IsNew, State),
    State2 = forward_clients(Message, State1),
    callback(State2, {handle_message, 4}, [Message, Node, IsNew, State2], State2).

command_called(#{verb := modify, path := [names, Name], value := false}, _, true, State) ->
    %% close conns when we stop managing a name
    close_clients(Name, State);
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
callback(#agent{mod=Mod}, FA, Args) ->
    loom:callback(Mod, FA, Args).

callback(#{spec := Spec}, FA, Args, Default) ->
    callback(Spec, FA, Args, Default);
callback(#agent{mod=Mod}, FA, Args, Default) ->
    loom:callback(Mod, FA, Args, Default).

%% token management
%%
%% tokens are just secrets kept by the agent and associated with a value
%% tokens can expire, and they can be used to ensure a client is allowed to connect
%% agent modules may use tokens for shared secrets in other types of messages

is_valid_token(State, Token) ->
    Now = time:unix(),
    case util:lookup(State, [tokens, Token]) of
        undefined ->
            false;
        #{expiration := Exp} when Exp < Now ->
            false;
        #{} ->
            true
    end.

create_token(State, Token, Value = #{}) ->
    util:modify(State, [tokens, Token], Value).

forget_token(State, Token) ->
    util:remove(State, [tokens, Token]).

purge_tokens(State = #{tokens := Tokens}) ->
    Now = time:unix(),
    Tokens1 =
        util:fold(fun ({_, #{expiration := Exp}}, Acc) when Exp < Now ->
                          Acc;
                      ({T, V}, Acc) ->
                          util:set(Acc, T, V)
                  end, #{}, Tokens),
    State#{tokens => Tokens1}.

%% client helpers

catchup_client(Client, Name, Since, State = #{point := Point}) ->
    %% XXX we should really batch these into limited size chunks
    %%     and wait for ack (or timeout and disconnect) on each chunk
    Missing =
        erloom_logs:fold(
          fun (Message, Locus, Acc) ->
                  case client_filter(Message, Name, State) of
                      undefined ->
                          Acc;
                      Msg ->
                          [{Msg, Locus}|Acc]
                  end
          end, [], {Since, Point}, State),
    Client ! {catchup, lists:reverse(Missing)},
    State.

forward_clients(Message, State = #{locus := Locus}) ->
    maps:fold(
      fun (Client, Name, S) ->
              case client_filter(Message, Name, S) of
                  undefined ->
                      S;
                  Msg ->
                      Client ! {forward, [{Msg, Locus}]},
                      S
              end
      end, State, util:get(State, clients, #{})).

close_clients(Name, State) ->
    maps:fold(
      fun (Client, N, S) when N =:= Name ->
              unlink(Client),
              Client ! {closed, Name},
              util:remove(S, [clients, Name]);
          (_, _, S) ->
              S
      end, State, util:get(State, clients, #{})).
