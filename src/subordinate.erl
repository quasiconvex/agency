-module(subordinate).
-include("agency.hrl").

%% the manager just needs a way to get spec from manager + id
-callback spec(#manager{}, manager:sub_id()) -> loom:spec().
