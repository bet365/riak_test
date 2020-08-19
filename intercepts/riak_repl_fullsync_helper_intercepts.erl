-module(riak_repl_fullsync_helper_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_repl_fullsync_helper_orig).

-record(diff_state, {
    fsm,
    ref,
    preflist,
    count = 0,
    replies = 0,
    diff_hash = 0,
    missing = 0,
    need_vclocks = true,
    errors = [],
    filter_buckets = false,
    shared_buckets = []}).


diff_keys_intercept(R, L, DS) ->
    DiffState = ?M:diff_keys_orig(R, L, DS),
    {ok, C} = riak:local_client(),
    C:put(riak_object:new(<<"diffstate">>, term_to_binary(DiffState#diff_state.ref), term_to_binary(DiffState))),
    DiffState.




