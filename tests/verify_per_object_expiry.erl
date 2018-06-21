-module(verify_per_object_expiry).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"Bucket">>).
-define(KEY(N), list_to_binary("Key-"++ integer_to_list(N))).
-define(VALUE(N), list_to_binary("Value-"++ integer_to_list(N))).
-define(REGNAME, riak_bitcask_test_verify_per_object_expiry).


confirm() ->
    register(?REGNAME, self()),
    Results =
        [
            test_expiration(),
            test_aae_exchange()
        ],
    _ = [begin io:format("Test name: ~p, Result: ~p ~n", [TestName, Result]), ?assertEqual(Result, pass) end ||
        {TestName, Result} <- lists:flatten(Results)],
    unregister(?REGNAME),
    pass.


test_expiration() ->
    {N, W, DW, R} = {3,3,3,3},
    Cluster = make_cluster({N, W, DW, R}),
    Results =
        [
            {test_none_expired_object ,test_not_expired_object(Cluster)},
            {test_expired_object, test_expired_object(Cluster)}
        ],
    rt:clean_cluster(Cluster),
    Results.

test_aae_exchange() ->
    {N, W, DW, R} = {3,1,1,1},
    Cluster = make_cluster({N, W, DW, R}),
    load_all_intercepts(Cluster),
    Results =
        [
            {test_aae_put_3_delete_2_not_expired, test_aae_put_3_delete_2_not_expired(Cluster)},
            {test_aae_put_3_delete_2_expired, test_aae_put_3_delete_2_expired(Cluster)},
            {test_aae_put_2_delete_2_expire_trigger_aae, test_aae_put_2_delete_2_expire_trigger_aae(Cluster)}
        ],

    rt:clean_cluster(Cluster),
    Results.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                         Expiration Tests                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_not_expired_object(Cluster) ->
    KeyNumber = 1,
    ok = put_object(Cluster, 1, KeyNumber),
    set_backend_reap_expiry(Cluster, 30),
    set_bit_cask_max_file_size(Cluster, 10240),
    ok = delete_object(Cluster, 2, KeyNumber),
    Dict = direct_kv_vnode_get_request(Cluster, KeyNumber),
    {ok, Tombstones} = dict:find(tombstone, Dict),
    ?assertEqual(length(Tombstones), 3),
    ?assertEqual({error, notfound}, get_object(Cluster, 1, KeyNumber)),
    pass.

test_expired_object(Cluster) ->
    KeyNumber = 2,
    ok = put_object(Cluster, 1, KeyNumber),
    set_backend_reap_expiry(Cluster, 1),
    set_bit_cask_max_file_size(Cluster, 10240),
    ok = delete_object(Cluster, 2, KeyNumber),
    timer:sleep(2000),
    Dict = direct_kv_vnode_get_request(Cluster, KeyNumber),
    {ok, NotFound} = dict:find(not_found, Dict),
    ?assertEqual(length(NotFound), 3),
    ?assertEqual({error, notfound}, get_object(Cluster, 1, KeyNumber)),
    pass.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                             AAE Tests                                                                %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_aae_put_3_delete_2_not_expired(Cluster) ->
    lager:info("test_aae_put_3_delete_2_not_expired"),
    KeyNumber = 3,
    set_backend_reap_expiry(Cluster, 500000),
    set_bit_cask_max_file_size(Cluster, 10240),
    ok = put_object(Cluster, 1, KeyNumber),
    timer:sleep(1000),
    add_preflist_intercept_to_all_nodes(Cluster),
    timer:sleep(1000),
    ok = delete_object(Cluster, 1, KeyNumber),
    timer:sleep(1000),

    Dict = direct_kv_vnode_get_request(Cluster, KeyNumber),
    lager:info("direct kv vnode get request"),
    print_dict_to_list(dict:to_list(Dict)),

    restore_preflist_intercept_to_all_nodes(Cluster),
    timer:sleep(7000),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(Cluster)),
    aae_manual_exchange(Cluster, KeyNumber),
    timer:sleep(3000),

    EqualityFun =
        fun(Dict) ->
            {ok, Result} = dict:find(tombstone, Dict),
            Bool = length(Result) == 3,
            {Result, Bool}
        end,

    Tombstone = direct_kv_vnode_get_loop(false, [], {Cluster, KeyNumber, EqualityFun}),
    reset_aae_envs(Cluster),
    ?assertEqual(length(Tombstone), 3),
    timer:sleep(3000),
    ?assertEqual({error, notfound}, get_object(Cluster, 1, KeyNumber)),
    pass.

test_aae_put_3_delete_2_expired(Cluster) ->
    lager:info("test_aae_put_3_delete_2_expired"),
    KeyNumber = 4,
    set_backend_reap_expiry(Cluster, 25),
    set_bit_cask_max_file_size(Cluster, 10240),
    ok = put_object(Cluster, 1, KeyNumber),
    timer:sleep(1000),
    add_preflist_intercept_to_all_nodes(Cluster),
    timer:sleep(1000),
    ok = delete_object(Cluster, 1, KeyNumber),
    timer:sleep(1000),

    Dict = direct_kv_vnode_get_request(Cluster, KeyNumber),
    lager:info("direct kv vnode get request"),
    print_dict_to_list(dict:to_list(Dict)),

    restore_preflist_intercept_to_all_nodes(Cluster),
    timer:sleep(7000),
    aae_manual_exchange(Cluster, KeyNumber),
    timer:sleep(3000),

    EqualityFun =
        fun(Dict) ->
            {ok, Result} = dict:find(not_found, Dict),
            Bool = length(Result) == 3,
            {Result, Bool}
        end,

    NotFound = direct_kv_vnode_get_loop(false, [], {Cluster, KeyNumber, EqualityFun}),
    reset_aae_envs(Cluster),
    ?assertEqual(length(NotFound), 3),
    timer:sleep(3000),
    ?assertEqual({error, notfound}, get_object(Cluster, 1, KeyNumber)),
    pass.

test_aae_put_2_delete_2_expire_trigger_aae(Cluster) ->
    lager:info("test_aae_put_2_delete_2_expire_trigger_aae"),
    KeyNumber = 5,
    set_backend_reap_expiry(Cluster, 10),
    set_bit_cask_max_file_size(Cluster, 10240),

    add_preflist_intercept_to_all_nodes(Cluster),
    timer:sleep(1000),
    ok = put_object(Cluster, 1, KeyNumber),
    timer:sleep(1000),
    ok = delete_object(Cluster, 1, KeyNumber),
    timer:sleep(30000),
    restore_preflist_intercept_to_all_nodes(Cluster),
    timer:sleep(7000),

    Dict = direct_kv_vnode_get_request(Cluster, KeyNumber),
    lager:info("direct kv vnode get request"),
    print_dict_to_list(dict:to_list(Dict)),

    aae_manual_exchange(Cluster, KeyNumber),
    timer:sleep(3000),

    EqualityFun =
        fun(Dict) ->
            {ok, Result} = dict:find(not_found, Dict),
            Bool = length(Result) == 3,
            {Result, Bool}
        end,

    NotFound = direct_kv_vnode_get_loop(false, [], {Cluster, KeyNumber, EqualityFun}),
    reset_aae_envs(Cluster),
    ?assertEqual(length(NotFound), 3),
    timer:sleep(3000),
    ?assertEqual({error, notfound}, get_object(Cluster, 1, KeyNumber)),
    pass.




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
direct_kv_vnode_get_loop(false, [], {Cluster, KeyNumber, EquailityFun}) ->
    Dict = direct_kv_vnode_get_request(Cluster, KeyNumber),
    lager:info("direct kv vnode get request"),
    print_dict_to_list(dict:to_list(Dict)),
    {Result, Bool} = EquailityFun(Dict),
    timer:sleep(1000),
    case Bool of
        true ->
            direct_kv_vnode_get_loop(Bool, Result, {Cluster, KeyNumber, EquailityFun});
        false ->
            direct_kv_vnode_get_loop(Bool, [], {Cluster, KeyNumber, EquailityFun})
    end;
direct_kv_vnode_get_loop(true, List, {_Cluster, _KeyNumber, _EquualityFun}) ->
    List.


print_dict_to_list([]) ->
    ok;
print_dict_to_list([{Key, ValueList} | Rest]) ->
    lager:info("Key: ~p", [Key]),
    print_value_list(ValueList, 0),
    print_dict_to_list(Rest).

print_value_list([], _) ->
    ok;
print_value_list([V | Rest], Counter) ->
    lager:info("Value~p: ~p", [Counter, V]),
    print_value_list(Rest, Counter+1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                         Helper Fucntions                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

make_cluster({N, W, DW, R}) ->
    lager:info("Deploy 8 nodes"),
    Conf = [
        {riak_core,
            [
                {ring_creation_size, 64},
                {default_bucket_props, [{n_val, N}, {w,W}, {dw,DW}, {r,R}, {allow_mult, false}]}
            ]
        }],
    Nodes = rt:deploy_nodes(8, Conf, [riak_kv]),
    lager:info("Build cluster A"),
    repl_util:make_cluster(Nodes),
    Nodes.



% ok
put_object(Cluster, NodeNumber, ObjectNumber) ->
    Node = lists:nth(NodeNumber, Cluster),
    {ok, C} = riak:client_connect(Node),
    Object = riak_object:new(?BUCKET, ?KEY(ObjectNumber), ?VALUE(ObjectNumber)),
    C:put(Object).

% ok
delete_object(Cluster, NodeNumber, ObjectNumber) ->
    Node = lists:nth(NodeNumber, Cluster),
    {ok, C} = riak:client_connect(Node),
    C:delete(?BUCKET, ?KEY(ObjectNumber)).

%% {ok, Obj} OR {error, notfound}
get_object(Cluster, NodeNumber, ObjectNumber) ->
    Node = lists:nth(NodeNumber, Cluster),
    {ok, C} = riak:client_connect(Node),
    C:get(?BUCKET, ?KEY(ObjectNumber)).

bucket_key_hash(Node, KeyN) ->
    rpc:call(Node, riak_core_util, chash_key, [{?BUCKET, ?KEY(KeyN)}]).

set_backend_reap_expiry(Cluster, Seconds) ->
    [rpc:call(Node, application, set_env, [riak_kv, delete_mode, {backend_reap, Seconds}]) || Node <- Cluster],
    ok.

set_bit_cask_max_file_size(Cluster, SizeInBytes) ->
    [rpc:call(Node, application, set_env, [bitcask, max_file_size, SizeInBytes]) || Node <- Cluster],
    ok.

direct_kv_vnode_get_request(Cluster, KeyN) ->
    Preflist = get_preflist(Cluster, KeyN, 3),
    Indexes = [Index || {Index, _} <- Preflist],
    BKey = {?BUCKET, ?KEY(KeyN)},
    ReqId = erlang:phash({os:timestamp(), BKey}, 1000000000),
    Ref = make_ref(),
    Sender = {raw, Ref, {?REGNAME, node()}},
    riak_kv_vnode:get(Preflist, BKey, ReqId, Sender),
    Dict0 = dict:from_list([{ok_object, []}, {tombstone, []}, {not_found, []}]),
    {Dict1, NoResposneIndexes} = loop(Dict0, {Ref, ReqId, Indexes}),
    ?assertEqual([], NoResposneIndexes),
    Dict1.


loop(Dict, {Ref, ReqId, Indexes}) ->
    receive
        {Ref, {r, {ok, Obj}, Idx, ReqId}} ->
            case dict:find(<<"X-Riak-Deleted">>, riak_object:get_metadata(Obj)) of
                {ok, "true"} ->
                    loop(dict:append(tombstone, {Idx, Obj}, Dict), {Ref, ReqId, lists:delete(Idx, Indexes)});
                _  ->
                    loop(dict:append(ok_object, {Idx, Obj}, Dict), {Ref, ReqId, lists:delete(Idx, Indexes)})
            end;
        {Ref, {r, {error, Obj}, Idx, ReqId}} ->
            loop(dict:append(not_found, {Idx, Obj}, Dict), {Ref, ReqId, lists:delete(Idx, Indexes)})
    after 1000 ->
        {Dict, Indexes}
    end.


load_all_intercepts(Cluster) ->
    lager:info("loading all intercepts to all nodes"),
    LoadFun = fun(Node) -> rt_intercept:load_code(Node) end,
    [LoadFun(Node) || Node <- Cluster].

add_preflist_intercept_to_all_nodes(Cluster) ->
    lager:info("adding prelist intercept to all nodes"),
    InterceptFun =
        fun(Node) ->
            rt_intercept:add(Node, {riak_core_apl, [{{get_primary_apl, 3}, return_cut_primary_apl}]}),
            rt_intercept:add(Node, {riak_core_apl, [{{get_apl_ann, 3}, return_cut_apl_ann}]})
        end,
    [InterceptFun(Node) || Node <- Cluster].

restore_preflist_intercept_to_all_nodes(Cluster) ->
    lager:info("restoring prelist intercept back to normal on all nodes"),
    InterceptFun =
        fun(Node) ->
            rt_intercept:add(Node, {riak_core_apl, [{{get_primary_apl, 3}, get_primary_apl}]}),
            rt_intercept:add(Node, {riak_core_apl, [{{get_apl_ann, 3}, get_apl_ann}]})
        end,
    [InterceptFun(Node) || Node <- Cluster].

get_preflist(Cluster, KeyN, N) ->
    Node = hd(Cluster),
    Hash = bucket_key_hash(Node, KeyN),
    [{PrefIndex, PrefNode} || {{PrefIndex, PrefNode}, _} <- rpc:call(Node, riak_core_apl, get_primary_apl, [Hash, N, riak_kv])].


aae_manual_exchange(Cluster, KeyN) ->
    ExchangeFun =
        fun(Node, Index) ->
            ok = rpc:call(Node, application, set_env, [riak_kv, anti_entropy_tick, 1000]),
            ok = rpc:call(Node, riak_kv_entropy_manager, set_mode, [manual]),
            _ = rpc:call(Node, riak_kv_entropy_manager, cancel_exchanges, []),
            ok = rpc:call(Node, riak_kv_entropy_manager, manual_exchange, [Index]),
            lager:info("manual exchange started on node: ~p at index: ~p", [Node, Index])
        end,
    Preflist = get_preflist(Cluster, KeyN, 3),
    lager:info("Preflist: ~p", [Preflist]),
    [ ExchangeFun(Node, Index) || {Index, Node} <- Preflist].

reset_aae_envs(Cluster) ->
    ResetFun =
        fun(Node) ->
            ok = rpc:call(Node, application, set_env, [riak_kv, anti_entropy_tick, 15000]),
            ok = rpc:call(Node, riak_kv_entropy_manager, set_mode, [automatic])
        end,
    [ResetFun(Node) || Node <- Cluster].