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
            test_expiration()
%%            test_aae_exchange()
        ],
    _ = [begin io:format("Test name: ~p, Result: ~p", [TestName, Result]), ?assertEqual(Result, pass) end ||
        {TestName, Result} <- lists:flatten(Results)],

    unregister(?REGNAME),
    pass.


test_expiration() ->
    {N, W, DW, R} = {3,3,3,3},
    Cluster = make_cluster({N, W, DW, R}),

    Results =
        [
            {test_none_expired_object ,test_none_expired_object(Cluster)}
        ],

    rt:clean_cluster(Cluster),
    Results.

%%test_aae_exchange() ->
%%    {N, W, DW, R} = {3,1,1,3},
%%    Cluster = make_cluster({N, W, DW, R}),
%%
%%    Results =
%%        [
%%
%%        ],
%%
%%    rt:clean_cluster(Cluster),
%%    Results.



test_none_expired_object(Cluster) ->
    ok = put_object(Cluster, 1, 1),
    ok = put_object(Cluster, 1, 2),
    set_backend_reap_expiry(Cluster, 500000),
    set_bit_cask_max_file_size(Cluster, 10240),
    ok = delete_object(Cluster, 2, 2),

    Index = responsible_index(hd(Cluster), 1),
    lager:info("responseible index works: ~p", [Index]),

    direct_kv_vnode_get_request(Cluster, 1),
    direct_kv_vnode_get_request(Cluster, 2),
    ?assertEqual(true, false).


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
%%get_object(Cluster, NodeNumber, ObjectNumber) ->
%%    Node = lists:nth(NodeNumber, Cluster),
%%    {ok, C} = riak:client_connect(Node),
%%    C:get(?BUCKET, ?KEY(ObjectNumber)).

bucket_key_hash(Node, KeyN) ->
    rpc:call(Node, riak_core_util, chash_key, [{?BUCKET, ?KEY(KeyN)}]).

responsible_index(Node, KeyN) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    rpc:call(Node, riak_core_ring, responsible_index, [bucket_key_hash(Node, KeyN), Ring]).

set_backend_reap_expiry(Cluster, Seconds) ->
    [rpc:call(Node, application, set_env, [riak_kv, delete_mode, {backend_reap, Seconds}]) || Node <- Cluster],
    ok.

set_bit_cask_max_file_size(Cluster, SizeInBytes) ->
    [rpc:call(Node, application, set_env, [bitcask, max_file_size, SizeInBytes]) || Node <- Cluster],
    ok.

direct_kv_vnode_get_request(Cluster, KeyN) ->
    Node = hd(Cluster),
    Hash = bucket_key_hash(Node, KeyN),
    Preflist = rpc:call(Node, riak_core_apl, get_apl, [Hash, 3, riak_kv]),
    BKey = {?BUCKET, ?KEY(KeyN)},
    ReqId = erlang:phash({os:timestamp(), BKey}, 1000000000),
    Ref = make_ref(),
    Sender = {raw, Ref, {?REGNAME, node()}},
    riak_kv_vnode:get(Preflist, BKey, ReqId, Sender),
    Dict = dict:from_list([{ok_object, []}, {tombstone, []}, {not_found, []}]),

    lager:info("Index: ~p", [responsible_index(Node, KeyN)]),
    lager:info("preflist: ~p", [{Preflist}]),

    loop(Dict, {Ref, ReqId}).

loop(Dict, Data = {Ref, ReqId}) ->
    receive
        {Ref, {r, {ok, Obj}, Idx, ReqId}} ->
            lager:info("object: ~p", [Obj]),
            loop(dict:append(ok_object, Idx, Dict), Data);
        {Ref, {r, {error, Obj}, Idx, ReqId}} ->
            lager:info("object: ~p", [Obj]),
            loop(dict:append(not_found, Idx, Dict), Data)
    after 3000 ->
        Dict
    end.