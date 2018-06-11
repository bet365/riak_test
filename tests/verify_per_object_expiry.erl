-module(verify_per_object_expiry).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"Bucket">>).
-define(KEY(N), list_to_binary("Key-"++ integer_to_list(N))).
-define(VALUE(N), list_to_binary("Value-"++ integer_to_list(N))).


confirm() ->

    Results =
        [
            test_expiration(),
            test_aae_exchange()
        ],
    _ = [begin io:format("Test name: ~p, Result: ~p", [TestName, Result]), ?assertEqual(Result, pass) end ||
        {TestName, Result} <- lists:flatten(Results)],
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

test_aae_exchange() ->
    {N, W, DW, R} = {3,1,1,3},
    Cluster = make_cluster({N, W, DW, R}),

    Results =
        [

        ],

    rt:clean_cluster(Cluster),
    Results.



test_none_expired_object(Cluster) ->
    ok = put_object(Cluster, 1, 1),
    application:set_env(riak_kv, delete_mode, {backend_reap, 20}),
    ok = delete_object(Cluster, 2, 1),
    timer:sleep(5000),

    Index = responsible_index(1),
    ct:pal("responseible index works: ~p", [Index]),

    bitcask_get_all_nodes(Cluster, 1),

    % should get tombstone back?
    {error, notfound} = get_object(Cluster, 1, 1).






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
        },
        {riak_repl,
            [
                {fullsync_strategy, keylist},
                {fullsync_on_connect, false},
                {fullsync_interval, disabled},
                {max_fssource_retries, infinity},
                {max_fssource_cluster, 20},
                {max_fssource_node, 20},
                {max_fssink_node, 20}
            ]}],
    Nodes = rt:deploy_nodes(8, Conf, [riak_kv, riak_repl]),
    CheckSize = 8,
    ?assertEqual(true, CheckSize),
    lager:info("Build cluster A"),
    repl_util:make_cluster(Nodes),
    rt:wait_for_service(Nodes, riak_kv),
    rt:wait_for_service(Nodes, riak_pipe),
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

bucket_key_hash(KeyN) ->
    riak_core_util:chash_key({?BUCKET, ?KEY(KeyN)}).

responsible_index(KeyN) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:responsible_index(bucket_key_hash(KeyN), Ring).

%%set_bit_cask_max_file_size(SizeInBytes) ->
%%    application:set_env(bitcask, max_file_size, SizeInBytes).
%%
%%set_backend_reap_expiry(TimeInSeconds) ->
%%    application:set_env(riak_kv, delete_mode, {backend_reap, TimeInSeconds}).

bitcask_get_all_nodes(Cluster, KeyN) ->
    Fun =
        fun(Node, Index) ->
            {ok, State0} = rpc:call(Node, riak_kv_bitcask_backend, start, [Index, []]),
            {Response, Obj, _State1} = rpc:call(Node, riak_kv_bitcask_backend, get, [?BUCKET, ?KEY(KeyN), State0]),
            {Response, Obj}
        end,
    [{Node1, Response1, Obj1}|| {Node1, {Response1, Obj1}} <- [{Node, Fun(Node, responsible_index(KeyN))} || Node <- Cluster], Response1 == ok].






