%%%-------------------------------------------------------------------
%%% @author dylanmitelo
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. Jul 2019 16:08
%%%-------------------------------------------------------------------
-module(split_backend_test).
-author("dylanmitelo").

%% API
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(ALL_BUCKETS_NUMS, 	["1", "2", "3"]).
-define(ALL_KEY_NUMS,		["1", "2", "3"]).




%% TODO This module can be deleted as work has changed and become multi_bitcask_instances_test.erl
confirm() ->
	[_C1, C2] = C = make_clusters_helper(),
%%	[?assertEqual(pass, test(N, C1)) || N <- lists:seq(6, 6)],
	?assertEqual(pass, test(6, C2)),
	destroy_clusters(C),
	pass.

%% Checks that only the default split is created and no others.
test(1, C1) ->
	lager:info("Starting test(1)"),
	Backends = [<<"bucket-1">>],

	{CoreVnodeStates, BackendLists, UBackends} = fetch_backend_data(C1),

	?assertEqual(length(CoreVnodeStates), length(BackendLists)), %% Checks there is only one backend per partition
	?assertEqual(UBackends, Backends),

	cleanup([C1], ["cluster1"]),

	pass;

%% Tests that when adding a new split backend it is started across each partition.
test(2, C1) ->
	lager:info("Starting test(2)"),
	Backends = [<<"bucket-1">>],
	Expected = lists:flatten([Backends, <<"bucket-2">>]),

	{CoreVnodeStates, BackendLists, UBackends} = fetch_backend_data(C1),

	?assertEqual(length(CoreVnodeStates), length(BackendLists)), %% Checks there is only one backend per partition
	?assertEqual(UBackends, Backends),

	rpc:call(hd(C1), riak_kv_console, add_split_backend, [[bitcask, 'bucket-2']]),
	timer:sleep(2000),
	{NewCoreVnodeStates, NewBackendList, NewUBackends} = fetch_backend_data(C1),

	lager:info("New corestate length: ~p BackendList length: ~p and UBackends: ~p~n", [length(NewCoreVnodeStates), length(NewBackendList), NewUBackends]),
	?assertEqual(2*length(NewCoreVnodeStates), length(NewBackendList)), %% Checks there is only one backend per partition
	?assertEqual(NewUBackends, Expected),

	Splits = rpc:call(hd(C1), riak_core_metadata, get, [{split_backend, bitcask}, splits]),

	lager:info("metadata: ~p~n", [Splits]),

	cleanup([C1], ["cluster1"]),

	pass;

%% Same as above but we kill a node then start back up to ensure the split is picked up upon starting the node up.
test(3, C1) ->
	lager:info("Starting test(2)"),
	Backends = [<<"bucket-1">>],
	Expected = lists:flatten([Backends, <<"bucket-2">>]),

	{CoreVnodeStates, BackendLists, UBackends} = fetch_backend_data(C1),

	?assertEqual(64, length(CoreVnodeStates)), %% Checks there is only one backend per partition, total 64
	?assertEqual(length(CoreVnodeStates), length(BackendLists)), %% Should be one backend per partition
	?assertEqual(UBackends, Backends),

	rt:stop_and_wait(hd(C1)),

	rpc:call(lists:nth(2, C1), riak_kv_console, add_split_backend, [[bitcask, 'bucket-2']]),
	timer:sleep(2000),
	{NewCoreVnodeStates, NewBackendList, NewUBackends} = fetch_backend_data(C1),

	lager:info("New corestate length: ~p BackendList length: ~p and UBackends: ~p~n", [length(NewCoreVnodeStates), length(NewBackendList), NewUBackends]),
	lager:info("Orignal corestate length: ~p Original BackendList length: ~p", [length(CoreVnodeStates), length(BackendLists)]),
	?assertEqual(56, length(NewCoreVnodeStates)), %% Node killed, should be 64 - 8 partitions
	?assertEqual(2*length(NewCoreVnodeStates), length(NewBackendList)), %% Should be 2 backends per partition, 56 parts 112 backends
	?assertEqual(NewUBackends, Expected),

	rt:start_and_wait(hd(C1)),
	rt:wait_for_service(hd(C1), [riak_kv]),
	rt:wait_until_transfers_complete([hd(C1)]),

	{NewCoreVnodeStates1, NewBackendList1, NewUBackends1} = fetch_backend_data(C1),
	lager:info("New corestate length: ~p BackendList length: ~p and UBackends: ~p~n", [length(NewCoreVnodeStates1), length(NewBackendList1), NewUBackends1]),
	?assertEqual(64, length(CoreVnodeStates)), %% Node brought back so should be back to 64 total partitions
	?assertEqual(2*length(NewCoreVnodeStates1), length(NewBackendList1)), %% Should pick up new backend from MD, 64 parts 128 backends
	?assertEqual(NewUBackends1, Expected),

	Splits = rpc:call(hd(C1), riak_core_metadata, get, [{split_backend, bitcask}, splits]),

	lager:info("metadata: ~p~n", [Splits]),

	pass;

%% Tests that puts are rejected when the default bucket flag is false, then sets to true and ensures puts are accepted.
test(4, C1) ->
	_Backends = [<<"bucket-1">>],

	rpc:call(hd(C1), riak_core_metadata, put, [{split_backend, all}, use_default_backend, false]),
	DefFlag = rpc:call(hd(C1), riak_core_metadata, get, [{split_backend, all}, use_default_backend]),
	?assertEqual(false, DefFlag),

	Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
	put_all_objects(C1, 3),
	?assertEqual(false, check_objects("cluster1", C1, Expected1, erlang:now(), 240)),

	rpc:call(hd(C1), riak_core_metadata, put, [{split_backend, all}, use_default_backend, true]),
	put_all_objects(C1, 3),
	?assertEqual(true, check_objects("cluster1", C1, Expected1, erlang:now(), 240)),

	cleanup([C1], ["cluster1"]),

	pass;

%% Keeps default bucket flag set to false checks puts are rejected then adds the required backends and confirms puts are now accepted.
test(5, C1) ->
	_Backends = [<<"bucket-1">>],

	rpc:call(hd(C1), riak_core_metadata, put, [{split_backend, all}, use_default_backend, false]),
	DefFlag = rpc:call(hd(C1), riak_core_metadata, get, [{split_backend, all}, use_default_backend]),
	?assertEqual(false, DefFlag),

	Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
	put_all_objects(C1, 3),
	?assertEqual(false, check_objects("cluster1", C1, Expected1, erlang:now(), 240)),

	rpc:call(hd(C1), riak_kv_console, add_split_backend, [[bitcask, 'bucket-2']]),
	rpc:call(hd(C1), riak_kv_console, add_split_backend, [[bitcask, 'bucket-3']]),

	timer:sleep(2000),
	Splits = rpc:call(hd(C1), riak_core_metadata, get, [{split_backend, bitcask}, splits]),
	?assertEqual(['bucket-2', 'bucket-3'], lists:sort(Splits)),

	put_all_objects(C1, 3),
	?assertEqual(true, check_objects("cluster1", C1, Expected1, erlang:now(), 240)),

	cleanup([C1], ["cluster1"]),

	pass;

test(6, C2) ->
	NewConf = conf(),
	lager:info("Cluster2 current bitcask config of first node: ~p~n", [rpc:call(hd(C2), application, get_all_env, [riak_kv])]),
	lager:info("Cluster2 current split config of first node: ~p~n", [rpc:call(hd(C2), application, get_env, [riak_kv, split_backend])]),
  rt:stop_and_wait(hd(C2)),
	rt:update_app_config(hd(C2), NewConf),

	lager:info("Cluster2 updated bitcask config of first node: ~p~n", [rpc:call(hd(C2), application, get_env, [riak_kv, bitcask])]),
	lager:info("Cluster2 updated split config of first node: ~p~n", [rpc:call(hd(C2), application, get_env, [riak_kv, split_backend])]),
	lager:info("Path of where we reside: ~p~n", [os:cmd("pwd")]),
	rt:start_and_wait(hd(C2)),
	rt:wait_for_service(hd(C2), [riak_kv]),
	rt:wait_until_transfers_complete([hd(C2)]),

	ok.

conf() ->
	[
		{riak_repl,
			[
				%% turn off fullsync
				{delete_mode, 1},
				{fullsync_interval, 0},
				{fullsync_strategy, keylist},
				{fullsync_on_connect, false},
				{fullsync_interval, disabled},
				{max_fssource_node, 64},
				{max_fssink_node, 64},
				{max_fssource_cluster, 64},
				{default_bucket_props, [{n_val, 3}, {allow_mult, false}]},
				{override_capability,
					[{default_bucket_props_hash, [{use, [consistent, datatype, n_val, allow_mult, last_write_wins]}]}]}
			]},
		{riak_kv,
			[
				{storage_backend, riak_kv_split_backend},
				{split_backend_default, <<"bucket-1">>},
				{split_backend, [
					{<<"bucket-1">>, riak_kv_bitcask_backend, [
						{data_root, "./data/bitcask/bucket-1/"}
					]},
					{<<"bucket-2">>, riak_kv_bitcask_backend, [
						{data_root, "./data/bitcask/bucket-2/"}
					]},
					{<<"bucket-3">>, riak_kv_bitcask_backend, [
						{data_root, "./data/bitcask/bucket-3/"}
					]},
					{<<"bucket-4">>, riak_kv_bitcask_backend, [
						{data_root, "./data/bitcask/bucket-4/"}
					]}
				]}
			]},
		{bitcask,
			[
				{merge_window, never}
			]}
	].

%% =======================================================
%% Setup
%% =======================================================
%%print_test(Number, Config, ExpectedList) ->
%%	lager:info("---------------------------------------"),
%%	lager:info("---------------------------------------"),
%%	lager:info("---------------------------------------"),
%%	lager:info("Test: ~p ~n", [Number]),
%%	lager:info("Config: ~p ~n", [Config]),
%%	lager:info("Expected: ~p ~n", [lists:sort(ExpectedList)]),
%%	lager:info("---------------------------------------"),
%%	lager:info("---------------------------------------"),
%%	lager:info("---------------------------------------").

make_clusters_helper() ->
	Conf = [
		{riak_repl,
			[
				%% turn off fullsync
				{delete_mode, 1},
				{fullsync_interval, 0},
				{fullsync_strategy, keylist},
				{fullsync_on_connect, false},
				{fullsync_interval, disabled},
				{max_fssource_node, 64},
				{max_fssink_node, 64},
				{max_fssource_cluster, 64},
				{default_bucket_props, [{n_val, 3}, {allow_mult, false}]},
				{override_capability,
					[{default_bucket_props_hash, [{use, [consistent, datatype, n_val, allow_mult, last_write_wins]}]}]}
			]},
		{riak_kv,
			[

				{storage_backend, riak_kv_split_backend},
				{split_backend_default, <<"bucket-1">>},
				{split_backend, [
					{<<"bucket-1">>, riak_kv_bitcask_backend, [
						{data_root, "./data/bitcask/bucket-1/"}
					]}
%%					{<<"bucket-2">>, riak_kv_bitcask_backend, [
%%						{data_root, "./data/bitcask/bucket-2/"}
%%					]}
				]}
			]},
		{bitcask,
			[
				{merge_window, never}
			]}
	],

	Conf2 = [
		{riak_repl,
			[
				%% turn off fullsync
				{delete_mode, 1},
				{fullsync_interval, 0},
				{fullsync_strategy, keylist},
				{fullsync_on_connect, false},
				{fullsync_interval, disabled},
				{max_fssource_node, 64},
				{max_fssink_node, 64},
				{max_fssource_cluster, 64},
				{default_bucket_props, [{n_val, 3}, {allow_mult, false}]},
				{override_capability,
					[{default_bucket_props_hash, [{use, [consistent, datatype, n_val, allow_mult, last_write_wins]}]}]}
			]}
%%		{riak_kv,
%%			[
%%				{storage_backend, riak_kv_bitcask_backend}
%%			]}
%%		{bitcask,
%%			[
%%				{merge_window, never}
%%			]}
	],
	NodeConf = [{current, Conf} || _ <- lists:seq(1,4)],
	NodeConf2 = [{current, Conf2} || _ <- lists:seq(5,8)],
	lager:info("Conf1: ~p~n", [NodeConf]),
	lager:info("Conf2: ~p~n", [NodeConf2]),
	AllConf = lists:flatten([NodeConf | NodeConf2]),
	lager:info("AllConf: ~p~n", [AllConf]),
	Nodes = rt:deploy_nodes(AllConf, [riak_kv, riak_repl]),		%% TODO Perhaps half the number of nodes to 4 for a single cluster
	lager:info("Nodes: ~p~n", [Nodes]),
%%	Nodes = rt:deploy_nodes(NodeConf2, [riak_kv, riak_repl]),

	Cluster1 = lists:sublist(Nodes, 1, 4),
	Cluster2 = lists:sublist(Nodes, 5, 8),

	[rpc:call(N1, erlang, disconnect_node, [N2]) || N1 <- Cluster1, N2 <- Cluster2],
%%	[rpc:call(N1, erlang, disconnect_node, [N2]) || N1 <- Cluster2, N2 <- Cluster1 ++ Cluster3],
%%	[rpc:call(N1, erlang, disconnect_node, [N2]) || N1 <- Cluster3, N2 <- Cluster1 ++ Cluster2],

	lager:info("Build cluster 1"),
	repl_util:make_cluster(Cluster1),
	lager:info("Build cluster 2"),
	repl_util:make_cluster(Cluster2),
%%	lager:info("Build cluster 3"),
%%	repl_util:make_cluster(Cluster3),

	lager:info("waiting for leader to converge on cluster 1"),
	?assertEqual(ok, repl_util:wait_until_leader_converge(Cluster1)),
	lager:info("waiting for leader to converge on cluster 2"),
	?assertEqual(ok, repl_util:wait_until_leader_converge(Cluster2)),
%%	lager:info("waiting for leader to converge on cluster 2"),
%%	?assertEqual(ok, repl_util:wait_until_leader_converge(Cluster3)),

	repl_util:name_cluster(hd(Cluster1), "cluster1"),
	rt:wait_until_ring_converged(Cluster1),
	repl_util:name_cluster(hd(Cluster2), "cluster2"),
	rt:wait_until_ring_converged(Cluster2),
%%	repl_util:name_cluster(hd(Cluster3), "cluster3"),
%%	rt:wait_until_ring_converged(Cluster3),

	lager:info("Cluster2 current bitcask config of first node: ~p~n", [rpc:call(hd(Cluster2), application, get_all_env, [riak_kv])]),
	lager:info("Cluster1 current split config of first node: ~p~n", [rpc:call(hd(Cluster1), application, get_all_env, [riak_kv])]),

	rt:wait_until_transfers_complete(Cluster1),
	rt:wait_until_transfers_complete(Cluster2),
%%	rt:wait_until_transfers_complete(Cluster3),

	[Cluster1, Cluster2].

destroy_clusters(Clusters) ->
	Nodes = lists:flatten(Clusters),
	rt:clean_cluster(Nodes).

all_bkeys() ->
	[{B,K} || B <- ?ALL_BUCKETS_NUMS, K <- ?ALL_KEY_NUMS].

put_all_objects(Cluster, TestNum) ->
	Node = hd(Cluster),
	{ok, C} = riak:client_connect(Node),
	[C:put(Obj) || Obj <- create_objects(all, TestNum)],
	lager:info("Placed all data on Cluster: ~p~n", [Cluster]).
%%put_objects(Cluster, TestNum, BucketNum) ->
%%	Node = hd(Cluster),
%%	{ok, C} = riak:client_connect(Node),
%%	Puts = [C:put(Obj) || Obj <- create_objects_for_buckets(BucketNum, TestNum)],
%%	lager:info("Placed all data on Cluster: ~p with Responses: ~p~n", [Cluster, Puts]).

create_objects(all, TestNum) ->
	[create_single_object(BN, KN, TestNum) || BN <- ?ALL_BUCKETS_NUMS, KN <- ?ALL_KEY_NUMS].
%%create_objects_for_buckets(BNList, TestNum) ->
%%	[create_single_object(BN, KN, TestNum) || BN <- BNList, KN <- ?ALL_KEY_NUMS].

create_single_object(Bucket, Key, TestNum) ->
	riak_object:new(make_bucket(Bucket), make_key(Key), make_value(Bucket, Key, TestNum)).

make_bucket(N) -> list_to_binary("bucket-" ++ N).
make_key(N) -> list_to_binary("key-" ++ N).
make_value(BN, KN, TestNumber) -> list_to_binary("test-" ++ integer_to_list(TestNumber) ++ " ------ value-"
	++ BN ++ "-" ++ KN).

cleanup(Clusters, ClusterNames) ->
%%	clear_config(Clusters),
%%	check_object_filtering_config(Clusters),
	lager:info("Starting cleanup procedure"),
	delete_data(Clusters, ClusterNames),
	delete_files(),
	timer:sleep(2000),
	lager:info("Cleanup complete ~n", []),
	check(Clusters, ClusterNames),
	ok.

delete_files() ->
	file:delete("/tmp/config1"),
	file:delete("/tmp/config2").

delete_data([], []) -> ok;
delete_data([Cluster|Rest1], [ClusterName|Rest2]) ->
	delete_all_objects(ClusterName, Cluster),
	delete_data(Rest1, Rest2).

delete_all_objects(ClusterName, Cluster) ->
	Node = hd(Cluster),
	{ok, C} = riak:client_connect(Node),
	[C:delete(make_bucket(BN), make_key(KN)) || BN <- ?ALL_BUCKETS_NUMS, KN <- ?ALL_KEY_NUMS],
	lager:info("Deleted all data for ~p", [ClusterName]).

check([], []) -> ?assertEqual(true, true);
check(Clusters = [Cluster|Rest1], ClusterNames =[ClusterName|Rest2]) ->
	case check_objects(ClusterName, Cluster, [], erlang:now(), 10) of
		true -> check(Rest1, Rest2);
		false -> cleanup(Clusters, ClusterNames)
	end.

check_objects(ClusterName, Cluster, Expected, Time, Timeout) ->
	check_object_helper(ClusterName, Cluster, Expected, Time, Timeout).
%% TODO make this quicker for []!
check_object_helper(ClusterName, Cluster, Expected, Time, Timeout) ->
	Actual = get_all_objects(Cluster),
	Result = lists:sort(Actual) == lists:sort(Expected),
	case {Result, timer:now_diff(erlang:now(), Time) > Timeout*100} of
		{true, _} ->
			print_objects(ClusterName, Actual),
			true;
		{false, false} ->
			check_object_helper(ClusterName, Cluster, Expected, Time, Timeout);
		{false, true} ->
			print_objects(ClusterName, Actual),
			false
	end.

get_all_objects(Cluster) ->
	Node = hd(Cluster),
	{ok, C} = riak:client_connect(Node),
	lager:info("Node trying to connect to: ~p, CLient: ~p~n", [Node, C]),
	AllObjects = [get_single_object(C, BN, KN) || BN <-?ALL_BUCKETS_NUMS, KN <- ?ALL_KEY_NUMS],
	[Obj || Obj <- AllObjects, Obj /= notfound].

get_single_object(C, BN, KN) ->
	lager:info("BN: ~p and KN: ~p~n", [BN, KN]),
	case C:get(make_bucket(BN), make_key(KN)) of
		{error, notfound} ->
			notfound;
		{ok, Obj} ->
			{riak_object:bucket(Obj), riak_object:key(Obj)}
	end.

print_objects(ClusterName, Objects) ->
	lager:info("All objects for ~p ~n", [ClusterName]),
	print_objects_helper(Objects),
	lager:info("~n", []).
print_objects_helper([]) -> ok;
print_objects_helper([{Bucket, Key}|Rest]) ->
	lager:info("~p, ~p ~n", [Bucket, Key]),
	print_objects_helper(Rest).

%%list_bitcask_files(Nodes, Backends) ->
%%	[{Node, list_node_bitcask_files(Node, Backends)} || Node <- Nodes].
%%
%%list_node_bitcask_files(Node, Backends) ->
%%	% Gather partitions owned, list *.bitcask.data on each.
%%	Partitions = rt:partitions_for_node(Node),
%%	lager:info("Node: ~p, Partitions: ~p~n", [Node, Partitions]),
%%	{ok, DataDir} = rt:rpc_get_env(Node, [{bitcask, data_root}]),
%%	{ok, RootDir} = rpc:call(Node, file, get_cwd, []),
%%	FullDir = filename:join(RootDir, DataDir),
%%	Files = [begin
%%		 BucketDir = filename:join(FullDir, Backend),
%%		 IdxStr = integer_to_list(Idx),
%%		 IdxDir = filename:join(BucketDir, IdxStr),
%%		 BitcaskPattern = filename:join([IdxDir, "*.bitcask.data"]),
%%		 Paths = rpc:call(Node, filelib, wildcard, [BitcaskPattern]),
%%		 ?assert(is_list(Paths)),
%%		 Files = [filename:basename(Path) || Path <- Paths],
%%		 {IdxDir, Files}
%%	 end || Backend <- Backends, Idx <- Partitions],
%%	[{IdxDir, Paths} || {IdxDir, Paths} <- Files, Paths =/= []].

%^% Fetches the backend for each partition in a cluster and returns the vnode states
% as well as a unique list of the current backends
fetch_backend_data(C1) ->
	Responses = [rpc:call(X, riak_core_vnode_manager, all_vnodes, [riak_kv_vnode]) || X <- C1],
	lager:info("All vnodes length: ~p~n", [length(Responses)]),
	Num = [{length(X)} || X <- Responses, X =/= {badrpc, nodedown}],
	lager:info("Length of partitions per node: ~p~n", [Num]),

	CoreVnodeStates = [sys:get_state(Pid) || Response <- Responses, Response =/= {badrpc, nodedown}, {_, _Idx, Pid} <- Response],
	lager:info("Length of core vnode states: ~p~n", [length(CoreVnodeStates)]),
	lager:info("CoreVnodeStates: ~p~n", [CoreVnodeStates]),
	BackendStates = [begin
						 KvVnodeState = element(4, N),
						 BackendState = element(5, KvVnodeState),
						 element(2, BackendState)
					 end || {active, N} <- CoreVnodeStates],
	lager:info("BackendStates: ~p, ~p~n", [length(BackendStates), BackendStates]),
	BackendNames = [element(1, X) || N <- BackendStates, X <- N],
	lager:info("Backends: ~p, ~p~n", [length(BackendNames), BackendNames]),
	UBackends = lists:usort(BackendNames),
	{CoreVnodeStates, BackendNames, UBackends}.