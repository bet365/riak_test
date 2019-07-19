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

confirm() ->
	?assertEqual(pass, test(1)),
	pass.

test(1) ->
	[C1] = _C = make_clusters_helper(),
%%	connect_clusters({hd(C1),"cluster1"}, {hd(C2), "cluster2"}),

	_BK = all_bkeys(),
	lager:info("Cluster of nodes: ~p~n", [C1]),

%%	Ring = rt:get_ring(hd(C1)),
%%	lager:info("Ring: ~p~n", [Ring]),

%%	Owners = riak_core_ring:all_owners(Ring),
%%	lager:info("Partitions length: ~p~n", [length(Owners)]),

	BitcaskFiles = list_bitcask_files(C1),
lager:info("Bitcask files: ~p~n", [BitcaskFiles]),
	{Node, File} = hd(BitcaskFiles),
	{BFile, Opt} = lists:nth(2, File),
	Ref = rpc:call(Node, bitcask, open, [BFile, Opt]),
	Output = rpc:call(Node, bitcask, list_keys, [Ref]),
	lager:info("Output of list keysL: ~p~n", [Output]),

	put_all_objects(C1, 0),
	cleanup([C1], ["cluster1"]),
%%	Test1 = {
%%		0,
%%		disabled,
%%		[{"cluster2", {allow, []}, {block,[]}}],
%%		all_bkeys()
%%	},
%%
%%	start_realtime(C1, "cluster2"),
%%	realtime_test(Test1, false, "repl", C),
%%	stop_realtime(C1, "cluster2"),

	pass.



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
		{riak_kv,
			[

				{storage_backend, riak_kv_split_backend},
				{split_backend_default, bucket_1},
				{split_backend, [
					{bucket_1, riak_kv_bitcask_backend, []}
				]}
			]}
	],
	Nodes = rt:deploy_nodes(8, Conf, [riak_kv, riak_repl]),

	Cluster1 = lists:sublist(Nodes, 1, 8),
%%	Cluster2 = lists:sublist(Nodes, 4, 3),

%%	[rpc:call(N1, erlang, disconnect_node, [N2]) || N1 <- Cluster1, N2 <- Cluster2 ++ Cluster3],
%%	[rpc:call(N1, erlang, disconnect_node, [N2]) || N1 <- Cluster2, N2 <- Cluster1 ++ Cluster3],
%%	[rpc:call(N1, erlang, disconnect_node, [N2]) || N1 <- Cluster3, N2 <- Cluster1 ++ Cluster2],

	lager:info("Build cluster 1"),
	repl_util:make_cluster(Cluster1),
%%	lager:info("Build cluster 2"),
%%	repl_util:make_cluster(Cluster2),
%%	lager:info("Build cluster 3"),
%%	repl_util:make_cluster(Cluster3),

	lager:info("waiting for leader to converge on cluster 1"),
	?assertEqual(ok, repl_util:wait_until_leader_converge(Cluster1)),
%%	lager:info("waiting for leader to converge on cluster 2"),
%%	?assertEqual(ok, repl_util:wait_until_leader_converge(Cluster2)),
%%	lager:info("waiting for leader to converge on cluster 2"),
%%	?assertEqual(ok, repl_util:wait_until_leader_converge(Cluster3)),

	repl_util:name_cluster(hd(Cluster1), "cluster1"),
	rt:wait_until_ring_converged(Cluster1),
%%	repl_util:name_cluster(hd(Cluster2), "cluster2"),
%%	rt:wait_until_ring_converged(Cluster2),
%%	repl_util:name_cluster(hd(Cluster3), "cluster3"),
%%	rt:wait_until_ring_converged(Cluster3),

%%	rt:wait_until_transfers_complete(Cluster1),
%%	rt:wait_until_transfers_complete(Cluster2),
%%	rt:wait_until_transfers_complete(Cluster3),

	[Cluster1].

all_bkeys() ->
	[{B,K} || B <- ?ALL_BUCKETS_NUMS, K <- ?ALL_KEY_NUMS].

put_all_objects(Cluster, TestNum) ->
	Node = hd(Cluster),
	{ok, C} = riak:client_connect(Node),
	[C:put(Obj) || Obj <- create_objects(all, TestNum)],
	lager:info("Placed all data on Cluster: ~p~n", [Cluster]).

create_objects(all, TestNum) ->
	[create_single_object(BN, KN, TestNum) || BN <- ?ALL_BUCKETS_NUMS, KN <- ?ALL_KEY_NUMS].

create_single_object(Bucket, Key, TestNum) ->
	riak_object:new(make_bucket(Bucket), make_key(Key), make_value(Bucket, Key, TestNum)).

make_bucket(N) -> list_to_binary("bucket-" ++ N).
make_key(N) -> list_to_binary("key-" ++ N).
make_value(BN, KN, TestNumber) -> list_to_binary("test-" ++ integer_to_list(TestNumber) ++ " ------ value-"
	++ BN ++ "-" ++ KN).

cleanup(Clusters, ClusterNames) ->
%%	clear_config(Clusters),
%%	check_object_filtering_config(Clusters),
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
	case {Result, timer:now_diff(erlang:now(), Time) > Timeout*1000000} of
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
	AllObjects = [get_single_object(C, BN, KN) || BN <-?ALL_BUCKETS_NUMS, KN <- ?ALL_KEY_NUMS],
	[Obj || Obj <- AllObjects, Obj /= notfound].

get_single_object(C, BN, KN) ->
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

list_bitcask_files(Nodes) ->
	[{Node, list_node_bitcask_files(Node)} || Node <- Nodes].

list_node_bitcask_files(Node) ->
	% Gather partitions owned, list *.bitcask.data on each.
	Partitions = rt:partitions_for_node(Node),
	{ok, DataDir} = rt:rpc_get_env(Node, [{bitcask, data_root}]),
	[begin
		 IdxStr = integer_to_list(Idx),
		 IdxDir = filename:join(DataDir, IdxStr),
		 BitcaskPattern = filename:join([IdxDir, "*.bitcask.data"]),
		 Paths = rpc:call(Node, filelib, wildcard, [BitcaskPattern]),
		 ?assert(is_list(Paths)),
		 Files = [filename:basename(Path) || Path <- Paths],
		 {IdxDir, Files}
	 end || Idx <- Partitions].