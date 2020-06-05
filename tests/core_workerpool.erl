%%%-------------------------------------------------------------------
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. May 2020 4:08 PM
%%%-------------------------------------------------------------------
-module(core_workerpool).

%% API
%%-behaviour(riak_test).
-export([confirm/0]).
-export([
	wait_func/0
]).
-include_lib("eunit/include/eunit.hrl").
-compile({parse_transform, rt_intercept_pt}).


-define(ALL_BUCKETS_NUMS, ["1","2","3"]).
-define(ALL_KEY_NUMS, ["1","2","3"]).

%% Test for bug when shutting down riak_core. The worker_pool hangs ofr 60 secs if a worker crashes during a shutdown.
%% In this test we check shutdown with a crash is under 30 secs to confirm bug fix
confirm() ->
	[C | Rest] = make_clusters_helper(),
	[?assertEqual(pass, test(N, [C])) || N <- lists:seq(1,1)],
	destroy_clusters(Rest),
	pass.

test(1, C) ->
	Expected = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
	put_all_objects(C, 1),
	?assertEqual(true, check_objects("cluster1", C, Expected, erlang:now(), 240)),

	fold_keys_and_wait_shutdown(C, make_bucket("1")).

fold_keys_and_wait_shutdown(Cluster, Bucket) ->
	Node = hd(Cluster),
	{ok, C} = rpc:call(Node, riak, client_connect, [Node]),
	SomePid = spawn(core_workerpool, wait_func, []),

	rt_intercept:add(Node, {riak_core_vnode_worker, [{{handle_cast, 2},
		{[Node, SomePid],
			fun({work, Work, WorkFrom, _Caller}, {state, Mod, ModState} = _State) ->
				case Mod:handle_work(Work, WorkFrom, ModState) of
					  {reply, Reply, NS} ->
						  riak_core_vnode:reply(WorkFrom, Reply),
						  NS;
					  {noreply, NS} ->
						  NS
				end,
				SomePid ! {rand_msg, Node},
				timer:sleep(5000),
				crash = something
			end}}]}),
	C:list_keys(Bucket),
	loop(0, Node).

loop(Count, Node) ->
	case rt:is_pingable(Node) of
		true ->
			timer:sleep(100),
			case Count of
				X when X >= 300 ->
					fail;
				_ ->
					loop(Count +1, Node)
			end;
		_ ->
			pass
	end.

wait_func() ->
	receive
		{rand_msg, Node} ->
			rtdev:stop(Node) %% The stop takes 10 secs before it actually calls the stop so we should wait for that first
	end.


all_bkeys() ->
	[{B,K} || B <- ?ALL_BUCKETS_NUMS, K <- ?ALL_KEY_NUMS].

put_all_objects([], _TestNum) ->
	ok;
put_all_objects([Node | Rest], TestNum) ->
	put_objects_helper(Node, TestNum),
	put_all_objects(Rest, TestNum).
put_objects_helper(Node, TestNum) ->
	{ok, C} = riak:client_connect(Node),
	[C:put(Obj) || Obj <- create_objects(TestNum)].

create_objects(TestNum) ->
	[create_single_object(BN, KN, TestNum) || BN <- ?ALL_BUCKETS_NUMS, KN <- ?ALL_KEY_NUMS].

create_single_object(Bucket, Key, TestNum) ->
	riak_object:new(make_bucket(Bucket), make_key(Key), make_value(Bucket, Key, TestNum)).

make_bucket(N) -> list_to_binary("bucket-" ++ N).
make_key(N) -> list_to_binary("key-" ++ N).
make_value(BN, KN, TestNumber) -> list_to_binary("test-" ++ integer_to_list(TestNumber) ++ " ------ value-"
	++ BN ++ "-" ++ KN).



check_objects(ClusterName, Cluster, Expected, Time, Timeout) ->
	check_object_helper(ClusterName, Cluster, Expected, Time, Timeout, ?ALL_BUCKETS_NUMS, ?ALL_KEY_NUMS).

%% TODO make this quicker for []!
check_object_helper(ClusterName, Cluster, Expected, Time, Timeout, BN, KN) ->
	Actual = get_all_objects(Cluster, BN, KN),
	Result = lists:sort(Actual) == lists:sort(Expected),
	case {Result, timer:now_diff(erlang:now(), Time) > Timeout*100} of
		{true, _} ->
			print_objects(ClusterName, Actual),
			true;
		{false, false} ->
			check_object_helper(ClusterName, Cluster, Expected, Time, Timeout, BN, KN);
		{false, true} ->
			print_objects(ClusterName, Actual),
			false
	end.

get_all_objects(Cluster, BN0, KN0) ->
	Node = hd(Cluster),
	{ok, C} = riak:client_connect(Node),
	AllObjects = [get_single_object(C, BN, KN) || BN <- BN0, KN <- KN0],
	[Obj || Obj <- AllObjects, Obj /= notfound].

get_single_object(C, BN, KN) ->
%%	lager:info("BN: ~p and KN ~p and Split: ~p~n", [BN, KN, Split0]),
	do_get(C, make_bucket(BN), make_key(KN)).

do_get(C, Bucket, Key) ->
	case C:get(Bucket, Key) of
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
				{storage_backend, riak_kv_bitcask_backend}
%%				{bitcask_merge_check_interval, 30000},
%%				{bitcask_merge_check_jitter, 0.3}
			]},
		{bitcask,
			[
				{merge_window, never},
				{data_root, "./data/bitcask"}
			]},
		{vm_args, [
			{'-shutdown_time', 1000000}]}
	],

	NodeConf = [{current, Conf} || _ <- lists:seq(1,4)],
	NodeConf2 = [{current, Conf} || _ <- lists:seq(5,8)],
	AllConf = lists:flatten([NodeConf | NodeConf2]),

	lager:info("Conf1: ~p~n", [NodeConf]),

	Nodes = rt:deploy_nodes(AllConf, [riak_kv, riak_repl]),
	lager:info("Nodes: ~p~n", [Nodes]),


	Cluster1 = lists:sublist(Nodes, 1, 4),
	Cluster2 = lists:sublist(Nodes, 5, 8),

	[rpc:call(N1, erlang, disconnect_node, [N2]) || N1 <- Cluster1, N2 <- Cluster2],

	lager:info("Build cluster 1"),
	repl_util:make_cluster(Cluster1),
	repl_util:make_cluster(Cluster2),

	lager:info("waiting for leader to converge on cluster 1"),
	?assertEqual(ok, repl_util:wait_until_leader_converge(Cluster1)),
	?assertEqual(ok, repl_util:wait_until_leader_converge(Cluster2)),

	repl_util:name_cluster(hd(Cluster1), "cluster1"),
	rt:wait_until_ring_converged(Cluster1),
	repl_util:name_cluster(hd(Cluster2), "cluster2"),
	rt:wait_until_ring_converged(Cluster2),

	lager:info("Cluster1 current bitcask config of first node: ~p~n", [rpc:call(hd(Cluster1), application, get_all_env, [riak_kv])]),

%%	rt:wait_until_transfers_complete(Cluster1),	%% No need for transfers in this test since we're only testing shutdown time of core.

	Nodes.

destroy_clusters(Clusters) ->
	Nodes = lists:flatten(Clusters),
	rt:clean_cluster(Nodes).
