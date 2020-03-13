%%%-------------------------------------------------------------------
%%% @author dylanmitelo
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. Mar 2020 14:05
%%%-------------------------------------------------------------------
-module(multi_bitcask_instances_test).
-author("dylanmitelo").

%% API
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(ALL_BUCKETS_NUMS, 	["1", "2", "3"]).
-define(ALL_KEY_NUMS,		["1", "2", "3"]).

confirm() ->
	[C1, _C2] = C = make_clusters_helper(),
	[?assertEqual(pass, test(N, C1)) || N <- lists:seq(1,5)],
%%	?assertEqual(pass, test(6, C2)),
	destroy_clusters(C),
	pass.

%% Test that only default location is started and confirm it runs as intended.
test(1, C1) ->
	lager:info("Starting test 1"),
	{_BackendStates, MDBackends} = fetch_backend_data(C1),

	?assertEqual(0, length(MDBackends)),		%% Confirm no additional backends exist in metadata and there on node
%%	?assertEqual(64, length(BackendStates)),	%% 4 Nodes in each cluster is 64 partitions per cluster. 256 without transfers running but this is unreliable as sometimes they dont start quick enough

	Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
	put_all_objects(C1, 1),
	?assertEqual(true, check_objects("cluster1", C1, Expected1, erlang:now(), 240)),

	Responses = check_backends(["default"], C1),	%% Confirm only default location has been created
	lager:info("Check for backends: ~p~n", [Responses]),

	cleanup([C1], ["cluster1"]),
	pass;

%% Test new split can be added across all partitions for each node and that data is only added to the split once activation is complete
test(2, C1) ->
	lager:info("Starting test 2"),
	Buckets2 	= ["4", "5", "6"],
	Keys2 		= ["4", "5", "6"],
	%% Start second backend
	[rpc:call(Node, riak_kv_console, add_split_backend_local, [second_split]) || Node <- lists:reverse(C1)],
	check_backends(["default", "second_split"], C1),	%% Confirm requested splits are created

	{_BackendStates, _MDBackends} = fetch_backend_data(C1),

%%	?assertEqual(64, length(MDBackends)),		%% Confirm new split exists in metadata for all partitions
%%	?assertEqual(64, length(BackendStates)),	%% 4 Nodes in each cluster is 64 partitions per cluster. 256 without transfers running but this is unreliable as sometimes they dont start quick enough

	Expected0 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
	Expected1 = [{make_bucket(BN, "second_split"), make_key(KN)} || {BN, KN} <- all_bkeys()],
	Expected2 = [{make_bucket(BN), make_key(KN, "second_split")} || {BN, KN} <- all_bkeys()],
	put_all_objects(C1, 2),
	put_all_objects(C1, 2, {bucket, "second_split"}),
	put_all_objects(C1, 2, {key, "second_split"}),
	?assertEqual(true, check_objects("cluster1", C1, Expected0, erlang:now(), 240)),
	?assertEqual(true, check_objects("cluster1", C1, Expected1, {bucket, "second_split"}, erlang:now(), 240)),
	?assertEqual(true, check_objects("cluster1", C1, Expected2, {key, "second_split"}, erlang:now(), 240)),

	%% Check no data exists in split locations.
	BitcaskData = list_bitcask_files(C1),
	[?assertEqual(BackendFiles, []) ||
		{_Node, IdxFiles} <- BitcaskData,
		BackendData <- IdxFiles,
		{Backend, BackendFiles} <- BackendData,
		Backend =:= "second_split"],

	%% Activate split put data and confirm its retrievable and exists in split location
	rpc:call(hd(C1), riak_kv_console, activate_split_backend_local, [second_split]),

	Expected3 = [{make_bucket(BN, "second_split"), make_key(KN)} || {BN, KN} <- all_bkeys(1, 6)],
	put_all_objects(C1, 2, {bucket, "second_split"}, Buckets2, Keys2),
	?assertEqual(true, check_objects("cluster1", C1, Expected3, {bucket, "second_split"}, erlang:now(), 240, ?ALL_BUCKETS_NUMS ++ Buckets2, ?ALL_KEY_NUMS ++ Keys2)),

	BitcaskData1 = list_bitcask_files(C1),
	SplitFiles = [BackendFiles ||
		{_Node, IdxFiles} <- BitcaskData1,
		BackendData <- IdxFiles,
		{Backend, BackendFiles} <- BackendData,
		Backend =:= "second_split" andalso BackendFiles =/= []],
	?assertNotEqual([], SplitFiles),	%% Test that new put data for splits is in correct location

	lager:info("Second split with files in: ~p ~p~n", [length(SplitFiles), SplitFiles]),
	cleanup([C1], ["cluster1"], ["second_split"], Buckets2, Keys2),
	pass;

%% Test new split can be added and that special merge will transfer data from default location to the new split location
%% TODO Stop riak on a node and open up bitcask on it directly to inspect the data is correct
test(3, C1) ->
	lager:info("Starting test 3"),
	rt:wait_until_transfers_complete(C1),
	%% Start second backend
	This = [rpc:call(Node, riak_kv_console, add_split_backend_local, [second_split]) || Node <- C1],
	lager:info("Add splti response: ~p~n", [This]),
	check_backends(["default", "second_split"], C1),	%% Confirm requested splits are created

	Something05 = [rpc:call(Node, riak_core_metadata, get, [{split_backend, splits}, {second_split, Node}]) || Node <- C1],
	lager:info("Are backends in metadat: ~p~n", [Something05]),

	{_BackendStates, _MDBackends} = fetch_backend_data(C1),

%%	?assertEqual(64, length(MDBackends)),		%% Confirm new split exists in metadata for all partitions
%%	?assertEqual(64, length(BackendStates)),	%% 4 Nodes in each cluster is 64 partitions per cluster. 256 without transfers running but this is unreliable as sometimes they dont start quick enough

	Expected0 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
	Expected1 = [{make_bucket(BN, "second_split"), make_key(KN)} || {BN, KN} <- all_bkeys()],
	Expected2 = [{make_bucket(BN), make_key(KN, "second_split")} || {BN, KN} <- all_bkeys()],
%%	?assertEqual(true, check_objects("cluster1", C1, Expected0, erlang:now(), 240)),
%%	?assertEqual(true, check_objects("cluster1", C1, Expected1, {bucket, "second_split"}, erlang:now(), 240)),
%%	?assertEqual(true, check_objects("cluster1", C1, Expected2, {key, "second_split"}, erlang:now(), 240)),
	put_all_objects(C1, 2),
	put_all_objects(C1, 2, {bucket, "second_split"}),
	put_all_objects(C1, 2, {key, "second_split"}),
	?assertEqual(true, check_objects("cluster1", C1, Expected0, erlang:now(), 240)),
	?assertEqual(true, check_objects("cluster1", C1, Expected1, {bucket, "second_split"}, erlang:now(), 240)),
	?assertEqual(true, check_objects("cluster1", C1, Expected2, {key, "second_split"}, erlang:now(), 240)),

	%% Check no data exists in split locations.
	BitcaskData = list_bitcask_files(C1),
	lager:info("BitcaskData: ~p~n", [BitcaskData]),
	[?assertEqual([], BackendFiles) ||
		{_Node, IdxFiles} <- BitcaskData,
		BackendData <- IdxFiles,
		{Backend, BackendFiles} <- BackendData,
		Backend =:= "second_split"],

	%% Activate split put data and confirm its retrievable and exists in split location
	[rpc:call(Node, riak_kv_console, activate_split_backend_local, [second_split]) || Node <- C1],	%% TODO This needs to be done on all nodes rather than just the one
	%% Special merge second split
	[rpc:call(Node, riak_kv_console, special_merge_local, [second_split]) || Node <- C1],

	%% Confirm data is still retrievable
	?assertEqual(true, check_objects("cluster1", C1, Expected0, erlang:now(), 240)),
	?assertEqual(true, check_objects("cluster1", C1, Expected1, {bucket, "second_split"}, erlang:now(), 240)),
	?assertEqual(true, check_objects("cluster1", C1, Expected2, {key, "second_split"}, erlang:now(), 240)),
	%% Confirm special_merge moved data to the new location
	BitcaskData1 = list_bitcask_files(C1),
	SplitFiles = [BackendFiles ||
		{_Node, IdxFiles} <- BitcaskData1,
		BackendData <- IdxFiles,
		{Backend, BackendFiles} <- BackendData,
		Backend =:= "second_split" andalso BackendFiles =/= []],
	?assertNotEqual([], SplitFiles),	%% Test that new put data for splits is in correct location

	lager:info("Second split with files in: ~p ~p~n", [length(SplitFiles), SplitFiles]),
	cleanup([C1], ["cluster1"], ["second_split"]),
	pass;

%% Update node state and ensure it survives a shutdown
test(4, C1) ->
	lager:info("Starting test 4"),
	%% Start second backend
	[rpc:call(Node, riak_kv_console, add_split_backend_local, [second_split]) || Node <- C1],
	check_backends(["default", "second_split"], C1),	%% Confirm requested splits are created

	{_BackendStates, _MDBackends} = fetch_backend_data(C1),

%%	?assertEqual(64, length(MDBackends)),		%% Confirm new split exists in metadata for all partitions
%%	?assertEqual(64, length(BackendStates)),	%% 4 Nodes in each cluster is 64 partitions per cluster. 256 without transfers running but this is unreliable as sometimes they dont start quick enough

	Partitions = rt:partitions_for_node(hd(C1)),

	Response = [rpc:call(hd(C1), riak_kv_bitcask_backend, fetch_metadata_backends, [P]) || P <- Partitions],
	lager:info("Response from metadata fetches: ~p~n", [Response]),

	%% Activate split and confirm its retrievable and exists in split location
	[rpc:call(Node, riak_kv_console, activate_split_backend_local, [second_split]) || Node <- C1],

	%% Check although active no data has yet to be put to location
	BitcaskData = list_bitcask_files(C1),
	lager:info("BitcaskData files: ~p~n", [BitcaskData]),
	[?assertEqual([], BackendFiles) ||
		{_Node, IdxFiles} <- BitcaskData,
		BackendData <- IdxFiles,
		{Backend, BackendFiles} <- BackendData,
		Backend =:= "second_split"],

	rt:stop_and_wait(hd(C1)),
	rt:start_and_wait(hd(C1)),
	rt:wait_for_service(hd(C1), [riak_kv]),

	Response1 = [rpc:call(hd(C1), riak_kv_bitcask_backend, fetch_metadata_backends, [P]) || P <- Partitions],
	lager:info("Response from metadata fetches: ~p~n", [Response1]),
	[?assertEqual(active, X) || Y <- Response1, {_, X} <- Y],

%%	Expected0 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
	Expected1 = [{make_bucket(BN, "second_split"), make_key(KN)} || {BN, KN} <- all_bkeys()],
	Expected2 = [{make_bucket(BN), make_key(KN, "second_split")} || {BN, KN} <- all_bkeys()],
%%	put_all_objects(C1, 2),
	put_all_objects(C1, 2, {bucket, "second_split"}),
	put_all_objects(C1, 2, {key, "second_split"}),
%%	?assertEqual(true, check_objects("cluster1", C1, Expected0, erlang:now(), 240)),
	?assertEqual(true, check_objects("cluster1", C1, Expected1, {bucket, "second_split"}, erlang:now(), 240)),
	?assertEqual(true, check_objects("cluster1", C1, Expected2, {key, "second_split"}, erlang:now(), 240)),

	%% Check data exists in the splits even after restart of node
	BitcaskData1 = list_bitcask_files(C1),
	lager:info("BitcaskData files: ~p~n", [BitcaskData1]),
	Files = [BackendFiles ||
		{_Node, IdxFiles} <- BitcaskData1,
		BackendData <- IdxFiles,
		{Backend, BackendFiles} <- BackendData,
		Backend =:= "second_split"],
	?assertEqual(true, lists:any(fun(X) -> X =/= [] end, Files)),

	cleanup([C1], ["cluster1"], ["second_split"]),
	pass;

%% Check that if node is down whilst changing state the data is fine and can still be retrieved and put, it will just need to be activated etc again
test(5, C1) ->
	Buckets2 	= ["4", "5", "6"],
	Keys2 		= ["4", "5", "6"],
	lager:info("Starting test 5"),
	%% Start second backend
	[rpc:call(Node, riak_kv_console, add_split_backend_local, [second_split]) || Node <- C1],
	check_backends(["default", "second_split"], C1),	%% Confirm requested splits are created

	{_BackendStates, _MDBackends} = fetch_backend_data(C1),

%%	?assertEqual(64, length(MDBackends)),		%% Confirm new split exists in metadata for all partitions
%%	?assertEqual(64, length(BackendStates)),	%% 4 Nodes in each cluster is 64 partitions per cluster. 256 without transfers running but this is unreliable as sometimes they dont start quick enough

	Partitions = rt:partitions_for_node(hd(C1)),

	Response0 = [rpc:call(hd(C1), riak_kv_bitcask_backend, fetch_metadata_backends, [P]) || P <- Partitions],
	lager:info("Response from metadata fetches: ~p~n", [Response0]),

	%% Stop node
	rt:stop_and_wait(hd(C1)),

	%% Activate cluster split and check down node is not acticated so data should not be put
	[rpc:call(Node, riak_kv_console, activate_split_backend_local, [second_split]) || Node <- C1],

	%% Start node again
	rt:start_and_wait(hd(C1)),
	rt:wait_for_service(hd(C1), [riak_kv]),

	Response = [rpc:call(hd(C1), riak_kv_bitcask_backend, fetch_metadata_backends, [P]) || P <- Partitions],
	NotActive = [?assertEqual(false, X) || Y <- Response, {_, X} <- Y],
	lager:info("Response from metadata fetches: ~p~n", [Response]),
	lager:info("Not Active: ~p~n", [NotActive]),

%%	Expected0 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
	Expected1 = [{make_bucket(BN, "second_split"), make_key(KN)} || {BN, KN} <- all_bkeys()],
	Expected2 = [{make_bucket(BN), make_key(KN, "second_split")} || {BN, KN} <- all_bkeys()],
%%	put_all_objects(C1, 2),
	put_all_objects(C1, 2, {bucket, "second_split"}),
	put_all_objects(C1, 2, {key, "second_split"}),
%%	?assertEqual(true, check_objects("cluster1", C1, Expected0, erlang:now(), 240)),
	?assertEqual(true, check_objects("cluster1", C1, Expected1, {bucket, "second_split"}, erlang:now(), 240)),
	?assertEqual(true, check_objects("cluster1", C1, Expected2, {key, "second_split"}, erlang:now(), 240)),

	%% Check no data exists in split locations for downed node.
	BitcaskData1 = list_bitcask_files(C1),
	lager:info("BitcaskData files: ~p~n", [BitcaskData1]),
	%% Check no data in split location for downed node
	[?assertEqual([], BackendFiles) ||
		{Node, IdxFiles} <- BitcaskData1,
		BackendData <- IdxFiles,
		{Backend, BackendFiles} <- BackendData,
		Node =:= hd(C1),
		Backend =:= "second_split"],
	%% Check no data in default for rest of cluster
	[?assertEqual([], BackendFiles) ||
		{Node, IdxFiles} <- BitcaskData1,
		BackendData <- IdxFiles,
		{Backend, BackendFiles} <- BackendData,
		Node =/= hd(C1),
		Backend =:= "default"],

	[rpc:call(Node, riak_kv_console, activate_split_backend_local, [second_split]) || Node <- C1],

	Expected3 = [{make_bucket(BN, "second_split"), make_key(KN)} || {BN, KN} <- all_bkeys(1, 6)],
	put_all_objects(C1, 2, {bucket, "second_split"}, Buckets2, Keys2),
	?assertEqual(true, check_objects("cluster1", C1, Expected3, {bucket, "second_split"}, erlang:now(), 240, ?ALL_BUCKETS_NUMS ++ Buckets2, ?ALL_KEY_NUMS ++ Keys2)),

	BitcaskData2 = list_bitcask_files(C1),
	lager:info("BitcaskData files: ~p~n", [BitcaskData2]),
	SplitFiles = [BackendFiles ||
		{_Node, IdxFiles} <- BitcaskData2,
		BackendData <- IdxFiles,
		{Backend, BackendFiles} <- BackendData,
		Backend =:= "second_split", BackendFiles =/= []],
	?assertNotEqual([], SplitFiles),

	cleanup([C1], ["cluster1"], ["second_split"]),
	pass.


%% =============================================================================================================================================================================
%%	TODO	Required tests
%% 1). Only start default, add data and confirm it exists and only default location has been populated. - Completed
%% 2). Add split, populate bitcask with data for both default and split and confirm no data has been put to split location. - Completed
%% 3). Add split, put data, activate split, put new split data. Confirm puts have gone to new location but that all data is retrievable. - Completed
%% 4). Same as above but then special merge. Confirm new files appear in split location and that data is still retrievable, from before and after activation. - Complete
%% 5). Do same as above but for single partitions rather than all. At each stage confirm if data is retrievable and correct files exist in partition/split location.
%% 6). Add split to single node and activate, put data and ensure everything works as intended. - This will be tested by the above
%% 7). Create splits then stop start nodes, confirm split state continuity and that all data exists where it is supposed to that other data operations continue as expected. - Completed
%% 8). Check special_merging and merging work as expected.
%% 9). Delete data (via expired puts??) and confirm that when active/special_merge/merged that data does not reappear.
%% =============================================================================================================================================================================

%%check_active(ActiveBackends, Nodes) ->
%%	{BackendStates, _MDBackends} = fetch_backend_data(Nodes),
%%	lager:info("State to be passed to is active: ~p~n", [BackendStates]),
%%	Output = [rpc:call(X, riak_kv_bitcask_backend, is_backend_active, [hd(ActiveBackends), State]) || X <- Nodes, State <- BackendStates],
%%	NewOutput = [X || X <- Output, false =:= is_tuple(X)],
%%	lager:info("Output: ~p~n", [NewOutput]),
%%	Output.


%% =======================================================
%% Internal Functions
%% =======================================================

fetch_backend_data(C1) ->
	Responses = [{X, rpc:call(X, riak_core_vnode_manager, all_vnodes, [riak_kv_vnode])} || X <- C1],
	lager:info("All vnodes length: ~p and ~p~n", [length(Responses), Responses]),

	CoreVnodeStates = [{X, Idx, sys:get_state(Pid)} || {X, Response} <- Responses, Response =/= {badrpc, nodedown}, {_, Idx, Pid} <- Response],
	lager:info("Length of core vnode states: ~p~n", [length(CoreVnodeStates)]),
	lager:info("CoreVnodeStates: ~p~n~n", [CoreVnodeStates]),
	BackendStates = [begin
						 KvVnodeState = element(4, N),
						 element(5, KvVnodeState)
					 end || {_, _, {active, N}} <- CoreVnodeStates],
	lager:info("BackendStates: ~p, ~p~n", [length(BackendStates), BackendStates]),

	MD = [rpc:call(X, riak_kv_bitcask_backend, fetch_metadata_backends, [Idx]) || {X, Idx, _} <- CoreVnodeStates],
	MDBackends = [X || X <- MD, X =/= []],
	lager:info("Length of Metadata backends: ~p~n", [length(MDBackends)]),
%%	BackendNames = [element(1, X) || N <- BackendStates, X <- N],
%%	lager:info("Backends: ~p, ~p~n", [length(BackendNames), BackendNames]),
%%	UBackends = lists:usort(BackendNames),
	{BackendStates, MDBackends}.
%%	{CoreVnodeStates, BackendNames, UBackends}.

check_backends(Backends, Nodes) ->
	NodeFiles = list_bitcask_files(Nodes),
	BitcaskBackends = [Backend ||
		{_Node, IdxFiles} <- NodeFiles,
		BackendData <- IdxFiles,
		{Backend, _BackendFiles} <- BackendData],
	?assertEqual(lists:sort(Backends), lists:usort(BitcaskBackends)).

list_bitcask_files(Nodes) ->
	[{Node, list_node_bitcask_files(Node)} || Node <- Nodes].

list_node_bitcask_files(Node) ->
	% Gather partitions owned, list *.bitcask.data on each.
	Partitions = rt:partitions_for_node(Node),
	{ok, DataRoot} = rt:rpc_get_env(Node, [{bitcask, data_root}]),
	{ok, RootDir} = rpc:call(Node, file, get_cwd, []),
	FullDir = filename:join(RootDir, DataRoot),
	lager:info("FullDir: ~p~n", [FullDir]),
	Something = [begin
		 IdxStr = integer_to_list(Idx),
		 IdxDir = filename:join(FullDir, IdxStr),
		 lager:info("File name attempting : ~p ~p~n", [FullDir, IdxStr]),
		 {ok, DataDirs} = rpc:call(Node, file, list_dir, [IdxDir]),
		 Dirs = [X || X <- DataDirs, X =/= "version.txt"],
		 BackendFiles = [begin
			  {ok, Files1} = rpc:call(Node, file, list_dir, [filename:join(IdxDir, Dir)]),
			  {Dir, Files1}
		 end || Dir <- Dirs],
%%		 {IdxDir, Dirs}
			 BackendFiles
	 end || Idx <- Partitions],
	lager:info("backend files: ~p~n", [Something]),
	Something.
%%	[{IdxDir, Paths} || {IdxDir, Paths} <- Files, Paths =/= []].


all_bkeys() ->
	[{B,K} || B <- ?ALL_BUCKETS_NUMS, K <- ?ALL_KEY_NUMS].
all_bkeys(Start, End) ->
	[{integer_to_list(B),integer_to_list(K)} || B <- lists:seq(Start, End), K <- lists:seq(Start, End)].

%%put_all_objects(Cluster, TestNum) ->
%%	Node = hd(Cluster),
%%	{ok, C} = riak:client_connect(Node),
%%	[C:put(Obj) || Obj <- create_objects(all, TestNum)],
%%	lager:info("Placed all data on Cluster: ~p~n", [Cluster]).

put_all_objects(Cluster, TestNum) ->
	put_all_objects(Cluster, TestNum, undefined).
put_all_objects(Cluster, TestNum, Split) ->
	put_all_objects(Cluster, TestNum, Split, all, all).
put_all_objects(Cluster, TestNum, Split, BNList, KNList) ->
	Node = hd(Cluster),
	{ok, C} = riak:client_connect(Node),
	[C:put(Obj) || Obj <- create_objects(BNList, KNList, TestNum, Split)],
	lager:info("Placed all data on Cluster: ~p~n", [Cluster]).

%%create_objects(all, TestNum) ->
%%	[create_single_object(BN, KN, TestNum) || BN <- ?ALL_BUCKETS_NUMS, KN <- ?ALL_KEY_NUMS].
%%create_objects_for_buckets(BNList, TestNum) ->
%%	[create_single_object(BN, KN, TestNum) || BN <- BNList, KN <- ?ALL_KEY_NUMS].
create_objects(all, all, TestNum, Split) ->
	[create_single_object(BN, KN, TestNum, Split) || BN <- ?ALL_BUCKETS_NUMS, KN <- ?ALL_KEY_NUMS];
create_objects(BNList, KNList, TestNum, Split) ->
	[create_single_object(BN, KN, TestNum, Split) || BN <- BNList, KN <- KNList].

%%create_single_object(Bucket, Key, TestNum) ->
%%	riak_object:new(make_bucket(Bucket), make_key(Key), make_value(Bucket, Key, TestNum)).
create_single_object(Bucket, Key, TestNum, undefined) ->
	riak_object:new(make_bucket(Bucket), make_key(Key), make_value(Bucket, Key, TestNum));
create_single_object(Bucket, Key, TestNum, {key, Split}) ->
	riak_object:new(make_bucket(Bucket), make_key(Key, Split), make_value(Bucket, Key, TestNum));
create_single_object(Bucket, Key, TestNum, {bucket, Split}) ->
	riak_object:new(make_bucket(Bucket, Split), make_key(Key), make_value(Bucket, Key, TestNum)).

make_bucket(N) -> list_to_binary("bucket-" ++ N).
make_bucket(_N, Split) -> list_to_binary(Split).
make_key(N) -> list_to_binary("key-" ++ N).
make_key(_N, Split) -> list_to_binary(Split).
make_value(BN, KN, TestNumber) -> list_to_binary("test-" ++ integer_to_list(TestNumber) ++ " ------ value-"
	++ BN ++ "-" ++ KN).

%% =======================================================
%% Setup
%% =======================================================

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
			]},
		{bitcask,
			[
				{merge_window, never},
				{data_root, "./data/bitcask"}
			]}
	],

%%	Conf2 = [
%%		{riak_repl,
%%			[
%%				%% turn off fullsync
%%				{delete_mode, 1},
%%				{fullsync_interval, 0},
%%				{fullsync_strategy, keylist},
%%				{fullsync_on_connect, false},
%%				{fullsync_interval, disabled},
%%				{max_fssource_node, 64},
%%				{max_fssink_node, 64},
%%				{max_fssource_cluster, 64},
%%				{default_bucket_props, [{n_val, 3}, {allow_mult, false}]},
%%				{override_capability,
%%					[{default_bucket_props_hash, [{use, [consistent, datatype, n_val, allow_mult, last_write_wins]}]}]}
%%			]},
%%		{riak_kv,
%%			[
%%				{storage_backend, riak_kv_bitcask_backend}
%%			]},
%%		{bitcask,
%%			[
%%				{merge_window, never},
%%				{data_root, "./data/bitcask"}
%%			]}
%%	],
	NodeConf = [{current, Conf} || _ <- lists:seq(1,4)],
	NodeConf2 = [{current, Conf} || _ <- lists:seq(5,8)],
	lager:info("Conf1: ~p~n", [NodeConf]),
	lager:info("Conf2: ~p~n", [NodeConf2]),
	AllConf = lists:flatten([NodeConf | NodeConf2]),
%%	AllConf = NodeConf,
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
	lager:info("Cluster1 current bitcask config of first node: ~p~n", [rpc:call(hd(Cluster1), application, get_all_env, [riak_kv])]),

	rt:wait_until_transfers_complete(Cluster1),	%% TODO Need the transfers to complete when properly running
%%	rt:wait_until_transfers_complete(Cluster2),
%%	rt:wait_until_transfers_complete(Cluster3),

	[Cluster1, Cluster2].

destroy_clusters(Clusters) ->
	Nodes = lists:flatten(Clusters),
	rt:clean_cluster(Nodes).

cleanup(Clusters, ClusterNames) ->
	cleanup(Clusters, ClusterNames, undefined, ?ALL_BUCKETS_NUMS, ?ALL_BUCKETS_NUMS).
cleanup(Clusters, ClusterNames, Splits) ->
	cleanup(Clusters, ClusterNames, Splits, ?ALL_BUCKETS_NUMS, ?ALL_BUCKETS_NUMS).
cleanup(Clusters, ClusterNames, Splits, BN, KN) ->
%%	clear_config(Clusters),
%%	check_object_filtering_config(Clusters),
	lager:info("Starting cleanup procedure"),
	delete_data(Clusters, ClusterNames, Splits, BN, KN),
	delete_files(),
	delete_split_backend(Clusters, Splits),
	timer:sleep(2000),
	lager:info("Cleanup complete ~n", []),
	check(Clusters, ClusterNames, Splits, BN, KN),
	ok.

delete_files() ->
	file:delete("/tmp/config1"),
	file:delete("/tmp/config2").

delete_split_backend([], _Splits) ->
	ok;
delete_split_backend(_, undefined) ->
	ok;
delete_split_backend([Cluster | Rest], Splits) ->
%%	lager:info("WHERE ARE WE#############################: ~p~n", [rpc:call(hd(hd(Clusters)), os, cmd, ["pwd"])]),
	Something05 = [rpc:call(Node, riak_core_metadata, get, [{split_backend, splits}, {list_to_atom(Split), Node}]) || Node <- Cluster, Split <- Splits],
	[rpc:call(Node, riak_kv_console, remove_split_backend_local, [list_to_atom(Split)]) || Node <- Cluster, Split <- Splits],
	Something2 = [rpc:call(Node, riak_core_metadata, get, [{split_backend, splits}, {list_to_atom(Split), Node}]) || Node <- Cluster, Split <- Splits],
%%	[rpc:call(Node, file, delete, ["./data/cluster_meta"]) || Node <- Cluster],
	lager:info("##################### Checking on metadata: ~p~n", [Something05]),
	lager:info("##################### Checking on metadata: ~p~n", [Something2]),
	delete_split_backend(Rest, Splits).

delete_data([], [], _, _, _) -> ok;
delete_data([Cluster|Rest1], [ClusterName|Rest2], Split, BN, KN) ->
	delete_all_objects(ClusterName, Cluster, Split, BN, KN),
	delete_data(Rest1, Rest2, Split, BN, KN).

%%delete_all_objects(ClusterName, Cluster, Split, all, all) ->
%%	Node = hd(Cluster),
%%	{ok, C} = riak:client_connect(Node),
%%	[C:delete(make_bucket(B), make_key(K)) || B <- ?ALL_BUCKETS_NUMS, K <- ?ALL_KEY_NUMS],
%%	[C:delete(make_bucket(B, Split), make_key(K)) || B <- ?ALL_BUCKETS_NUMS, K <- ?ALL_KEY_NUMS],
%%	[C:delete(make_bucket(B), make_key(K, Split)) || B <- ?ALL_BUCKETS_NUMS, K <- ?ALL_KEY_NUMS],
%%	lager:info("Deleted all data for ~p", [ClusterName]);
delete_all_objects(ClusterName, Cluster, Split, BN, KN) ->
	Node = hd(Cluster),
	{ok, C} = riak:client_connect(Node),
	case Split of
		undefined ->
			[C:delete(make_bucket(B), make_key(K)) || B <- BN, K <- KN];
		_ ->
			[C:delete(make_bucket(B), make_key(K)) || B <- BN, K <- KN],
			[C:delete(make_bucket(B, Split), make_key(K)) || B <- BN, K <- KN],
			[C:delete(make_bucket(B), make_key(K, Split)) || B <- BN, K <- KN]
	end,
	lager:info("Deleted all data for ~p", [ClusterName]).

check([], [], _, _, _) -> ?assertEqual(true, true);
check(Clusters = [Cluster|Rest1], ClusterNames =[ClusterName|Rest2], Split, BN, KN) ->
	case check_objects(ClusterName, Cluster, [], Split, erlang:now(), 10, BN, KN) of
		true -> check(Rest1, Rest2, Split, BN, KN);
		false -> cleanup(Clusters, ClusterNames, Split, BN, KN)
	end.

check_objects(ClusterName, Cluster, Expected, Time, Timeout) ->
	check_objects(ClusterName, Cluster, Expected, undefined, Time, Timeout).
check_objects(ClusterName, Cluster, Expected, Split, Time, Timeout) ->
	check_objects(ClusterName, Cluster, Expected, Split, Time, Timeout, ?ALL_BUCKETS_NUMS, ?ALL_KEY_NUMS).
check_objects(ClusterName, Cluster, Expected, Split, Time, Timeout, BN, KN) ->
	check_object_helper(ClusterName, Cluster, Expected, Split, Time, Timeout, BN, KN).
%% TODO make this quicker for []!
check_object_helper(ClusterName, Cluster, Expected, Split, Time, Timeout, BN, KN) ->
	Actual = get_all_objects(Cluster, Split, BN, KN),
	lager:info("Actual objects: ~p~n", [Actual]),
	lager:info("Expected objects: ~p~n", [Expected]),
	Result = lists:sort(Actual) == lists:sort(Expected),
	case {Result, timer:now_diff(erlang:now(), Time) > Timeout*100} of
		{true, _} ->
			print_objects(ClusterName, Actual),
			true;
		{false, false} ->
			check_object_helper(ClusterName, Cluster, Expected, Split, Time, Timeout, BN, KN);
		{false, true} ->
			print_objects(ClusterName, Actual),
			false
	end.

get_all_objects(Cluster, Split, BN0, KN0) ->
	Node = hd(Cluster),
	{ok, C} = riak:client_connect(Node),
	lager:info("Node trying to connect to: ~p, CLient: ~p~n", [Node, C]),
	AllObjects = [get_single_object(C, BN, KN, Split) || BN <- BN0, KN <- KN0],
	[Obj || Obj <- AllObjects, Obj /= notfound].

get_single_object(C, BN, KN, Split0) ->
	lager:info("BN: ~p and KN ~p and Split: ~p~n", [BN, KN, Split0]),
	case Split0 of
		undefined ->
			do_get(C, make_bucket(BN), make_key(KN));
		{bucket, Split1} ->
			do_get(C, make_bucket(BN, Split1), make_key(KN));
		{key, Split1} ->
			do_get(C, make_bucket(BN), make_key(KN, Split1));
		Split1 ->
			case do_get(C, make_bucket(BN, Split1), make_key(KN)) of
				notfound ->
					do_get(C, make_bucket(BN), make_key(KN, Split1));
				Obj ->
					Obj
			end
		end.

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