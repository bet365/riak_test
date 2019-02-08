-module(repl2_object_filtering).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(ALL_BUCKETS_NUMS, ["1","2", "3", {"1", "1"}, {"1", "2"}, {"1", "3"}]).
-define(ALL_KEY_NUMS, ["1","2","3"]).

confirm() ->

    %% Single tests - these are run as {TestNumeber, Status, Config, ExpectedObjects}
    SingleTests = get_config(),

    %% Double tests - 2 tests are run on each of these.
    %% allow is changed to {allow, ['*']}
    %% block is changed to {block, Allowed}
    %% expected is now the inverse of what was expected in the first test
    DoubleTests2 = lists:flatten(
        [
            [merge_configs([Config1, Config2]) || Config1 <- get_config(bucket), Config2 <- get_config(metadata)],
            [merge_configs([Config1, Config2]) || Config1 <- get_config(bucket), Config2 <- get_config(lastmod_age)],
            [merge_configs([Config1, Config2]) || Config1 <- get_config(bucket), Config2 <- get_config(lastmod)],
            [merge_configs([Config1, Config2]) || Config1 <- get_config(metadata), Config2 <- get_config(lastmod_age)],
            [merge_configs([Config1, Config2]) || Config1 <- get_config(metadata), Config2 <- get_config(lastmod)],
            [merge_configs([Config1, Config2]) || Config1 <- get_config(lastmod), Config2 <- get_config(lastmod_age)]
        ]),
    DoubleTests4 = lists:flatten(
        [
            [merge_configs([Config1, Config2, Config3]) || Config1 <- get_config(bucket), Config2 <- get_config(metadata), Config3 <- get_config(lastmod_age)],
            [merge_configs([Config1, Config2, Config3]) || Config1 <- get_config(bucket), Config2 <- get_config(metadata), Config3 <- get_config(lastmod)],
            [merge_configs([Config1, Config2, Config3]) || Config1 <- get_config(bucket), Config2 <- get_config(lastmod), Config3 <- get_config(lastmod_age)],
            [merge_configs([Config1, Config2, Config3]) || Config1 <- get_config(metadata), Config2 <- get_config(lastmod), Config3 <- get_config(lastmod_age)]
        ]),

    DoubleTests1 = get_config(bucket) ++ get_config(metadata) ++ get_config(lastmod_age) ++ get_config(lastmod),
    DoubleTests3 = DoubleTests2 ++ lists:map(fun not_rule/1, DoubleTests2),
    DoubleTests5 = DoubleTests4 ++ lists:map(fun not_rule/1, DoubleTests4),
    DoubleTests = DoubleTests1 ++ DoubleTests3 ++ DoubleTests5,

    Tests = {SingleTests, DoubleTests},

    Clusters = make_clusters(),
    run_all_tests_realtime(Tests, Clusters),
    destroy_clusters(Clusters),
    run_all_tests_fullsync(Tests, make_clusters()),
    pass.

make_clusters() ->
    delete_files(),
    [Cluster1, Cluster2, Cluster3] = make_clusters_helper(),
    connect_clusters({hd(Cluster1),"cluster1"}, {hd(Cluster2), "cluster2"}),
    connect_clusters({hd(Cluster2),"cluster2"}, {hd(Cluster3), "cluster3"}),

    %% create bucket types
    rpc:call(hd(Cluster1), riak_kv_console, bucket_type_create, [["type-1", "{\"props\":{\"allow_mult\":\"false\",\"dvv_enabled\":false}}"]]),
    rpc:call(hd(Cluster2), riak_kv_console, bucket_type_create, [["type-1", "{\"props\":{\"allow_mult\":\"false\",\"dvv_enabled\":false}}"]]),
    rpc:call(hd(Cluster3), riak_kv_console, bucket_type_create, [["type-1", "{\"props\":{\"allow_mult\":\"false\",\"dvv_enabled\":false}}"]]),
    rpc:call(hd(Cluster1), riak_kv_console, bucket_type_activate, [["type-1"]]),
    rpc:call(hd(Cluster2), riak_kv_console, bucket_type_activate, [["type-1"]]),
    rpc:call(hd(Cluster3), riak_kv_console, bucket_type_activate, [["type-1"]]),
    [Cluster1, Cluster2, Cluster3].

destroy_clusters(Clusters) ->
    Nodes = lists:flatten(Clusters),
    rt:clean_cluster(Nodes).

merge_configs(List) ->
    merge_configs(List, []).

merge_configs([], []) -> [];
merge_configs([C], []) -> C;
merge_configs([], Merged) ->
    Merged;
merge_configs([C], Merged) ->
    merge_configs_helper(C, Merged);
merge_configs([Config1, Config2 | Rest], []) ->
    Merge1 = merge_configs_helper(Config1, Config2),
    merge_configs(Rest, Merge1);
merge_configs([Config1 | Rest], Merged) ->
    Merge1 = merge_configs_helper(Config1, Merged),
    merge_configs(Rest, Merge1).

merge_configs_helper(Config1, Config2) ->
    {N1, Enabled, [{ClusterName, {allow, R1}, {block, []}}], E1} = Config1,
    {N2, Enabled, [{ClusterName, {allow, R2}, {block, []}}], E2} = Config2,
    S1 = sets:from_list(E1),
    S2 = sets:from_list(E2),
    S3 = sets:intersection(S1, S2),
    E3 = sets:to_list(S3),
    Rule = lists:flatten(R1++R2),
    {N1+N2, Enabled, [{ClusterName, {allow, [Rule]}, {block, []}}], E3}.



run_all_tests_realtime({SingleTests, DoubleTests}, [Cluster1, Cluster2, Cluster3]) ->
    start_realtime(Cluster1, "cluster2"),
    start_realtime(Cluster2, "cluster3"),
    timer:sleep(60000),
    [run_test_single(realtime, Test, [Cluster1, Cluster2, Cluster3]) || Test <- SingleTests],
    [run_test_double(realtime, Test, [Cluster1, Cluster2, Cluster3]) || Test <- DoubleTests],
    stop_realtime(Cluster1, "cluster2"),
    stop_realtime(Cluster2, "cluster3"),
    timer:sleep(60000).

run_all_tests_fullsync({SingleTests, DoubleTests}, [Cluster1, Cluster2, Cluster3]) ->
    [run_test_single(fullsync, Test, [Cluster1, Cluster2, Cluster3]) || Test <- SingleTests],
    [run_test_double(fullsync, Test, [Cluster1, Cluster2, Cluster3]) || Test <- DoubleTests].


run_test_double(ReplMode, Test1={N,Status,[{Name, {allow, Allowed}, {block, []}}],Expected}, Clusters)->
    run_test_single(ReplMode, Test1, Clusters),
    Expected2 = all_bkeys() -- Expected,
    Test2 = {N*-1, Status, [{Name, {allow, ['*']}, {block, Allowed}}], Expected2},
    run_test_single(ReplMode, Test2, Clusters).

run_test_single(fullsync, Test, Clusters) ->
    ?assertEqual(pass, fullsync_test(Test, "repl", Clusters)),
    ?assertEqual(pass, fullsync_test(Test, "fullsync", Clusters));

run_test_single(realtime, Test, Clusters) ->
    ?assertEqual(pass, realtime_test(Test, true, "repl", Clusters)),
    ?assertEqual(pass, realtime_test(Test, true, "realtime", Clusters)),
    ?assertEqual(pass, realtime_test(Test, false, "repl", Clusters)),
    ?assertEqual(pass, realtime_test(Test, false, "realtime", Clusters)).

print_test(Number, Mode, Config) ->
    ReplMode = fullsync,
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("Test: ~p ~n", [Number]),
    lager:info("Repl Mode: ~p ~n", [ReplMode]),
    lager:info("Object Filtering Mode: ~p ~n", [Mode]),
    lager:info("Config: ~p ~n", [Config]),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------").

print_test(Number, Mode, SendToCluster3, Config) ->
    ReplMode = realtime,
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("Test: ~p ~n", [Number]),
    lager:info("Repl Mode: ~p ~n", [ReplMode]),
    lager:info("Object Filtering Mode: ~p ~n", [Mode]),
    lager:info("Send To Cluster 3: ~p ~n", [SendToCluster3]),
    lager:info("Config: ~p ~n", [Config]),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------").

%% ================================================================================================================== %%
%%                                              Tests                                                                 %%
%% ================================================================================================================== %%
fullsync_test({0,_,Config,_}, Mode, [Cluster1, Cluster2, Cluster3]) ->
    print_test(0, Mode, Config),
    put_all_objects(Cluster1, 0, fullsync, Mode),
    start_fullsync(Cluster1, "cluster2"),
    %% ================================================================== %%
    Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
    Expected2 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
    ?assertEqual(true, check_objects("cluster1", Cluster1, Expected1, erlang:now(), 30)),
    ?assertEqual(true, check_objects("cluster2", Cluster2, Expected2, erlang:now(), 30)),
    cleanup([Cluster1, Cluster2, Cluster3]),
    stop_fullsync(Cluster1, "cluster2"),
    pass;
fullsync_test({TestNumber, Status, Config, ExpectedList}, Mode, C=[Cluster1, Cluster2, Cluster3]) ->
    print_test(TestNumber, Mode, Config),
    write_terms("/tmp/config1", Config),
    set_object_filtering(Cluster1, Status, "/tmp/config1", Mode),
    check_object_filtering_config(C),
    %% ================================================================== %%
    put_all_objects(Cluster1, TestNumber, fullsync, Mode),
    start_fullsync(Cluster1, "cluster2"),
    %% ================================================================== %%

    Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
    Expected2 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- ExpectedList],
    ?assertEqual(true, check_objects("cluster1", Cluster1, Expected1, erlang:now(), 30)),
    ?assertEqual(true, check_objects("cluster2", Cluster2, Expected2, erlang:now(), 30)),
    cleanup([Cluster1, Cluster2, Cluster3]),
    stop_fullsync(Cluster1, "cluster2"),
    pass.

realtime_test({0,_,Config,_}, SendToCluster3, Mode, [Cluster1, Cluster2, Cluster3]) ->
    print_test(0, Mode, SendToCluster3, Config),
    put_all_objects(Cluster1, 0, {realtime, SendToCluster3}, Mode),
    %% ================================================================== %%

    Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
    Expected2 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
    Expected3 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
    ?assertEqual(true, check_objects("cluster1", Cluster1, Expected1, erlang:now(), 30)),
    ?assertEqual(true, check_objects("cluster2", Cluster2, Expected2, erlang:now(), 30)),
    ?assertEqual(true, check_objects("cluster3", Cluster2, Expected3, erlang:now(), 30)),
    cleanup([Cluster1, Cluster2, Cluster3]),
    pass;
realtime_test({TestNumber, Status, Config, ExpectedList}, SendToCluster3, Mode, C=[Cluster1, Cluster2, Cluster3]) ->
    print_test(TestNumber, Mode, SendToCluster3, Config),
    write_terms("/tmp/config1", Config),
    set_object_filtering(Cluster1, Status, "/tmp/config1", "realtime"),
    Config2 =
        case SendToCluster3 of
            true -> [{"cluster3", {allow, ['*']}, {block, []}}];
            false -> [{"cluster3", {allow, []}, {block, ['*']}}]
        end,
    write_terms("/tmp/config2", Config2),
    set_object_filtering(Cluster2, enabled, "/tmp/config2", Mode),
    check_object_filtering_config(C),
    %% ================================================================== %%
    put_all_objects(Cluster1, TestNumber, {realtime, SendToCluster3}, Mode),
    %% ================================================================== %%

    Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
    Expected2 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- ExpectedList],
    Expected3 =
        case SendToCluster3 of
            true -> Expected2;
            false-> []
        end,
    ?assertEqual(true, check_objects("cluster1", Cluster1, Expected1, erlang:now(), 30)),
    ?assertEqual(true, check_objects("cluster2", Cluster2, Expected2, erlang:now(), 30)),
    ?assertEqual(true, check_objects("cluster3", Cluster3, Expected3, erlang:now(), 30)),
    cleanup([Cluster1, Cluster2, Cluster3]),
    pass.

%% ================================================================================================================== %%
%%                                        Riak Test Functions                                                         %%
%% ================================================================================================================== %%

all_bkeys() ->
    [{B,K} || B <- ?ALL_BUCKETS_NUMS, K <- ?ALL_KEY_NUMS].

put_all_objects(Cluster, TestNumber, Repl, OFMode) ->
    Node = hd(Cluster),
    {ok, C} = riak:client_connect(Node),
    [C:put(Obj) || Obj <- create_objects(all, TestNumber, {Repl, OFMode})],
    lager:info("Placed all data on cluster1").

delete_all_objects(ClusterName, Cluster) ->
    Node = hd(Cluster),
    {ok, C} = riak:client_connect(Node),
    [C:delete(make_bucket(BN), make_key(KN)) || BN <- ?ALL_BUCKETS_NUMS, KN <- ?ALL_KEY_NUMS],
    lager:info("Deleted all data for ~p", [ClusterName]).


check_objects(ClusterName, Cluster, Expected, Time, Timeout) ->
    check_object_helper(ClusterName, Cluster, Expected, Time, Timeout).
%% TODO make this quicker for []!
check_object_helper(ClusterName, Cluster, Expected, Time, Timeout) ->
    Actual = get_all_objects(Cluster),
    Result = lists:sort(Actual) == lists:sort(Expected),
    case {Result, timer:now_diff(erlang:now(), Time) > Timeout*1000*1000} of
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

create_objects(all, TestNumber, Value) -> [create_single_object(BN, KN, TestNumber, Value) || BN <- ?ALL_BUCKETS_NUMS, KN <- ?ALL_KEY_NUMS].

create_single_object(BN, KN, TestNumber, Value) ->
    riak_object:new(make_bucket(BN), make_key(KN) , make_value(BN, KN, TestNumber, Value), make_dict(KN)).

make_bucket({T,N}) -> {list_to_binary("type-"++T), list_to_binary("bucket-" ++ N)};
make_bucket(N) -> list_to_binary("bucket-" ++ N).

make_key(N) -> list_to_binary("key-" ++ N).

make_value({T,BN}, KN, TestNumber, {fullsync, OFMode}) -> list_to_binary("test-" ++ integer_to_list(TestNumber) ++ " ------ value-"
    ++ T ++ "-" ++ BN ++ "-" ++ KN ++ "-" ++ "fullsync" ++ "-" ++ OFMode);
make_value({T, BN}, KN, TestNumber, {{realtime
    , SendToCluster3}, OFMode}) -> list_to_binary("test-" ++ integer_to_list(TestNumber) ++ " ------ value-"
    ++ T ++ "-" ++ BN ++ "-" ++ KN ++ "-" ++ "realtime" ++ "-" ++ atom_to_list(SendToCluster3) ++ "-" ++ OFMode);
make_value(BN, KN, TestNumber, {fullsync, OFMode}) -> list_to_binary("test-" ++ integer_to_list(TestNumber) ++ " ------ value-"
    ++ BN ++ "-" ++ KN ++ "-" ++ "fullsync" ++ "-" ++ OFMode);
make_value(BN, KN, TestNumber, {{realtime, SendToCluster3}, OFMode}) -> list_to_binary("test-" ++ integer_to_list(TestNumber) ++ " ------ value-"
    ++ BN ++ "-" ++ KN ++ "-" ++ "realtime" ++ "-" ++ atom_to_list(SendToCluster3) ++ "-" ++ OFMode).

make_dict("1") -> dict:new();
make_dict(N) -> dict:from_list([{filter, N}]).


check_object_filtering_config(Clusters) ->
    timer:sleep(200),
    [check_object_filtering_config_helper(C) || C <- Clusters].

check_object_filtering_config_helper(Cluster) ->
    Node = hd(Cluster),
    {status_all_nodes, StatusList} = rpc:call(Node, riak_repl2_object_filter, status_all, []),
    check_configs_equal(Cluster, StatusList).

check_configs_equal(Cluster, StatusList) ->
    S1 = [Status || {_, Status} <- StatusList],
    S2 = lists:usort(S1),
    case length(S2) == 1 of
        true -> ok;
        false ->
            timer:sleep(1000),
            check_object_filtering_config_helper(Cluster)
    end.





make_clusters_helper() ->
    Conf = [
        {riak_repl,
            [
                %% turn off fullsync
                {fullsync_strategy, keylist},
                {fullsync_on_connect, false},
                {fullsync_interval, disabled},
                {max_fssource_node, 8},
                {max_fssink_node, 64},
                {max_fssource_cluster, 64},
                {default_bucket_props, [{n_val, 3}, {allow_mult, false}]},
                {override_capability,
                    [{default_bucket_props_hash, [{use, [consistent, datatype, n_val, allow_mult, last_write_wins]}]}]}
            ]},

        {riak_kv,
            [
                {backend_reap_threshold, 86400},
                {override_capability, [{object_hash_version, [{use, legacy}]}]}

            ]}
    ],
    Nodes = rt:deploy_nodes(8, Conf, [riak_kv, riak_repl]),
    Cluster1 = lists:sublist(Nodes, 1, 3),
    lager:info("Build cluster 1"),
    repl_util:make_cluster(Cluster1),
    Cluster2 = lists:sublist(Nodes, 4, 3),
    lager:info("Build cluster 2"),
    repl_util:make_cluster(Cluster2),
    Cluster3 = lists:sublist(Nodes, 7, 2),
    lager:info("Build cluster 3"),
    repl_util:make_cluster(Cluster3),

    lager:info("waiting for leader to converge on cluster 1"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(Cluster1)),
    lager:info("waiting for leader to converge on cluster 2"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(Cluster2)),
    lager:info("waiting for leader to converge on cluster 2"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(Cluster3)),

    repl_util:name_cluster(hd(Cluster1), "cluster1"),
    rt:wait_until_ring_converged(Cluster1),
    repl_util:name_cluster(hd(Cluster2), "cluster2"),
    rt:wait_until_ring_converged(Cluster2),
    repl_util:name_cluster(hd(Cluster3), "cluster3"),
    rt:wait_until_ring_converged(Cluster3),
    [Cluster1, Cluster2, Cluster3].


connect_clusters({Leader1, C1}, {Leader2, C2}) ->
    {ok, {_IP, Port}} = rpc:call(Leader2, application, get_env, [riak_core, cluster_mgr]),
    lager:info("connect cluster ~p:~p to ~p on port ~p", [C1, Leader1, C2, Port]),
    repl_util:connect_cluster(Leader1, "127.0.0.1", Port),
    ?assertEqual(ok, repl_util:wait_for_connection(Leader1, C2)).

enable_realtime(Cluster, C2Name) ->
    repl_util:enable_realtime(hd(Cluster), C2Name),
    rt:wait_until_ring_converged(Cluster).

disable_realtime(Cluster, C2Name) ->
    repl_util:disable_realtime(hd(Cluster), C2Name),
    rt:wait_until_ring_converged(Cluster).

start_realtime(Cluster, C2Name) ->
    enable_realtime(Cluster, C2Name),
    Node = hd(Cluster),
    lager:info("Starting realtime on: ~p", [Node]),
    rpc:call(Node, riak_repl_console, realtime, [["start", C2Name]]),
    rt:wait_until_ring_converged(Cluster).

stop_realtime(Cluster, C2Name) ->
    Node = hd(Cluster),
    lager:info("Stopping realtime on: ~p", [Node]),
    rpc:call(Node, riak_repl_console, realtime, [["stop", C2Name]]),
    disable_realtime(Cluster, C2Name),
    rt:wait_until_ring_converged(Cluster).

enable_fullsync(Cluster, C2Name) ->
    repl_util:enable_fullsync(hd(Cluster), C2Name),
    rt:wait_until_ring_converged(Cluster).

disable_fullsync(Cluster, C2Name) ->
    repl_util:disable_fullsync(hd(Cluster), C2Name),
    rt:wait_until_ring_converged(Cluster).

start_fullsync(Cluster, C2Name) ->
    enable_fullsync(Cluster, C2Name),
    Node = hd(Cluster),
    lager:info("Starting fullsync on: ~p", [Node]),
    rpc:call(Node, riak_repl_console, fullsync, [["start", C2Name]]),
    rt:wait_until_ring_converged(Cluster).

stop_fullsync(Cluster, C2Name) ->
    Node = hd(Cluster),
    lager:info("Stopping fullsync on: ~p", [Node]),
    rpc:call(Node, riak_repl_console, fullsync, [["stop", C2Name]]),
    disable_fullsync(Cluster, C2Name),
    rt:wait_until_ring_converged(Cluster).

set_object_filtering(Cluster, Status, Config, Mode) ->
    set_object_filtering_status(Cluster, Status),
    set_object_filtering_config(Cluster, Config, Mode).

set_object_filtering_status(Cluster, Status) ->
    case Status of
        enabled ->
            rpc:call(hd(Cluster), riak_repl_console, object_filtering_enable, [[]]);
        disabled ->
            rpc:call(hd(Cluster), riak_repl_console, object_filtering_disable, [[]])
    end.

set_object_filtering_config(Cluster, Config, Mode) ->
    rpc:call(hd(Cluster), riak_repl_console, object_filtering_load_config, [[Mode, Config]]).




%% ================================================================================================================== %%
%%                                        Helper Functions                                                            %%
%% ================================================================================================================== %%

write_terms(Filename, List) ->
    Format = fun(Term) -> io_lib:format("~tp.~n", [Term]) end,
    Text = lists:map(Format, List),
    file:write_file(Filename, Text).

cleanup(Clusters) ->
    ClusterNames = ["cluster1", "cluster2", "cluster3"],
    clear_config(Clusters),
    check_object_filtering_config(Clusters),
    delete_data(Clusters, ClusterNames),
    delete_files(),
    lager:info("Cleanup complete ~n", []),
    check(Clusters, ClusterNames),
    ok.

delete_data([], []) -> ok;
delete_data([Cluster|Rest1], [ClusterName|Rest2]) ->
    delete_all_objects(ClusterName, Cluster),
    delete_data(Rest1, Rest2).

check([], []) -> ?assertEqual(true, true);
check([Cluster|Rest1], [ClusterName|Rest2]) ->
    ?assertEqual(true, check_objects(ClusterName, Cluster, [], erlang:now(), 10)), check(Rest1, Rest2).

delete_files() ->
    file:delete("/tmp/config1"),
    file:delete("/tmp/config2").

clear_config([]) -> ok;
clear_config([Cluster| Rest]) ->
    rpc:call(hd(Cluster), riak_repl_console, object_filtering_clear_config, [["all"]]) ,
    rpc:call(hd(Cluster), riak_repl_console, object_filtering_disable, [[]]),
    clear_config(Rest).

print_objects(ClusterName, Objects) ->
    lager:info("All objects for ~p ~n", [ClusterName]),
    print_objects_helper(Objects),
    lager:info("~n", []).
print_objects_helper([]) -> ok;
print_objects_helper([{Bucket, Key}|Rest]) ->
    lager:info("~p, ~p ~n", [Bucket, Key]),
    print_objects_helper(Rest).



get_config() ->
    [
        {0, disabled, [{"cluster2", {allow, []}, {block,[]}}], []},
        {
            101,
            disabled,
            [{"cluster2", {allow, ['*']}, {block,['*']}}],
            all_bkeys()
        },
        {
            102,
            disabled,
            [{"cluster2", {allow, ['*']}, {block,[]}}],
            all_bkeys()
        },
        {
            103,
            disabled,
            [{"cluster2", {allow, []}, {block,['*']}}],
            all_bkeys()
        },
        {
            104,
            disabled,
            [{"cluster2", {allow, []}, {block,[]}}],
            all_bkeys()
        },
        {
            105,
            enabled,
            [{"cluster2", {allow, ['*']}, {block,['*']}}],
            []
        },
        {
            106,
            enabled,
            [{"cluster2", {allow, ['*']}, {block,[]}}],
            all_bkeys()
        },
        {
            107,
            enabled,
            [{"cluster2", {allow, []}, {block,['*']}}],
            []
        },
        {
            108,
            enabled,
            [{"cluster2", {allow, []}, {block,[]}}],
            []
        },

        %% Tests with multi rules

        {
            109,
            enabled,
            [{"cluster2", {allow, [{bucket,<<"bucket-1">>}, {bucket,<<"bucket-2">>}]}, {block,[]}}],
            [{"1","1"}, {"1","2"}, {"1","3"}, {"2","1"}, {"2","2"}, {"2","3"}]
        },
        {
            110,
            enabled,
            [{"cluster2", {allow, [{bucket,<<"bucket-1">>}, {metadata,{filter, "2"}}]}, {block,[{bucket, <<"bucket-3">>}]}}],
            [{"1","1"}, {"1","2"}, {"1","3"}, {"2","2"}, {{"1", "1"},"2"}, {{"1", "2"},"2"}, {{"1", "3"},"2"}]
        },
        {
            111,
            enabled,
            [{"cluster2", {allow, [{bucket,<<"bucket-1">>}, {metadata,{filter, "2"}}]}, {block,[[{bucket, <<"bucket-1">>}, {metadata, {filter}}]]}}],
            [{"1","1"},{"2","2"},{"3","2"}, {{"1", "1"},"2"}, {{"1", "2"},"2"}, {{"1", "3"},"2"}]
        },
        {
            112,
            enabled,
            [{"cluster2", {allow, [[{bucket, <<"bucket-1">>}, {metadata, {filter}}]]}, {block,[[{bucket, <<"bucket-1">>}, {metadata, {filter, "2"}}]]}}],
            [{"1","3"}]
        }
    ].
get_config(bucket) ->
    Configs =
        [
            {
                200,
                enabled,
                [{"cluster2", {allow, [{bucket, <<"bucket-1">>}]}, {block,[]}}],
                [{"1","1"}, {"1","2"}, {"1","3"}]
            },
            {
                202,
                enabled,
                [{"cluster2", {allow, [{bucket, <<"bucket-4">>}]}, {block,[]}}],
                []
            },
            {
                204,
                enabled,
                [{"cluster2", {allow, [{bucket, {<<"type-1">>, <<"bucket-1">>}}]}, {block,[]}}],
                [{{"1","1"},"1"}, {{"1","1"},"2"}, {{"1","1"},"3"}]
            },
            {
                206,
                enabled,
                [{"cluster2", {allow, [{bucket, {<<"type-2">>, <<"bucket-1">>}}]}, {block,[]}}],
                []
            }
        ],
    lists:sort(Configs ++ lists:map(fun not_rule/1, Configs));
get_config(metadata) ->
    Configs =
        [
            {
                300,
                enabled,
                [{"cluster2", {allow, [{metadata,{filter}}]}, {block,[]}}],
                [{"1","2"}, {"1","3"}, {"2","2"}, {"2","3"}, {"3","2"}, {"3","3"}, {{"1", "1"},"2"}, {{"1", "1"},"3"}, {{"1", "2"},"2"}, {{"1", "2"},"3"}, {{"1", "3"},"2"}, {{"1", "3"},"3"}]
            },
            {
                302,
                enabled,
                [{"cluster2", {allow, [{metadata,{other}}]}, {block,[]}}],
                []
            },
            {
                304,
                enabled,
                [{"cluster2", {allow, [{metadata,{filter, "2"}}]}, {block,[]}}],
                [{"1","2"},{"2","2"}, {"3","2"}, {{"1", "1"},"2"}, {{"1", "2"},"2"}, {{"1", "3"},"2"}]
            },
            {
                306,
                enabled,
                [{"cluster2", {allow, [{metadata,{filter, "4"}}]}, {block,[]}}],
                []
            },
            {
                308,
                enabled,
                [{"cluster2", {allow, [{metadata,{other, "2"}}]}, {block,[]}}],
                []
            }
        ],
    lists:sort(Configs ++ lists:map(fun not_rule/1, Configs));
get_config(lastmod_age) ->
    Configs =
        [
            {
                400,
                enabled,
                [{"cluster2", {allow, [{lastmod_age_less_than, -1000}]}, {block,[]}}],
                []
            },
            {
                402,
                enabled,
                [{"cluster2", {allow, [{lastmod_age_less_than, 1000}]}, {block,[]}}],
                all_bkeys()
            },
            {
                404,
                enabled,
                [{"cluster2", {allow, [{lastmod_age_greater_than, -1000}]}, {block,[]}}],
                all_bkeys()
            },
            {
                406,
                enabled,
                [{"cluster2", {allow, [{lastmod_age_greater_than, 1000}]}, {block,[]}}],
                []
            }
        ],
    lists:sort(Configs ++ lists:map(fun not_rule/1, Configs));
get_config(lastmod) ->
    Configs =
        [
            {
                500,
                enabled,
                [{"cluster2", {allow, [{lastmod_less_than, 0}]}, {block,[]}}],
                []
            },
            {
                502,
                enabled,
                [{"cluster2", {allow, [{lastmod_less_than, 4294967295}]}, {block,[]}}],
                all_bkeys()
            },
            {
                504,
                enabled,
                [{"cluster2", {allow, [{lastmod_greater_than, 0}]}, {block,[]}}],
                all_bkeys()
            },
            {
                506,
                enabled,
                [{"cluster2", {allow, [{lastmod_greater_than, 4294967295}]}, {block,[]}}],
                []
            }
        ],
    lists:sort(Configs ++ lists:map(fun not_rule/1, Configs)).

not_rule(Config) ->
    {N, enabled, [{Name, {allow, [Rule]}, {block, []}}], Expected} = Config,
    {N+1, enabled, [{Name, {allow, [{lnot, Rule}]}, {block, []}}], all_bkeys() -- Expected}.