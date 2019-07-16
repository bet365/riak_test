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
    DoubleTests1 = get_config(bucket) ++ get_config(metadata) ++ get_config(user_metadata) ++ get_config(lastmod_age) ++ get_config(lastmod),
    DoubleTests2 = DoubleTests1 ++ lists:map(fun not_rule_2/1, DoubleTests1),

    Tests = {SingleTests, DoubleTests2},

    Flag = all,
    case Flag of
        all ->
            C1 = make_clusters(),
            run_all_tests_fullsync(Tests, C1),
            destroy_clusters(C1),
            C2 = make_clusters(),
            run_all_tests_realtime(Tests, C2),
            destroy_clusters(C2);
        realtime ->
            run_all_tests_realtime(Tests, make_clusters());
        fullsync ->
            run_all_tests_fullsync(Tests, make_clusters())
    end,
    pass.

make_clusters() ->
    delete_files(),
    [Cluster1, Cluster2, Cluster3] = make_clusters_helper(),
    connect_clusters({hd(Cluster1),"cluster1"}, {hd(Cluster2), "cluster2"}),
    connect_clusters({hd(Cluster2),"cluster2"}, {hd(Cluster3), "cluster3"}),
    rt:create_and_activate_bucket_type(hd(Cluster1), <<"type-1">>, [{allow_mult, false}, {dvv_enabled, false}]),
    rt:create_and_activate_bucket_type(hd(Cluster2), <<"type-1">>, [{allow_mult, false}, {dvv_enabled, false}]),
    rt:create_and_activate_bucket_type(hd(Cluster3), <<"type-1">>, [{allow_mult, false}, {dvv_enabled, false}]),
    rt:wait_until_bucket_type_status(<<"type-1">>, active, [hd(Cluster1)]),
    rt:wait_until_bucket_type_status(<<"type-1">>, active, [hd(Cluster2)]),
    rt:wait_until_bucket_type_status(<<"type-1">>, active, [hd(Cluster3)]),
    [Cluster1, Cluster2, Cluster3].

destroy_clusters(Clusters) ->
    Nodes = lists:flatten(Clusters),
    rt:clean_cluster(Nodes).

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
    start_fullsync(Cluster1, "cluster2"),
    [run_test_single(fullsync, Test, [Cluster1, Cluster2, Cluster3]) || Test <- SingleTests],
    [run_test_double(fullsync, Test, [Cluster1, Cluster2, Cluster3]) || Test <- DoubleTests],
    stop_fullsync(Cluster1, "cluster2").


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

print_test(Number, Mode, Config, ExpectedList) ->
    ReplMode = fullsync,
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("Test: ~p ~n", [Number]),
    lager:info("Repl Mode: ~p ~n", [ReplMode]),
    lager:info("Object Filtering Mode: ~p ~n", [Mode]),
    lager:info("Config: ~p ~n", [Config]),
    lager:info("Expected: ~p ~n", [lists:sort(ExpectedList)]),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------").

print_test(Number, Mode, SendToCluster3, Config, ExpectedList) ->
    ReplMode = realtime,
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("Test: ~p ~n", [Number]),
    lager:info("Repl Mode: ~p ~n", [ReplMode]),
    lager:info("Object Filtering Mode: ~p ~n", [Mode]),
    lager:info("Send To Cluster 3: ~p ~n", [SendToCluster3]),
    lager:info("Config: ~p ~n", [Config]),
    lager:info("Expected: ~p ~n", [lists:sort(ExpectedList)]),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------").

%% ================================================================================================================== %%
%%                                              Tests                                                                 %%
%% ================================================================================================================== %%
fullsync_test({0,_,Config, ExpectedList}, Mode, [Cluster1, Cluster2, Cluster3]) ->
    print_test(0, Mode, Config, ExpectedList),
    put_all_objects(Cluster1, 0, fullsync, Mode),
    Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
    ?assertEqual(true, check_objects("cluster1", Cluster1, Expected1, erlang:now(), 240)),
    %% ================================================================== %%
    Expected2 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
    wait_for_fullsync_and_check(Cluster1, "cluster2", Expected2, Cluster2),
    ?assertEqual(true, check_objects("cluster2", Cluster2, Expected2, erlang:now(), 240)),
%%    stop_fullsync(Cluster1, "cluster2"),
    %% ================================================================== %%
    cleanup([Cluster1, Cluster2, Cluster3], ["cluster1", "cluster2", "cluster3"]),
    pass;
fullsync_test({TestNumber, Status, Config, ExpectedList}, Mode, C=[Cluster1, Cluster2, Cluster3]) ->
    print_test(TestNumber, Mode, Config, ExpectedList),
    write_terms("/tmp/config1", Config),
    set_object_filtering(Cluster1, Status, "/tmp/config1", Mode),
    check_object_filtering_config(C),
    %% ================================================================== %%
    put_all_objects(Cluster1, TestNumber, fullsync, Mode),
    Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
    ?assertEqual(true, check_objects("cluster1", Cluster1, Expected1, erlang:now(), 240)),
    %% ================================================================== %%
    Expected2 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- ExpectedList],
    wait_for_fullsync_and_check(Cluster1, "cluster2", Expected2, Cluster2),
    ?assertEqual(true, check_objects("cluster2", Cluster2, Expected2, erlang:now(), 240)),
%%    stop_fullsync(Cluster1, "cluster2"),
    %% ================================================================== %%
    cleanup([Cluster1, Cluster2, Cluster3], ["cluster1", "cluster2", "cluster3"]),
    pass.

realtime_test({0,_,Config, ExpectedList}, SendToCluster3, Mode, [Cluster1, Cluster2, Cluster3]) ->
    print_test(0, Mode, SendToCluster3, Config, ExpectedList),
    put_all_objects(Cluster1, 0, {realtime, SendToCluster3}, Mode),
    %% ================================================================== %%

    Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
    Expected2 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
    Expected3 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- all_bkeys()],
    ?assertEqual(true, check_objects("cluster1", Cluster1, Expected1, erlang:now(), 240)),
    ?assertEqual(true, check_objects("cluster2", Cluster2, Expected2, erlang:now(), 240)),
    ?assertEqual(true, check_objects("cluster3", Cluster3, Expected3, erlang:now(), 240)),
    cleanup([Cluster1, Cluster2, Cluster3], ["cluster1", "cluster2", "cluster3"]),
    pass;
realtime_test({TestNumber, Status, Config, ExpectedList}, SendToCluster3, Mode, C=[Cluster1, Cluster2, Cluster3]) ->
    print_test(TestNumber, Mode, SendToCluster3, Config, ExpectedList),
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
    ?assertEqual(true, check_objects("cluster1", Cluster1, Expected1, erlang:now(), 240)),
    ?assertEqual(true, check_objects("cluster2", Cluster2, Expected2, erlang:now(), 240)),
    ?assertEqual(true, check_objects("cluster3", Cluster3, Expected3, erlang:now(), 240)),
    cleanup([Cluster1, Cluster2, Cluster3], ["cluster1", "cluster2", "cluster3"]),
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
make_dict(N) ->
    MD_USERMETA = <<"X-Riak-Meta">>,
    NN = list_to_integer(N),
    UserMetaData = [{<<"filter">>, <<NN:32>>}],
    dict:from_list([{filter, N}, {MD_USERMETA, UserMetaData}]).


check_object_filtering_config(Clusters) ->
    timer:sleep(200),
    [check_object_filtering_config_helper(C) || C <- Clusters].

check_object_filtering_config_helper(Cluster) ->
    Node = hd(Cluster),
    {StatusAllNodes, _} = rpc:call(Node, riak_core_util, rpc_every_member, [riak_repl2_object_filter_console, get_status, [], 10000]),
    Result = [R ||{status_single_node, R} <- StatusAllNodes],
    check_configs_equal(Cluster, lists:sort(Result)).

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

                {override_capability, [{object_hash_version, [{use, legacy}]}]}

            ]}
    ],
    Nodes = rt:deploy_nodes(8, Conf, [riak_kv, riak_repl]),

    Cluster1 = lists:sublist(Nodes, 1, 3),
    Cluster2 = lists:sublist(Nodes, 4, 3),
    Cluster3 = lists:sublist(Nodes, 7, 2),

    [rpc:call(N1, erlang, disconnect_node, [N2]) || N1 <- Cluster1, N2 <- Cluster2 ++ Cluster3],
    [rpc:call(N1, erlang, disconnect_node, [N2]) || N1 <- Cluster2, N2 <- Cluster1 ++ Cluster3],
    [rpc:call(N1, erlang, disconnect_node, [N2]) || N1 <- Cluster3, N2 <- Cluster1 ++ Cluster2],

    lager:info("Build cluster 1"),
    repl_util:make_cluster(Cluster1),
    lager:info("Build cluster 2"),
    repl_util:make_cluster(Cluster2),
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

    rt:wait_until_transfers_complete(Cluster1),
    rt:wait_until_transfers_complete(Cluster2),
    rt:wait_until_transfers_complete(Cluster3),

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

start_fullsync(Cluster, C2Name) ->
    Node = hd(Cluster),
    lager:info("Starting fullsync on: ~p", [Node]),
    repl_util:enable_fullsync(Node, C2Name),
    rpc:call(Node, riak_repl_console, fullsync, [["start", C2Name]]).

wait_for_fullsync_and_check(Cluster, C2Name, Expected, Cluster2) ->
    wait_until_fullsync_complete(hd(Cluster), C2Name, 11, Expected, Cluster2).


wait_until_fullsync_complete(Node, C2Name, Retries, Expected, Cluster2) ->
    check_fullsync_completed(Node, C2Name, Retries, Expected, Cluster2, 50).

check_fullsync_completed(_, _, 0, _, _,_) ->
    failed;
check_fullsync_completed(Node, C2Name, Retries, Expected, Cluster2, Sleep) ->
    Status0 = rpc:call(Node, riak_repl_console, status, [quiet]),
    [{C2Name, List}] = proplists:get_value(fullsync_coordinator, Status0, []),
    Count = proplists:get_value(fullsyncs_completed, List, 0),
    rpc:call(Node, riak_repl_console, fullsync, [["start", C2Name]]),
    lager:info("fullsync completed count: ~p", [Count]),
    %% sleep because of the old bug where stats will crash if you call it too
    %% soon after starting a fullsync
    timer:sleep(round(Sleep)),

    case rt:wait_until(make_fullsync_wait_fun(Node, Count+1), 100, 1000) of
        ok ->
            check_fullsync_objects(Node, C2Name, Retries, Expected, Cluster2, Sleep*2);
        _ ->
            stop_fullsync([Node], C2Name),
            timer:sleep(2000),
            start_fullsync([Node], C2Name),
            check_fullsync_completed(Node, C2Name, Retries-1, Expected, Cluster2, Sleep*2)
    end.

check_fullsync_objects(Node, C2Name, Retries, Expected, Cluster2, Sleep) ->
    Actual = get_all_objects(Cluster2),
    case lists:sort(Actual) == lists:sort(Expected) of
        true ->
            lager:info("Fullsync on ~p complete", [Node]),
            ok;
        false ->
            check_fullsync_completed(Node, C2Name, Retries-1, Expected, Cluster2, Sleep*2)
    end.

make_fullsync_wait_fun(Node, Count) ->
    fun() ->
        Status = rpc:call(Node, riak_repl_console, status, [quiet]),
        case Status of
            {badrpc, _} ->
                false;
            _ ->
                case proplists:get_value(fullsyncs_completed, Status) of
                    C when C >= Count ->
                        true;
                    _ ->
                        false
                end
        end
    end.

stop_fullsync(Cluster, C2Name) ->
    Node = hd(Cluster),
    lager:info("Stopping fullsync on: ~p", [Node]),
    repl_util:disable_fullsync(Node, C2Name),
    repl_util:stop_fullsync(Node, C2Name).

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

cleanup(Clusters, ClusterNames) ->
    clear_config(Clusters),
    check_object_filtering_config(Clusters),
    delete_data(Clusters, ClusterNames),
    delete_files(),
    timer:sleep(2000),
    lager:info("Cleanup complete ~n", []),
    check(Clusters, ClusterNames),
    ok.

delete_data([], []) -> ok;
delete_data([Cluster|Rest1], [ClusterName|Rest2]) ->
    delete_all_objects(ClusterName, Cluster),
    delete_data(Rest1, Rest2).

check([], []) -> ?assertEqual(true, true);
check(Clusters = [Cluster|Rest1], ClusterNames =[ClusterName|Rest2]) ->
    case check_objects(ClusterName, Cluster, [], erlang:now(), 10) of
        true -> check(Rest1, Rest2);
        false -> cleanup(Clusters, ClusterNames)
    end.

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
        {
            0,
            disabled,
            [{"cluster2", {allow, []}, {block,[]}}],
            all_bkeys()
        },

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
                [{"cluster2", {allow, [{lastmod_age_less_than, -100000}]}, {block,[]}}],
                []
            },
            {
                402,
                enabled,
                [{"cluster2", {allow, [{lastmod_age_less_than, 100000}]}, {block,[]}}],
                all_bkeys()
            },
            {
                404,
                enabled,
                [{"cluster2", {allow, [{lastmod_age_greater_than, -100000}]}, {block,[]}}],
                all_bkeys()
            },
            {
                406,
                enabled,
                [{"cluster2", {allow, [{lastmod_age_greater_than, 100000}]}, {block,[]}}],
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
    lists:sort(Configs ++ lists:map(fun not_rule/1, Configs));
get_config(user_metadata) ->
    Configs =
        [
            {
                600,
                enabled,
                [{"cluster2", {allow, [{user_metadata,{<<"filter">>}}]}, {block,[]}}],
                [{"1","2"}, {"1","3"}, {"2","2"}, {"2","3"}, {"3","2"}, {"3","3"}, {{"1", "1"},"2"}, {{"1", "1"},"3"}, {{"1", "2"},"2"}, {{"1", "2"},"3"}, {{"1", "3"},"2"}, {{"1", "3"},"3"}]
            },
            {
                602,
                enabled,
                [{"cluster2", {allow, [{user_metadata,{<<"other">>}}]}, {block,[]}}],
                []
            },
            {
                604,
                enabled,
                [{"cluster2", {allow, [{user_metadata,{<<"filter">>, <<2:32>>}}]}, {block,[]}}],
                [{"1","2"},{"2","2"}, {"3","2"}, {{"1", "1"},"2"}, {{"1", "2"},"2"}, {{"1", "3"},"2"}]
            },
            {
                606,
                enabled,
                [{"cluster2", {allow, [{user_metadata,{<<"filter">>, <<4:32>>}}]}, {block,[]}}],
                []
            },
            {
                608,
                enabled,
                [{"cluster2", {allow, [{user_metadata,{<<"other">>, <<2:32>>}}]}, {block,[]}}],
                []
            }
        ],
    lists:sort(Configs ++ lists:map(fun not_rule/1, Configs)).

not_rule(Config) ->
    {N, enabled, [{Name, {allow, [Rule]}, {block, []}}], Expected} = Config,
    {N+1, enabled, [{Name, {allow, [{lnot, Rule}]}, {block, []}}], all_bkeys() -- Expected}.

not_rule_2(Config) ->
    {N, enabled, [{Name, {allow, [Rule]}, {block, []}}], Expected} = Config,
    {N+1, enabled, [{Name, {allow, [{lnot, [Rule, {lnot, {bucket, <<"does not exist">>}}]}]}, {block, []}}], all_bkeys() -- Expected}.
