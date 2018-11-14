-module(repl2_object_filtering).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(ALL_BUCKETS_NUMS, ["1","2", "3"]).
-define(ALL_KEY_NUMS, ["1","2","3"]).

confirm() ->
    delete_files(),
    [Cluster1, Cluster2, Cluster3] = make_clusters(),
    connect_clusters({hd(Cluster1),"cluster1"}, {hd(Cluster2), "cluster2"}),
    connect_clusters({hd(Cluster2),"cluster2"}, {hd(Cluster3), "cluster3"}),
    %% Single tests - these are run as {TestNumeber, Status, Config, ExpectedObjects}
    Test1 =
        [
            {0, disabled, [{"cluster2", {allow, []}, {block,[]}}], []},
            {
                1,
                disabled,
                [{"cluster2", {allow, ['*']}, {block,['*']}}],
                [{"1","1"}, {"1","2"}, {"1","3"}, {"2","1"}, {"2","2"}, {"2","3"}, {"3","1"}, {"3","2"}, {"3","3"}]
            },
            {
                2,
                disabled,
                [{"cluster2", {allow, ['*']}, {block,[]}}],
                [{"1","1"}, {"1","2"}, {"1","3"}, {"2","1"}, {"2","2"}, {"2","3"}, {"3","1"}, {"3","2"}, {"3","3"}]
            },
            {
                3,
                disabled,
                [{"cluster2", {allow, []}, {block,['*']}}],
                [{"1","1"}, {"1","2"}, {"1","3"}, {"2","1"}, {"2","2"}, {"2","3"}, {"3","1"}, {"3","2"}, {"3","3"}]
            },
            {
                4,
                disabled,
                [{"cluster2", {allow, []}, {block,[]}}],
                [{"1","1"}, {"1","2"}, {"1","3"}, {"2","1"}, {"2","2"}, {"2","3"}, {"3","1"}, {"3","2"}, {"3","3"}]
            },
            {
                5,
                enabled,
                [{"cluster2", {allow, ['*']}, {block,['*']}}],
                []
            },
            {
                6,
                enabled,
                [{"cluster2", {allow, ['*']}, {block,[]}}],
                [{"1","1"}, {"1","2"}, {"1","3"}, {"2","1"}, {"2","2"}, {"2","3"}, {"3","1"}, {"3","2"}, {"3","3"}]
            },
            {
                7,
                enabled,
                [{"cluster2", {allow, []}, {block,['*']}}],
                []
            },
            {
                8,
                enabled,
                [{"cluster2", {allow, []}, {block,[]}}],
                []
            }],

    %% Double tests - 2 tests are run on each of these.
    %% allow is changed to {allow, ['*']}
    %% block is changed to {block, Allowed}
    %% expected is now the inverse of what was expected in the first test
    Test2 =
    [
        {
            9, %% bucket - match
            enabled,
            [{"cluster2", {allow, [{bucket, <<"bucket-1">>}]}, {block,[]}}],
            [{"1","1"}, {"1","2"}, {"1","3"}]
        },
        {
            11, %% bucket - not match
            enabled,
            [{"cluster2", {allow, [{bucket, <<"bucket-4">>}]}, {block,[]}}],
            []
        },
        {
            13, %% bucket - all
            enabled,
            [{"cluster2", {allow, [{bucket,all}]}, {block,[]}}],
            [{"1","1"}, {"1","2"}, {"1","3"}, {"2","1"}, {"2","2"}, {"2","3"}, {"3","1"}, {"3","2"}, {"3","3"}]
        },
        {
            15, %% not_bucket - match
            enabled,
            [{"cluster2", {allow, [{not_bucket, <<"bucket-1">>}]}, {block,[]}}],
            [{"2","1"}, {"2","2"}, {"2","3"}, {"3","1"}, {"3","2"}, {"3","3"}]
        },
        {
            17, %% not_bucket - not match
            enabled,
            [{"cluster2", {allow, [{not_bucket, <<"bucket-4">>}]}, {block,[]}}],
            [{"1","1"}, {"1","2"}, {"1","3"}, {"2","1"}, {"2","2"}, {"2","3"}, {"3","1"}, {"3","2"}, {"3","3"}]
        },
        {
            19, %% not_bucket - all
            enabled,
            [{"cluster2", {allow, [{not_bucket, all}]}, {block,[]}}],
            []
        },
        {
            21, %% metadata - match
            enabled,
            [{"cluster2", {allow, [{metadata,{filter, "2"}}]}, {block,[]}}],
            [{"1","2"},{"2","2"}, {"3","2"}]
        },
        {
            23, %% metadata - not match 1
            enabled,
            [{"cluster2", {allow, [{metadata,{filter, "4"}}]}, {block,[]}}],
            []
        },
        {
            25, %% metadata - not match 2
            enabled,
            [{"cluster2", {allow, [{metadata,{other, "2"}}]}, {block,[]}}],
            []
        },
        {
            27, %% metadata - not match 3
            enabled,
            [{"cluster2", {allow, [{metadata,{other, all}}]}, {block,[]}}],
            []
        },
        {
            29, %% metadata - all
            enabled,
            [{"cluster2", {allow, [{metadata,{filter, all}}]}, {block,[]}}],
            [{"1","2"}, {"1","3"}, {"2","2"}, {"2","3"}, {"3","2"}, {"3","3"}]
        },
        {
            31, %% not_metadata - match
            enabled,
            [{"cluster2", {allow, [{not_metadata,{filter, "2"}}]}, {block,[]}}],
            [{"1","1"}, {"1","3"}, {"2","1"}, {"2","3"}, {"3","1"}, {"3","3"}]
        },
        {
            33, %% not_metadata - not match 1
            enabled,
            [{"cluster2", {allow, [{not_metadata,{filter, "4"}}]}, {block,[]}}],
            [{"1","1"}, {"1","2"}, {"1","3"}, {"2","1"}, {"2","2"}, {"2","3"}, {"3","1"}, {"3","2"}, {"3","3"}]
        },
        {
            35, %% not_metadata - not match 2
            enabled,
            [{"cluster2", {allow, [{not_metadata,{other, "2"}}]}, {block,[]}}],
            [{"1","1"}, {"1","2"}, {"1","3"}, {"2","1"}, {"2","2"}, {"2","3"}, {"3","1"}, {"3","2"}, {"3","3"}]
        },
        {
            37, %% not_metadata - not match 3
            enabled,
            [{"cluster2", {allow, [{not_metadata,{other, all}}]}, {block,[]}}],
            [{"1","1"}, {"1","2"}, {"1","3"}, {"2","1"}, {"2","2"}, {"2","3"}, {"3","1"}, {"3","2"}, {"3","3"}]
        },
        {
            39, %% not_metadata - all
            enabled,
            [{"cluster2", {allow, [{not_metadata,{filter, all}}]}, {block,[]}}],
            [{"1","1"}, {"2","1"}, {"3","1"}]
        }
    ],

    Test3 =
    [
        {
            41,
            enabled,
            [{"cluster2", {allow, [{bucket,<<"bucket-1">>}, {bucket,<<"bucket-2">>}]}, {block,[]}}],
            [{"1","1"}, {"1","2"}, {"1","3"}, {"2","1"}, {"2","2"}, {"2","3"}]
        },
        {
            42,
            enabled,
            [{"cluster2", {allow, [{bucket,<<"bucket-1">>}, {metadata,{filter, "2"}}]}, {block,[{bucket, <<"bucket-3">>}]}}],
            [{"1","1"}, {"1","2"}, {"1","3"},{"2","2"}]
        },
        {
            43,
            enabled,
            [{"cluster2", {allow, [{bucket,<<"bucket-1">>}, {metadata,{filter, "2"}}]}, {block,[[{bucket, <<"bucket-1">>}, {metadata, {filter, all}}]]}}],
            [{"1","1"},{"2","2"},{"3","2"}]
        },
        {
            44,
            enabled,
            [{"cluster2", {allow, [[{bucket, <<"bucket-1">>}, {metadata, {filter, all}}]]}, {block,[[{bucket, <<"bucket-1">>}, {metadata, {filter, "2"}}]]}}],
            [{"1","3"}]
        },
        {
            45,
            enabled,
            [{"cluster2", {allow, [[{bucket, <<"bucket-1">>}, {metadata, {filter, all}}], {bucket, all}]}, {block,[[{bucket, <<"bucket-1">>}, {metadata, {filter, "2"}}]]}}],
            [{"1","1"}, {"1","3"}, {"2","1"}, {"2","2"}, {"2","3"}, {"3","1"}, {"3","2"}, {"3","3"}]
        }
    ],

    Tests = [Test1, Test2, Test3],
    Clusters = [Cluster1, Cluster2, Cluster3],
    run_all_tests_realtime(Tests, Clusters),
    run_all_tests_fullsync(Tests, Clusters),
    pass.



run_all_tests_realtime([Test1, Test2, Test3], [Cluster1, Cluster2, Cluster3]) ->
    start_realtime(Cluster1, "cluster2"),
    start_realtime(Cluster2, "cluster3"),
    [run_test(realtime, Test, [Cluster1, Cluster2, Cluster3]) || Test <- Test1],
    [run_test2(realtime, Test, [Cluster1, Cluster2, Cluster3]) || Test <- Test2],
    [run_test(realtime, Test, [Cluster1, Cluster2, Cluster3]) || Test <- Test3],
    stop_realtime(Cluster1, "cluster2"),
    stop_realtime(Cluster2, "cluster3").

run_all_tests_fullsync([Test1, Test2, Test3], [Cluster1, Cluster2, Cluster3]) ->
    start_fullsync(Cluster1, "cluster2"),
    [run_test(realtime, Test, [Cluster1, Cluster2, Cluster3]) || Test <- Test1],
    [run_test2(realtime, Test, [Cluster1, Cluster2, Cluster3]) || Test <- Test2],
    [run_test(realtime, Test, [Cluster1, Cluster2, Cluster3]) || Test <- Test3],
    stop_fullsync(Cluster1, "cluster2").

run_test2(ReplMode, Test1={N,Status,[{Name, {allow, Allowed}, {block, []}}],Expected}, Clusters)->
    run_test(ReplMode, Test1, Clusters),
    Expected2 =
        [{"1","1"}, {"1","2"}, {"1","3"},{"2","1"}, {"2","2"}, {"2","3"},{"3","1"}, {"3","2"}, {"3","3"}] -- Expected,
    Test2 = {N+1, Status, [{Name, {allow, ['*']}, {block, Allowed}}], Expected2},
    run_test(ReplMode, Test2, Clusters).

run_test(fullsync, Test={N,_,_,_}, Clusters) ->
    print_test(fullsync_ofmode_repl, N),
    ?assertEqual(pass, fullsync_test(Test, "repl", Clusters)),
    print_test(fullsync_ofmode_fullsync, N),
    ?assertEqual(pass, fullsync_test(Test, "fullsync", Clusters));
run_test(realtime, Test={N,_,_,_}, Clusters) ->
    print_test(realtime_a_ofmode_repl, N),
    ?assertEqual(pass, realtime_test(Test, true, "repl", Clusters)),
    print_test(realtime_a_ofmode_realtime, N),
    ?assertEqual(pass, realtime_test(Test, true, "realtime", Clusters)),
    print_test(realtime_b_ofmode_repl, N),
    ?assertEqual(pass, realtime_test(Test, false, "repl", Clusters)),
    print_test(realtime_b_ofmode_realtime, N),
    ?assertEqual(pass, realtime_test(Test, false, "realtime", Clusters)).

print_test(Name, Number) ->
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("-------- Test ~p ~p ---------------", [Name, Number]),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------").

%% ================================================================================================================== %%
%%                                              Tests                                                                 %%
%% ================================================================================================================== %%
fullsync_test({0,_,_,_}, _, [Cluster1, Cluster2, Cluster3]) ->
    put_all_objects(Cluster1, 0),
    %% ================================================================== %%
    List=
        [
            {"1","1"}, {"1","2"}, {"1","3"},
            {"2","1"}, {"2","2"}, {"2","3"},
            {"3","1"}, {"3","2"}, {"3","3"}
        ],
    Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- List],
    Expected2 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- List],
    ?assertEqual(true, check_objects("cluster1", Cluster1, Expected1, erlang:now(), 30)),
    ?assertEqual(true, check_objects("cluster2", Cluster2, Expected2, erlang:now(), 30)),
    cleanup([Cluster1, Cluster2, Cluster3]),
    pass;
fullsync_test({TestNumber, Status, Config, ExpectedList}, Mode, [Cluster1, Cluster2, Cluster3]) ->
    write_terms("/tmp/config1", Config),
    set_object_filtering(Cluster1, Status, "/tmp/config1", Mode),
    %% ================================================================== %%
    put_all_objects(Cluster1, TestNumber),
    %% ================================================================== %%
    List1 =
        [
            {"1","1"}, {"1","2"}, {"1","3"},
            {"2","1"}, {"2","2"}, {"2","3"},
            {"3","1"}, {"3","2"}, {"3","3"}
        ],
    Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- List1],
    Expected2 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- ExpectedList],
    ?assertEqual(true, check_objects("cluster1", Cluster1, Expected1, erlang:now(), 30)),
    ?assertEqual(true, check_objects("cluster2", Cluster2, Expected2, erlang:now(), 30)),
    cleanup([Cluster1, Cluster2, Cluster3]),
    pass.

realtime_test({0,_,_,_}, _, _,[Cluster1, Cluster2, Cluster3]) ->
    put_all_objects(Cluster1, 0),
    %% ================================================================== %%
    List=
        [
            {"1","1"}, {"1","2"}, {"1","3"},
            {"2","1"}, {"2","2"}, {"2","3"},
            {"3","1"}, {"3","2"}, {"3","3"}
        ],
    Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- List],
    Expected2 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- List],
    Expected3 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- List],
    ?assertEqual(true, check_objects("cluster1", Cluster1, Expected1, erlang:now(), 30)),
    ?assertEqual(true, check_objects("cluster2", Cluster2, Expected2, erlang:now(), 30)),
    ?assertEqual(true, check_objects("cluster3", Cluster2, Expected3, erlang:now(), 120)),
    cleanup([Cluster1, Cluster2, Cluster3]),
    pass;
realtime_test({TestNumber, Status, Config, ExpectedList}, SendToCluster3, Mode, [Cluster1, Cluster2, Cluster3]) ->
    write_terms("/tmp/config1", Config),
    set_object_filtering(Cluster1, Status, "/tmp/config1", "realtime"),
    Config2 =
        case SendToCluster3 of
            true -> [{"cluster3", {allow, ['*']}, {block, []}}];
            false -> [{"cluster3", {allow, []}, {block, ['*']}}]
        end,
    write_terms("/tmp/config2", Config2),
    set_object_filtering(Cluster2, enabled, "/tmp/config2", Mode),
    %% ================================================================== %%
    put_all_objects(Cluster1, TestNumber),
    %% ================================================================== %%
    List1 =
        [
            {"1","1"}, {"1","2"}, {"1","3"},
            {"2","1"}, {"2","2"}, {"2","3"},
            {"3","1"}, {"3","2"}, {"3","3"}
        ],
    Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- List1],
    Expected2 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- ExpectedList],
    Expected3 =
        case SendToCluster3 of
            true -> Expected2;
            false-> []
        end,
    ?assertEqual(true, check_objects("cluster1", Cluster1, Expected1, erlang:now(), 30)),
    ?assertEqual(true, check_objects("cluster2", Cluster2, Expected2, erlang:now(), 30)),
    ?assertEqual(true, check_objects("cluster3", Cluster3, Expected3, erlang:now(), 120)),
    cleanup([Cluster1, Cluster2, Cluster3]),
    pass.

%% ================================================================================================================== %%
%%                                        Riak Test Functions                                                         %%
%% ================================================================================================================== %%

put_all_objects(Cluster, TestNumber) ->
    Node = hd(Cluster),
    {ok, C} = riak:client_connect(Node),
    [C:put(Obj) || Obj <- create_objects(all, TestNumber)],
    lager:info("Placed all data on cluster1").

delete_all_objects(ClusterName, Cluster) ->
    Node = hd(Cluster),
    {ok, C} = riak:client_connect(Node),
    [C:delete(make_bucket(BN), make_key(KN)) || BN <- ?ALL_BUCKETS_NUMS, KN <- ?ALL_KEY_NUMS],
    lager:info("Deleted all data for ~p", [ClusterName]).


check_objects(ClusterName, Cluster, Expected, Time, Timeout) ->
    check_object_helper(ClusterName, Cluster, Expected, Time, Timeout).

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

create_objects(all, TestNumber) -> [create_single_object(BN, KN, TestNumber) || BN <- ?ALL_BUCKETS_NUMS, KN <- ?ALL_KEY_NUMS];
create_objects({bucket, BN}, TestNumber) -> [create_single_object(BN, KN, TestNumber) || KN <- ?ALL_KEY_NUMS];
create_objects({metadata, KN}, TestNumber) -> [create_single_object(BN, KN, TestNumber) || BN <- ?ALL_BUCKETS_NUMS].

create_single_object(BN, KN, TestNumber) ->
    riak_object:new(make_bucket(BN), make_key(KN) , make_value(BN, KN, TestNumber), make_dict(KN)).

make_bucket(N) -> list_to_binary("bucket-" ++ N).
make_key(N) -> list_to_binary("key-" ++ N).
make_value(BN, KN, TestNumber) -> list_to_binary("test-" ++ integer_to_list(TestNumber) ++ " ------ value-" ++ BN ++ "-" ++ KN).
make_dict("1") -> dict:new();
make_dict(N) -> dict:from_list([{filter, N}]).





make_clusters() ->
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
                {default_bucket_props, [{n_val, 3}, {allow_mult, false}]}
            ]},

        {riak_kv,
            [
                {backend_reap_threshold, 86400}
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
    delete_data(Clusters, ClusterNames),
    clear_config(Clusters),
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
    rpc:call(hd(Cluster), riak_repl_console, object_filtering_clear_config, [[]]) ,
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