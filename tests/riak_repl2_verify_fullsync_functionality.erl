-module(riak_repl2_verify_fullsync_functionality).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [run_test(Test) || Test <- lists:seq(2,2)],
    pass.

run_test(Test) ->
    print_test(Test),
    ?assertEqual(pass, test(Test)).

print_test(Number) ->
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("------------ Test ~p ------------------", [Number]),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------").



%% ================================================================================================================== %%
%%                                        Fullsync Tests                                                              %%
%% ================================================================================================================== %%
%% 1) Same Key, Same Hashes
test(1) ->
    [Cluster1, Cluster2] = make_clusters(),
    connect_clusters({hd(Cluster1),"cluster1"}, {hd(Cluster2), "cluster2"}),
    A = hd(Cluster1),
    B = hd(Cluster2),
    {ok, C1} = riak:client_connect(A),
    {ok, C2} = riak:client_connect(B),
    timer:sleep(30000),

    % enable realtime
    start_realtime(Cluster1, "cluster2"),
    lager:info("Starting realtime replication Cluster1 -> Cluster2 ~n", []),
    timer:sleep(20000),

    % place a 500 Keys on cluster1
    [C1:put(riak_object:new(<<"bucket">>, <<X:32>>, <<"value-1">>)) || X <- lists:seq(1, 500)],
    lager:info("Placing 500 objects onto Cluster1 ~n", []),
    timer:sleep(30000),

    % verify its on cluster2
    RTList = [C2:get(<<"bucket">>, <<X:32>>) || X <- lists:seq(1, 500)],
    [?assertEqual(ok, Ans1) || {Ans1, _} <- RTList],
    [?assertEqual(<<"value-1">>, riak_object:get_value(Obj)) || {_, Obj} <- RTList],
    lager:info("Verified that the object reached Cluster2 ~n", []),

    % turn realtime off
    stop_realtime(Cluster1, "cluster2"),
    lager:info("Stopping realtime replication Cluster1 -> Cluster2 ~n", []),
    timer:sleep(10000),

    % turn on fullsync
    start_fullsync(Cluster1, "cluster2"),
    lager:info("Starting fullsync Cluster1 -> Cluster2 ~n", []),
    timer:sleep(30000),

    % check that the other cluster has the data
    L1 = [C2:get(<<"bucket">>, <<X:32>>) || X <- lists:seq(1,500)],
    [?assertEqual(ok, Answer) || {Answer, _} <- L1],
    [?assertEqual(<<"value-1">>, riak_object:get_value(Obj)) || {_, Obj} <- L1],

    stop_fullsync(Cluster1, "cluster2"),
    rt:clean_cluster(Cluster1),
    rt:clean_cluster(Cluster2),
    pass;
%% 2) Same Key, Different Hashes
test(2) ->
    [Cluster1, Cluster2] = make_clusters(),
    connect_clusters({hd(Cluster1),"cluster1"}, {hd(Cluster2), "cluster2"}),
    A = hd(Cluster1),
    B = hd(Cluster2),
    {ok, C1} = riak:client_connect(A),
    {ok, C2} = riak:client_connect(B),
    timer:sleep(30000),

    % enable realtime
    start_realtime(Cluster1, "cluster2"),
    lager:info("Starting realtime replication Cluster1 -> Cluster2 ~n", []),
    timer:sleep(20000),

    % place a 500 Keys on cluster1
    [C1:put(riak_object:new(<<"bucket">>, <<X:32>>, <<"value-1">>)) || X <- lists:seq(1, 500)],
    lager:info("Placing 500 objects onto Cluster1 ~n", []),
    timer:sleep(30000),

    % verify its on cluster2
    RTList = [C2:get(<<"bucket">>, <<X:32>>) || X <- lists:seq(1, 500)],
    [?assertEqual(ok, Ans1) || {Ans1, _} <- RTList],
    [?assertEqual(<<"value-1">>, riak_object:get_value(Obj)) || {_, Obj} <- RTList],
    lager:info("Verified that the object reached Cluster2 ~n", []),

    % turn realtime off
    stop_realtime(Cluster1, "cluster2"),
    lager:info("Stopping realtime replication Cluster1 -> Cluster2 ~n", []),
    timer:sleep(10000),

    % place 500 objects of a higher key value onto cluster1
    [C1:put(riak_object:new(<<"bucket">>, <<X:32>>, <<"value-2">>)) || X <- lists:seq(1, 500)],
    lager:info("Updating 500 objects on Cluster1 ~n", []),
    timer:sleep(30000),

    % turn on fullsync
    start_fullsync(Cluster1, "cluster2"),
    lager:info("Starting fullsync Cluster1 -> Cluster2 ~n", []),
    timer:sleep(30000),

    % check that the other cluster has the data
    L1 = [C2:get(<<"bucket">>, <<X:32>>) || X <- lists:seq(1,500)],
    [?assertEqual(ok, Answer) || {Answer, _} <- L1],
    [?assertEqual(<<"value-2">>, riak_object:get_value(Obj)) || {_, Obj} <- L1],

    stop_fullsync(Cluster1, "cluster2"),
    rt:clean_cluster(Cluster1),
    rt:clean_cluster(Cluster2),
    pass;
%% 3) Diff Key, Remote < Local
test(3) ->
    [Cluster1, Cluster2] = make_clusters(),
    connect_clusters({hd(Cluster1),"cluster1"}, {hd(Cluster2), "cluster2"}),
    A = hd(Cluster1),
    B = hd(Cluster2),
    {ok, C1} = riak:client_connect(A),
    {ok, C2} = riak:client_connect(B),
    timer:sleep(30000),

    % enable realtime
    start_realtime(Cluster1, "cluster2"),
    lager:info("Starting realtime replication Cluster1 -> Cluster2 ~n", []),
    timer:sleep(20000),

    % place a 100 Keys on cluster1
    [C1:put(riak_object:new(<<"bucket">>, <<X:32>>, <<"value-1">>)) || X <- lists:seq(1, 100)],
    lager:info("Placing 100 objects onto Cluster1 ~n", []),
    timer:sleep(30000),

    % verify its on cluster2
    RTList = [C2:get(<<"bucket">>, <<X:32>>) || X <- lists:seq(1, 100)],
    [?assertEqual(ok, Ans1) || {Ans1, _} <- RTList],
    [?assertEqual(<<"value-1">>, riak_object:get_value(Obj)) || {_, Obj} <- RTList],
    lager:info("Verified that the object reached Cluster2 ~n", []),

    % turn realtime off
    stop_realtime(Cluster1, "cluster2"),
    lager:info("Stopping realtime replication Cluster1 -> Cluster2 ~n", []),
    timer:sleep(10000),

    % delete them from cluster 1
    [C1:delete(<<"bucket">>, <<X:32>>) || X <- lists:seq(1, 100)],
    %% wait for them to expire
    timer:sleep(15000),

    % place 300 objects of a higher key value onto cluster1
    [C1:put(riak_object:new(<<"bucket">>, <<X:32>>, <<"value-1">>)) || X <- lists:seq(101, 600)],
    lager:info("Placing 500 objects on Cluster1 ~n", []),
    timer:sleep(30000),

    % turn on fullsync
    start_fullsync(Cluster1, "cluster2"),
    lager:info("Starting fullsync Cluster1 -> Cluster2 ~n", []),
    timer:sleep(30000),

    % check that the other cluster has the data
    L1 = [C2:get(<<"bucket">>, <<X:32>>) || X <- lists:seq(1,600)],
    [?assertEqual(ok, Answer) || {Answer, _} <- L1],
    [?assertEqual(<<"value-1">>, riak_object:get_value(Obj)) || {_, Obj} <- L1],

    stop_fullsync(Cluster1, "cluster2"),
    rt:clean_cluster(Cluster1),
    rt:clean_cluster(Cluster2),
    pass;
%% 4) Diff Key, Remote > Local
test(4) ->
    [Cluster1, Cluster2] = make_clusters(),
    connect_clusters({hd(Cluster1),"cluster1"}, {hd(Cluster2), "cluster2"}),
    A = hd(Cluster1),
    B = hd(Cluster2),
    {ok, C1} = riak:client_connect(A),
    {ok, C2} = riak:client_connect(B),
    timer:sleep(30000),

    % enable realtime
    start_realtime(Cluster1, "cluster2"),
    lager:info("Starting realtime replication Cluster1 -> Cluster2 ~n", []),
    timer:sleep(20000),

    % place a 100 Keys on cluster1
    [C1:put(riak_object:new(<<"bucket">>, <<X:32>>, <<"value-1">>)) || X <- lists:seq(10000, 10100)],
    lager:info("Placing 100 objects onto Cluster1 ~n", []),
    timer:sleep(30000),

    % verify its on cluster2
    RTList = [C2:get(<<"bucket">>, <<X:32>>) || X <- lists:seq(10000, 10100)],
    [?assertEqual(ok, Ans1) || {Ans1, _} <- RTList],
    [?assertEqual(<<"value-1">>, riak_object:get_value(Obj)) || {_, Obj} <- RTList],
    lager:info("Verified that the object reached Cluster2 ~n", []),

    % turn realtime off
    stop_realtime(Cluster1, "cluster2"),
    lager:info("Stopping realtime replication Cluster1 -> Cluster2 ~n", []),
    timer:sleep(10000),

    % place 300 objects of a higher key value onto cluster1
    [C1:put(riak_object:new(<<"bucket">>, <<X:32>>, <<"value-1">>)) || X <- lists:seq(1, 500)],
    lager:info("Placing 500 objects on Cluster1 ~n", []),
    timer:sleep(30000),

    % turn on fullsync
    start_fullsync(Cluster1, "cluster2"),
    lager:info("Starting fullsync Cluster1 -> Cluster2 ~n", []),
    timer:sleep(30000),

    % check that the other cluster has the data
    L1 = [C2:get(<<"bucket">>, <<X:32>>) || X <- lists:seq(1,500)],
    L2 = [C2:get(<<"bucket">>, <<X:32>>) || X <- lists:seq(10000,10100)],
    [?assertEqual(ok, Answer) || {Answer, _} <- L1],
    [?assertEqual(ok, Answer) || {Answer, _} <- L2],
    [?assertEqual(<<"value-1">>, riak_object:get_value(Obj)) || {_, Obj} <- L1],
    [?assertEqual(<<"value-1">>, riak_object:get_value(Obj)) || {_, Obj} <- L2],

    stop_fullsync(Cluster1, "cluster2"),
    rt:clean_cluster(Cluster1),
    rt:clean_cluster(Cluster2),
    pass.





%% ================================================================================================================== %%
%%                                       Helper Functions                                                             %%
%% ================================================================================================================== %%
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
                {backend_reap_threshold, 3}
            ]}
    ],
    Nodes = rt:deploy_nodes(8, Conf, [riak_kv, riak_repl]),
    Cluster1 = lists:sublist(Nodes, 1, 4),
    lager:info("Build cluster 1"),
    repl_util:make_cluster(Cluster1),
    Cluster2 = lists:sublist(Nodes, 5, 4),
    lager:info("Build cluster 2"),
    repl_util:make_cluster(Cluster2),

    lager:info("waiting for leader to converge on cluster 1"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(Cluster1)),
    lager:info("waiting for leader to converge on cluster 2"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(Cluster2)),

    repl_util:name_cluster(hd(Cluster1), "cluster1"),
    rt:wait_until_ring_converged(Cluster1),
    repl_util:name_cluster(hd(Cluster2), "cluster2"),
    rt:wait_until_ring_converged(Cluster2),

    [Cluster1, Cluster2].


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