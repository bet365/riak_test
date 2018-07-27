-module(repl2_object_filtering).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").


confirm() ->
    [ run_fullsync_test(N) || N <- lists:seq(1,1)],
    pass.

run_fullsync_test(N) ->
    print_test(fullsync, N),
    ?assertEqual(pass, fullsync_test(N)).

print_test(Name, Number) ->
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("-------- Test ~p ~p ---------------", [Name, Number]),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------").

%% ================================================================================================================== %%
%%                                        Fullsync Tests                                                              %%
%% ================================================================================================================== %%
fullsync_test(1) ->
    Nodes = make_clusters(2, 5),
    [_Cluster1, _Cluster2] = Nodes,
    pass.
%% ================================================================================================================== %%
%%                                        Riak Test Functions                                                         %%
%% ================================================================================================================== %%

make_clusters(NumberOfClusters, NumberOfNodesPerCluster) ->

    TotalNumberOfNodes = NumberOfClusters * NumberOfNodesPerCluster,
    lager:info("Deploy ~p nodes", [TotalNumberOfNodes]),

    Conf = [
        {riak_repl,
            [
                %% turn off fullsync
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
            ]},
        {riak_kv,
            [
                {backend_reap_threshold, 86400}
            ]}
    ],

    Nodes1 = rt:deploy_nodes(8, Conf, [riak_kv, riak_repl]),
    Nodes2 = rt:deploy_nodes(8, Conf, [riak_kv, riak_repl]),
    [Nodes1, Nodes2].

%%    Leaders = [((L*NumberOfNodesPerCluster)-5)+1 || L <- lists:seq(1,NumberOfClusters)],
%%    [lists:sublist(Nodes, X, NumberOfNodesPerCluster) || X <- Leaders].


%%connect_clusters(LeaderA, LeaderB) ->
%%    {ok, {_IP, Port}} = rpc:call(LeaderB, application, get_env,
%%        [riak_core, cluster_mgr]),
%%    lager:info("connect cluster A:~p to B on port ~p", [LeaderA, Port]),
%%    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port),
%%    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")).
%%
%%enable_rt(LeaderA, ANodes) ->
%%    repl_util:enable_realtime(LeaderA, "B"),
%%    rt:wait_until_ring_converged(ANodes),
%%
%%    repl_util:start_realtime(LeaderA, "B"),
%%    rt:wait_until_ring_converged(ANodes).
%%
%%verify_rt(LeaderA, LeaderB) ->
%%    TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
%%        <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
%%    TestBucket = <<TestHash/binary, "-rt_test_a">>,
%%    First = 101,
%%    Last = 200,
%%
%%    %% Write some objects to the source cluster (A),
%%    rt:log_to_nodes([LeaderA], "write objects (verify_rt)"),
%%    lager:info("Writing ~p keys to ~p, which should RT repl to ~p",
%%        [Last-First+1, LeaderA, LeaderB]),
%%    ?assertEqual([], repl_util:do_write(LeaderA, First, Last, TestBucket, 2)),
%%
%%    %% verify data is replicated to B
%%    rt:log_to_nodes([LeaderA], "read objects (verify_rt)"),
%%    lager:info("Reading ~p keys written from ~p", [Last-First+1, LeaderB]),
%%    ?assertEqual(0, repl_util:wait_for_reads(LeaderB, First, Last, TestBucket, 2)).



%% ================================================================================================================== %%
%%                                        Helper Functions                                                            %%
%% ================================================================================================================== %%
