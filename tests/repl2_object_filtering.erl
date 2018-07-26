-module(repl2_object_filtering).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").


confirm() ->
    %% run fullsync tests
    [ run_fullsync_test(N) || N <- lists:seq(1,1)],

    %% run realtime tests
%%    [ run_realtime_test(N) || N <- lists(1,1)],
    pass.

run_fullsync_test(N) ->
    print_test(fullsync, N),
    ?assertEqual(pass, fullsync_test(N)).

%%run_realtime_test(N) ->
%%    print_test(realtime, N),
%%    ?assertEqual(pass, realtime_test(N)).


print_test(TestType, N) ->
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("--------------- Test ~p  ~p ---------------", [TestType, N]),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------"),
    lager:info("---------------------------------------").



%% ================================================================================================================== %%
%%                                         Fullsync Tests                                                             %%
%% ================================================================================================================== %%

fullsync_test(1) ->
    pass.

%% ================================================================================================================== %%
%%                                         Helper Fucntions                                                           %%
%% ================================================================================================================== %%

make_cluster(N, ClusterSize) ->
    NumberOfNodes = N*ClusterSize,
    lager:info("Deploy ~p nodes", [NumberOfNodes]),
    Conf = [
        {riak_core,
            [
                {ring_creation_size, 64},
                {default_bucket_props, [{n_val, 3}, {w,3}, {dw,3}, {r,3}, {allow_mult, false}]}
            ]
        },

        {riak_kv,
            [
                {backend_reap_threshold, 86400}
            ]
        },
        {riak_repl,
            [
                %% turn off fullsync
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
            ]}],
    Nodes = rt:deploy_nodes(NumberOfNodes, Conf, [riak_kv, riak_repl]),
    [make_cluster_helper(Nodes, ClusterNumber, ClusterSize) || ClusterNumber <- lists:seq(1,N)].

make_cluster_helper(Nodes, ClusterNumber, ClusterSize) ->
    N = (ClusterNumber*ClusterSize) +1,
    ClusterNodes = lists:sublist(Nodes, N, ClusterSize),
    repl_util:make_cluster(ClusterNodes),
    lager:info("waiting for leader to converge on cluster ~p", [ClusterNumber]),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(lists:nth(1, Nodes))),
    ClusterNodes.


connect_clusters({LeaderA, ClusterAName}, {LeaderB, ClusterBName}) ->
    {ok, {_IP, Port}} = rpc:call(LeaderB, application, get_env, [riak_core, cluster_mgr]),
    lager:info("connect cluster ~p:~p to ~p on port ~p", [ClusterAName, LeaderA, ClusterBName, Port]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, ClusterBName)).