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
    [Cluster1, Cluster2, _Cluster3] = make_clusters(),
    connect_clusters({hd(Cluster1),"Cluster1"}, {hd(Cluster2), "Cluster2"}),
    enable_fullsync(Cluster1, "Cluster2"),
    put_all_objects(Cluster1),
    false.
%% ================================================================================================================== %%
%%                                        Riak Test Functions                                                         %%
%% ================================================================================================================== %%

put_all_objects(Cluster) ->
    Node = hd(Cluster),
    {ok, C} = riak:client_connect(Node),
    [C:put(Obj) || Obj <- create_objects(all)],
    lager:info("Placed all data on Cluster1").

%% TODO: create the three functions below from the bottom function
create_objects(all) ->
    Dict1 = dict:new(),
    Dict2 = dict:from_list([{filter, 1}]),
    Dict3 = dict:from_list([{filter, 2}]),
    [
        riak_object:new(<<"bucket-1">>, <<"key-1">>, <<"value-1-1">>, Dict1),
        riak_object:new(<<"bucket-1">>, <<"key-2">>, <<"value-1-2">>, Dict2),
        riak_object:new(<<"bucket-1">>, <<"key-3">>, <<"value-1-3">>, Dict3),
        riak_object:new(<<"bucket-2">>, <<"key-1">>, <<"value-2-1">>, Dict1),
        riak_object:new(<<"bucket-2">>, <<"key-2">>, <<"value-2-2">>, Dict2),
        riak_object:new(<<"bucket-2">>, <<"key-3">>, <<"value-2-3">>, Dict3),
        riak_object:new(<<"bucket-3">>, <<"key-1">>, <<"value-3-1">>, Dict1),
        riak_object:new(<<"bucket-3">>, <<"key-2">>, <<"value-3-2">>, Dict2),
        riak_object:new(<<"bucket-3">>, <<"key-3">>, <<"value-3-3">>, Dict3),
        riak_object:new(<<"bucket-4">>, <<"key-1">>, <<"value-4-1">>, Dict1),
        riak_object:new(<<"bucket-4">>, <<"key-2">>, <<"value-4-2">>, Dict2),
        riak_object:new(<<"bucket-4">>, <<"key-3">>, <<"value-4-3">>, Dict3)
    ].
create_objects(bucket, Bucket) ->
    Dict1 = dict:new(),
    Dict2 = dict:from_list([{filter, 1}]),
    Dict3 = dict:from_list([{filter, 2}]),
    [
        riak_object:new(<<"Bucket-", Bucket>>, <<"key-1">>, <<"value-", Bucket, "-1">>, Dict1),
        riak_object:new(<<"Bucket-", Bucket>>, <<"key-2">>, <<"value-", Bucket, "-2">>, Dict2),
        riak_object:new(<<"Bucket-", Bucket>>, <<"key-3">>, <<"value-", Bucket, "-3">>, Dict3)
    ];
create_objects(metadata, {Dict, N}) ->
    [
        riak_object:new(<<"bucket-1">>, <<"key-", N>>, <<"value-1-1">>, Dict),
        riak_object:new(<<"bucket-2">>, <<"key-", N>>, <<"value-2-1">>, Dict),
        riak_object:new(<<"bucket-3">>, <<"key-", N>>, <<"value-3-1">>, Dict),
        riak_object:new(<<"bucket-4">>, <<"key-", N>>, <<"value-4-1">>, Dict)
    ];
create_objects(metadata_and_bucket, {Bucket, {Dict, N}}) ->
    riak_object:new(<<"Bucket-", Bucket>>, <<"key-", N>>, <<"value-", Bucket, "-", N>>, Dict).





make_clusters() ->
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
    lager:info("waiting for leader to converge on cluster 3"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(Cluster3)),

    repl_util:name_cluster(hd(Cluster1), "Cluster1"),
    rt:wait_until_ring_converged(Cluster1),
    repl_util:name_cluster(hd(Cluster2), "Cluster2"),
    rt:wait_until_ring_converged(Cluster2),
    repl_util:name_cluster(hd(Cluster3), "Cluster3"),
    rt:wait_until_ring_converged(Cluster3),
    [Cluster1, Cluster2, Cluster3].


connect_clusters({Leader1, C1}, {Leader2, C2}) ->
    {ok, {_IP, Port}} = rpc:call(Leader2, application, get_env, [riak_core, cluster_mgr]),
    lager:info("connect cluster ~p:~p to ~p on port ~p", [C1, Leader1, C2, Port]),
    repl_util:connect_cluster(Leader1, "127.0.0.1", Port),
    ?assertEqual(ok, repl_util:wait_for_connection(Leader1, C2)).

%%enable_rt(Cluster, C2Name) ->
%%    repl_util:enable_realtime(hd(Cluster), C2Name),
%%    rt:wait_until_ring_converged(Cluster),
%%
%%    repl_util:start_realtime(hd(Cluster), C2Name),
%%    rt:wait_until_ring_converged(Cluster).

enable_fullsync(Cluster, C2Name) ->
    repl_util:enable_fullsync(hd(Cluster), C2Name),
    rt:wait_until_ring_converged(Cluster).

%%start_fullsync(Cluster, C2Name) ->
%%    repl_util:start_and_wait_until_fullsync_complete(hd(Cluster), C2Name),
%%    rt:wait_until_ring_converged(Cluster).




%% ================================================================================================================== %%
%%                                        Helper Functions                                                            %%
%% ================================================================================================================== %%
