-module(verify_fullsync_object_hash).

%% API
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").


confirm() ->
    [{A1,A2}, {B1,B2}, {C1,C2}, {D1, D2}] = make_clusters(),

    %% setup clusters with different object hash functions

    %% run tests
    pass = run_test({A1, A2}, "A2"),
    pass = run_test({B1, B2}, "B2"),
    pass = run_test({C1, C2}, "C2"),
    pass = run_test({D1, D2}, "D2").


%% ================================================================================================================== %%
%%                                              Tests                                                                 %%
%% ================================================================================================================== %%

run_test({Node1, Node2}, Node2Name) ->

    %% Turn on realtime replication
    enable_realtime(Node1, Node2Name),
    start_realtime(Node1, Node2Name)

    %% Place 10,000 keys

    %% Turn off realtime replication

    %% Place 5,000 keys

    %% add intercept to capture diff state in an ets table

    %% turn on fullsync

    %% wait until fullsync completes

    %% loop ets table with diff states (8 in total), sum all the missing and differences
    %% should be 1000 missing, 5000 different

    %% clear ets table

    pass.

%% ================================================================================================================== %%
%%                                        Riak Test Functions                                                         %%
%% ================================================================================================================== %%

make_clusters() ->
    Nodes = make_clusters_helper(),
    _ = [rpc:call(N1, erlang, disconnect_node, [N2]) || N1 <- Nodes, N2 <- Nodes],
    A1 = lists:nth(1, Nodes),
    A2 = lists:nth(2, Nodes),
    B1 = lists:nth(3, Nodes),
    B2 = lists:nth(4, Nodes),
    C1 = lists:nth(5, Nodes),
    C2 = lists:nth(6, Nodes),
    D1 = lists:nth(7, Nodes),
    D2 = lists:nth(8, Nodes),
    connect_clusters({A1,"A1"}, {A2, "A2"}),
    connect_clusters({B1,"B1"}, {B2, "B2"}),
    connect_clusters({C1,"C1"}, {C2, "C2"}),
    connect_clusters({D1,"D1"}, {D2, "D2"}),
    [{A1,A2}, {B1,B2}, {C1,C2}, {D1, D2}].



make_clusters_helper() ->
    Conf =
        [
            {riak_core,
                [
                    {default_bucket_props,
                        [
                            {allow_mult,false},
                            {basic_quorum,false},
                            {big_vclock,50},
                            {chash_keyfun,{riak_core_util,chash_std_keyfun}},
                            {dvv_enabled,false},
                            {dw,quorum},
                            {last_write_wins,false},
                            {linkfun,{modfun,riak_kv_wm_link_walker,mapreduce_linkfun}},
                            {n_val,1},
                            {node_confirms,0},
                            {notfound_ok,true},
                            {old_vclock,86400},
                            {postcommit,[]},
                            {pr,0},
                            {precommit,[]},
                            {pw,0},
                            {r,quorum},
                            {repl,true},
                            {rw,quorum},
                            {small_vclock,50},
                            {w,quorum},
                            {write_once,false},
                            {young_vclock,20}
                        ]},

                    {ring_creation_size, 8}
                ]
            },
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
                ]
            },
            {riak_kv,
                [

                    {override_capability, [{object_hash_version, [{use, legacy}]}]}

                ]
            }
        ],
    Nodes = rt:deploy_nodes(8, Conf, [riak_kv, riak_repl]),
    lists:foreach(
        fun(Node) ->
            rt:wait_until_ring_converged(Node),
            rt:wait_until_transfers_complete(Node)
        end, Nodes),
    Nodes.


connect_clusters({Node1, Name1}, {Node2, Name2}) ->
    repl_util:name_cluster(Node1, Name1),
    repl_util:name_cluster(Node2, Name2),
    {ok, {_IP, Port}} = rpc:call(Node2, application, get_env, [riak_core, cluster_mgr]),
    lager:info("connect cluster ~p:~p to ~p on port ~p", [Name1, Node1, Name2, Port]),
    repl_util:connect_cluster(Node1, "127.0.0.1", Port),
    ?assertEqual(ok, repl_util:wait_for_connection(Node1, Name2)).

enable_realtime(Node, C2Name) ->
    repl_util:enable_realtime(Node, C2Name),
    rt:wait_until_ring_converged(Node).

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



wait_until_fullsync_complete(Node, C2Name, Retries, Expected) ->
    check_fullsync_completed(Node, C2Name, Retries, Expected, 50).

check_fullsync_completed(_, _, 0, _, _) ->
    failed;
check_fullsync_completed(Node, C2Name, Retries, Expected, Sleep) ->
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
            ok;
        _ ->
            stop_fullsync([Node], C2Name),
            timer:sleep(2000),
            start_fullsync([Node], C2Name),
            check_fullsync_completed(Node, C2Name, Retries-1, Expected, Sleep*2)
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




%% ================================================================================================================== %%
%%                                        Helper Functions                                                            %%
%% ================================================================================================================== %%







