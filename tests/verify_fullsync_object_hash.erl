-module(verify_fullsync_object_hash).

%% API
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-record(diff_state, {fsm,
    ref,
    preflist,
    count = 0,
    replies = 0,
    diff_hash = 0,
    missing = 0,
    need_vclocks = true,
    errors = [],
    filter_buckets = false,
    shared_buckets = []}).



confirm() ->
    [{A1,A2}, {B1,B2}, {C1,C2}, {D1,D2}] = make_clusters(),

    %% run tests
    io:format(" ---------- Test 1 ----------"),
    pass = run_test({A1, 0}, {A2, 0}, "A2", 10000),
    pass = run_test({B1, 0}, {B2, 1}, "B2", 10000),
    pass = run_test({C1, 1}, {C2, 0}, "C2", 10000),
    pass = run_test({D1, 1}, {D2, 1}, "D2", 0).


%% ================================================================================================================== %%
%%                                              Tests                                                                 %%
%% ================================================================================================================== %%

run_test({Node1, V1}, {Node2, V2}, Node2Name, ExpectedDiff) ->

    %% Turn on realtime replication
    enable_realtime(Node1, Node2Name),
    start_realtime(Node1, Node2Name),

    %% Place and Delete 50,000 keys
    {ok, Client} = riak:client_connect(Node1),
    io:format("Placing and Deleting 10,000 keys"),
    lists:foreach(
                    fun(X) ->
                        Client:put(riak_object:new(<<"test-1">>, <<X:32>>, <<"value-1">>)),
                        Client:delete(<<"test-1">>, <<X:32>>)
                    end,
        lists:seq(1,10000)),

    %% set fullsync object hash version
    rpc:call(Node1, application, set_env, [riak_repl, fullsync_object_hash_version, V1]),
    rpc:call(Node2, application, set_env, [riak_repl, fullsync_object_hash_version, V2]),

%%    %% set merges on on Node 2
%%    rpc:call(Node2, application, set_env, [bitcask, merge_window, always], 10000),
%%
%%    %% wait for some merges to take place
%%    io:format("Wait for merges"),
%%    timer:sleep(45000),
%%
%%    %% stop the merges
%%    rpc:call(Node2, application, set_env, [bitcask, merge_window, never], 10000),

    %% add intercept to capture diff state in an ets table
    rt_intercept:add(Node1,
        {riak_repl_fullsync_helper,
            [
                {{diff_keys, 3}, diff_keys_intercept}
            ]}
    ),

    %% turn on fullsync
    start_fullsync(Node1, Node2Name),

    %% wait until fullsync completes
    wait_until_fullsync_complete(Node1, Node2Name, 3),
    timer:sleep(5000),

    %% Get Difference Count
    {ok, Keys} = Client:list_keys(<<"diffstate">>),
    AllDiffState =
        lists:foldl(
            fun(Key, TotalDiffState = #diff_state{diff_hash = B1, missing = C1}) ->
                {ok, Obj} = Client:get(<<"diffstate">>, Key),
                DiffState = binary_to_term(riak_object:get_value(Obj)),
                #diff_state{diff_hash = B2, missing = C2} = DiffState,
                TotalDiffState#diff_state{diff_hash = B1+B2, missing = C1+C2}
            end, #diff_state{diff_hash = 0, missing = 0}, Keys),

    stop_realtime(Node1, Node2Name),
    stop_fullsync(Node1, Node2Name),

    io:format("DiffState: ~p", [AllDiffState]),

    ?assertEqual(ExpectedDiff, AllDiffState#diff_state.diff_hash),
    ?assertEqual(0, AllDiffState#diff_state.missing),
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
                    {fullsync_interval, 60},
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
                    {backend_reap_threshold, 86400},
                    {override_capability, [{object_hash_version, [{use, legacy}]}]},
                    {bitcask_merge_check_interval, 1000},
                    {bitcask_merge_check_jitter, 0}

                ]
            },
            {bitcask,
                [
                    {merge_window, never},
                    {max_file_size, 100000},
                    {dead_bytes_threshold, 40000},
                    {dead_bytes_merge_trigger, 25000}
                ]
            }
        ],
    Nodes = rt:deploy_nodes(8, Conf, [riak_kv, riak_repl]),
    rt:wait_until_ring_converged(Nodes),
    rt:wait_until_transfers_complete(Nodes),
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
    rt:wait_until_ring_converged([Node]).

disable_realtime(Node, C2Name) ->
    repl_util:disable_realtime(Node, C2Name),
    rt:wait_until_ring_converged([Node]).

start_realtime(Node, C2Name) ->
    enable_realtime(Node, C2Name),
    lager:info("Starting realtime on: ~p", [Node]),
    rpc:call(Node, riak_repl_console, realtime, [["start", C2Name]]),
    rt:wait_until_ring_converged([Node]).

stop_realtime(Node, C2Name) ->
    lager:info("Stopping realtime on: ~p", [Node]),
    rpc:call(Node, riak_repl_console, realtime, [["stop", C2Name]]),
    disable_realtime(Node, C2Name),
    rt:wait_until_ring_converged([Node]).

start_fullsync(Node, C2Name) ->
    lager:info("Starting fullsync on: ~p", [Node]),
    repl_util:enable_fullsync(Node, C2Name),
    rpc:call(Node, riak_repl_console, fullsync, [["start", C2Name]]).



wait_until_fullsync_complete(Node, C2Name, Retries) ->
    check_fullsync_completed(Node, C2Name, Retries, 50).

check_fullsync_completed(_, _, 0, _) ->
    failed;
check_fullsync_completed(Node, C2Name, Retries, Sleep) ->
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
            check_fullsync_completed(Node, C2Name, Retries-1, Sleep*2)
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

stop_fullsync(Node, C2Name) ->
    lager:info("Stopping fullsync on: ~p", [Node]),
    repl_util:disable_fullsync(Node, C2Name),
    repl_util:stop_fullsync(Node, C2Name).




%% ================================================================================================================== %%
%%                                        Helper Functions                                                            %%
%% ================================================================================================================== %%







