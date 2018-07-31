-module(repl2_object_filtering).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(ALL_BUCKETS_NUMS, ["1","2","3","4", "5"]).
-define(ALL_KEY_NUMS, ["1","2","3"]).

confirm() ->
    delete_files(),

    [Cluster1, Cluster2, Cluster3] = make_clusters(),
    connect_clusters({hd(Cluster1),"Cluster1"}, {hd(Cluster2), "Cluster2"}),
    connect_clusters({hd(Cluster1),"Cluster1"}, {hd(Cluster3), "Cluster3"}),
    enable_fullsync(Cluster1, "Cluster2"),
    enable_fullsync(Cluster1, "Cluster3"),

    [ run_fullsync_test(N, [Cluster1, Cluster2, Cluster3]) || N <- lists:seq(2,2)],
    pass.

run_fullsync_test(N, Clusters) ->
    print_test(fullsync, N),
    ?assertEqual(pass, fullsync_test(N, Clusters)).

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
fullsync_test(1, [Cluster1, Cluster2, Cluster3]) ->
    Status1 = disabled,
    Config1 =
    [
        {{bucket, <<"bucket-2">>},      {whitelist, ["Cluster2"]}},
        {{bucket, <<"bucket-3">>},      {whitelist, ["Cluster3"]}},
        {{bucket, <<"bucket-4">>},      {blacklist, ["Cluster2"]}},
        {{bucket, <<"bucket-5">>},      {blacklist, ["Cluster3"]}}
    ],

    Status2 = disabled,
    Config2 =
        [

        ],

    Status3 = disabled,
    Config3 =
        [

        ],

    write_terms("/tmp/config1", Config1),
    write_terms("/tmp/config2", Config2),
    write_terms("/tmp/config3", Config3),
    set_object_filtering(Cluster1, Status1, "/tmp/config1"),
    set_object_filtering(Cluster2, Status2, "/tmp/config2"),
    set_object_filtering(Cluster3, Status3, "/tmp/config3"),
    %% ================================================================== %%

    put_all_objects(Cluster1),

    %% ================================================================== %%
    start_fullsync(Cluster1, "Cluster2"),
    start_fullsync(Cluster1, "Cluster3"),
    timer:sleep(30000),
    %% ================================================================== %%
    List1 =
        [
            {"1","1"}, {"1","2"}, {"1","3"},
            {"2","1"}, {"2","2"}, {"2","3"},
            {"3","1"}, {"3","2"}, {"3","3"},
            {"4","1"}, {"4","2"}, {"4","3"},
            {"5","1"}, {"5","2"}, {"5","3"}
        ],
    Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- List1],
    lager:info("Cluster 1 ~n", []),
    ?assertEqual(true, check_objects(Cluster1, Expected1)),
    lager:info("~n", []),

    List2 =
        [
            {"1","1"}, {"1","2"}, {"1","3"},
            {"2","1"}, {"2","2"}, {"2","3"},
            {"3","1"}, {"3","2"}, {"3","3"},
            {"4","1"}, {"4","2"}, {"4","3"},
            {"5","1"}, {"5","2"}, {"5","3"}
        ],

    Expected2 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- List2],
    lager:info("Cluster 2 ~n", []),
    ?assertEqual(true, check_objects(Cluster2, Expected2)),
    lager:info("~n", []),
    List3 =
        [
            {"1","1"}, {"1","2"}, {"1","3"},
            {"2","1"}, {"2","2"}, {"2","3"},
            {"3","1"}, {"3","2"}, {"3","3"},
            {"4","1"}, {"4","2"}, {"4","3"},
            {"5","1"}, {"5","2"}, {"5","3"}
        ],
    Expected3 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- List3],
    lager:info("Cluster 3 ~n", []),
    ?assertEqual(true, check_objects(Cluster3, Expected3)),
    lager:info("~n", []),
    cleanup([Cluster1, Cluster2, Cluster3]),
    timer:sleep(30000),
    pass;

fullsync_test(2, [Cluster1, Cluster2, Cluster3]) ->
    Status1 = enabled,
    Config1 =
        [
            {{bucket, <<"bucket-2">>},      {whitelist, ["Cluster2"]}},
            {{bucket, <<"bucket-3">>},      {whitelist, ["Cluster3"]}},
            {{bucket, <<"bucket-4">>},      {blacklist, ["Cluster2"]}},
            {{bucket, <<"bucket-5">>},      {blacklist, ["Cluster3"]}}
        ],

    Status2 = disabled,
    Config2 =
        [

        ],

    Status3 = disabled,
    Config3 =
        [

        ],

    write_terms("/tmp/config1", Config1),
    write_terms("/tmp/config2", Config2),
    write_terms("/tmp/config3", Config3),
    set_object_filtering(Cluster1, Status1, "/tmp/config1"),
    set_object_filtering(Cluster2, Status2, "/tmp/config2"),
    set_object_filtering(Cluster3, Status3, "/tmp/config3"),
    %% ================================================================== %%

    put_all_objects(Cluster1),

    %% ================================================================== %%
    start_fullsync(Cluster1, "Cluster2"),
    start_fullsync(Cluster1, "Cluster3"),
    timer:sleep(30000),
    %% ================================================================== %%
    List1 =
        [
            {"1","1"}, {"1","2"}, {"1","3"},
            {"2","1"}, {"2","2"}, {"2","3"},
            {"3","1"}, {"3","2"}, {"3","3"},
            {"4","1"}, {"4","2"}, {"4","3"},
            {"5","1"}, {"5","2"}, {"5","3"}
        ],
    Expected1 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- List1],
    lager:info("Cluster 1 ~n", []),
    ?assertEqual(true, check_objects(Cluster1, Expected1)),
    lager:info("~n", []),

    List2 =
        [
            {"1","1"}, {"1","2"}, {"1","3"},
            {"2","1"}, {"2","2"}, {"2","3"},
            {"5","1"}, {"5","2"}, {"5","3"}
        ],

    Expected2 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- List2],
    lager:info("Cluster 2 ~n", []),
    ?assertEqual(true, check_objects(Cluster2, Expected2)),
    lager:info("~n", []),
    List3 =
        [
            {"1","1"}, {"1","2"}, {"1","3"},
            {"3","1"}, {"3","2"}, {"3","3"},
            {"4","1"}, {"4","2"}, {"4","3"}
        ],
    Expected3 = [{make_bucket(BN), make_key(KN)} || {BN, KN} <- List3],
    lager:info("Cluster 3 ~n", []),
    ?assertEqual(true, check_objects(Cluster3, Expected3)),
    lager:info("~n", []),
    cleanup([Cluster1, Cluster2, Cluster3]),
    timer:sleep(30000),
    pass.
%% ================================================================================================================== %%
%%                                        Riak Test Functions                                                         %%
%% ================================================================================================================== %%

put_all_objects(Cluster) ->
    Node = hd(Cluster),
    {ok, C} = riak:client_connect(Node),
    [C:put(Obj) || Obj <- create_objects(all)],
    lager:info("Placed all data on Cluster1").

delete_all_objects(Cluster) ->
    Node = hd(Cluster),
    {ok, C} = riak:client_connect(Node),
    [C:delete(make_bucket(BN), make_key(KN)) || BN <- ?ALL_BUCKETS_NUMS, KN <- ?ALL_KEY_NUMS],
    lager:info("Deleted all data on Cluster1").


check_objects(Cluster, Expected) ->
    Actual = get_all_objects(Cluster),
    print_objects(Actual),
    lists:sort(Actual) == lists:sort(Expected).

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

create_objects(all) -> [create_single_object(BN, KN) || BN <- ?ALL_BUCKETS_NUMS, KN <- ?ALL_KEY_NUMS];
create_objects({bucket, BN}) -> [create_single_object(BN, KN) || KN <- ?ALL_KEY_NUMS];
create_objects({metadata, KN}) -> [create_single_object(BN, KN) || BN <- ?ALL_BUCKETS_NUMS].

create_single_object(BN, KN) ->
    riak_object:new(make_bucket(BN), make_key(KN) , make_value(BN, KN), make_dict(KN)).

make_bucket(N) -> list_to_binary("bucket-" ++ N).
make_key(N) -> list_to_binary("key-" ++ N).
make_value(BN, KN) -> list_to_binary("value-" ++ BN ++ "-" ++ KN).
make_dict("1") -> dict:new();
make_dict(N) -> dict:from_list([{filter, N}]).





make_clusters() ->
    Conf = [
        {riak_repl,
            [
                %% turn off fullsync
                {fullsync_on_connect, false},
                {fullsync_interval, disabled},
                {max_fssource_node, 8},
                {max_fssink_node, 64},
                {max_fssource_cluster, 64}
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

start_fullsync(Cluster, C2Name) ->
    Node = hd(Cluster),
    lager:info("Starting fullsync on: ~p", [Node]),
    rpc:call(Node, riak_repl_console, fullsync, [["start", C2Name]]),
    lager:info("Cluster for fullsync start: ~p", [Cluster]),
    rt:wait_until_ring_converged(Cluster).

set_object_filtering(Cluster, Status, Config) ->
    set_object_filtering_status(Cluster, Status),
    set_object_filtering_config(Cluster, Config).

set_object_filtering_status(Cluster, Status) ->
    case Status of
        enabled ->
            rpc:call(hd(Cluster), riak_repl_console, object_filtering_enable, [[]]);
        disabled ->
            rpc:call(hd(Cluster), riak_repl_console, object_filtering_disable, [[]])
    end.

set_object_filtering_config(Cluster, Config) ->
    rpc:call(hd(Cluster), riak_repl_console, object_filtering_load_config, [[Config]]).




%% ================================================================================================================== %%
%%                                        Helper Functions                                                            %%
%% ================================================================================================================== %%

write_terms(Filename, List) ->
    Format = fun(Term) -> io_lib:format("~tp.~n", [Term]) end,
    Text = lists:map(Format, List),
    file:write_file(Filename, Text).

cleanup(Clusters) ->
    delete_all_objects(hd(Clusters)),
    clear_config(Clusters),
    delete_files(),
    start_fullsync(hd(Clusters), "Cluster2"),
    start_fullsync(hd(Clusters), "Cluster3"),
    ok.

delete_files() ->
    file:delete("/tmp/config1"),
    file:delete("/tmp/config2"),
    file:delete("/tmp/config3").

clear_config([]) -> ok;
clear_config([Cluster| Rest]) ->
    rpc:call(hd(Cluster), riak_repl_console, object_filtering_clear_config, [[]]) ,
    rpc:call(hd(Cluster), riak_repl_console, object_filtering_disable, [[]]),
    clear_config(Rest).

print_objects([]) -> ok;
print_objects([{Bucket, Key}|Rest]) ->
    lager:info("~p, ~p ~n", [Bucket, Key]),
    print_objects(Rest).