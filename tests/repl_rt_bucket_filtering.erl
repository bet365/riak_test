-module(repl_rt_bucket_filtering).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(SINKA_BUCKET, <<"sinkA-bkt">>).
-define(SINKB_BUCKET, <<"sinkB-bkt">>).


setup() ->
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    {SourceLeader, SinkLeaderA, SinkLeaderB, _, _, _} = ClusterNodes = make_clusters(),

    connect_clusters(SourceLeader, SinkLeaderA, "SinkA"),
    connect_clusters(SourceLeader, SinkLeaderB, "SinkB"),
    ClusterNodes.

confirm() ->
    SetupData = setup(),
    rtq_bucket_filtering_test(SetupData),
    pass.

%% The toplogy for this test is as follows:
%%     +--------+
%%     | Source |
%%     +--------+
%%       ^   ^
%%      /     \
%%     V       V
%% +-------+  +-------+
%% | SinkA |  | SinkB |
%% +-------+  +-------+
rtq_bucket_filtering_test(ClusterNodes) ->
    {SourceLeader, SinkLeaderA, SinkLeaderB, SourceNodes, _SinkANodes, _SinkBNodes} = ClusterNodes,

    lager:info("Enabling bucket filtering for source nodes, leader: ~p", [SourceLeader]),
    enable_bucket_filtering(SourceLeader),
    rt:wait_until_ring_converged(SourceNodes),

    %%% Enabling repl etc

    %% Enable RT replication from source cluster "SinkA"
    lager:info("Enabling realtime between ~p and ~p", [SourceLeader, SinkLeaderB]),
    enable_rt(SourceLeader, SourceNodes, "SinkA"),

    %% Enable RT replication from source cluster "SinkB"
    lager:info("Enabling realtime between ~p and ~p", [SourceLeader, SinkLeaderA]),
    enable_rt(SourceLeader, SourceNodes, "SinkB"),

    lager:info("Add filtered buckets for SinkA and SinkB to leader ~p", [SourceLeader]),

    add_bucket_filtering(SourceLeader, binary_to_list(?SINKB_BUCKET), "SinkA"),
    rt:wait_until_ring_converged(SourceNodes),

    add_bucket_filtering(SourceLeader, binary_to_list(?SINKA_BUCKET), "SinkB"),

    lager:info("Waiting for the ring to converge"),
    rt:wait_until_ring_converged(SourceNodes),


    KeyCount = 501,
    write_to_cluster(SourceLeader, 1, KeyCount, ?SINKA_BUCKET),
    write_to_cluster(SourceLeader, KeyCount + 1, KeyCount * 2, ?SINKB_BUCKET),

    read_from_cluster(SinkLeaderA, 1, KeyCount, 0, ?SINKA_BUCKET),
    read_from_cluster(SinkLeaderB, KeyCount + 1, KeyCount * 2, 0, ?SINKB_BUCKET),

    %% Checking buckets that shouldn't be there
    read_from_cluster(SinkLeaderA, KeyCount + 1, KeyCount * 2, 501, ?SINKB_BUCKET),
    read_from_cluster(SinkLeaderB, KeyCount + 1, KeyCount * 2, 501, ?SINKA_BUCKET).



make_clusters() ->
    NodeCount = rt_config:get(num_nodes, 6),
    lager:info("Deploy ~p nodes", [NodeCount]),
    Nodes = deploy_nodes(NodeCount, true),

    {SourceNodes, SinkNodes} = lists:split(2, Nodes),
    {SinkANodes, SinkBNodes} = lists:split(2, SinkNodes),
    lager:info("SinkANodes: ~p", [SinkANodes]),
    lager:info("SinkBNodes: ~p", [SinkBNodes]),

    lager:info("Build source cluster"),
    repl_util:make_cluster(SourceNodes),

    lager:info("Build sink cluster A"),
    repl_util:make_cluster(SinkANodes),

    lager:info("Build sink cluster B"),
    repl_util:make_cluster(SinkBNodes),

    SourceFirst = hd(SourceNodes),
    AFirst = hd(SinkANodes),
    BFirst = hd(SinkBNodes),

    %% Name the clusters
    repl_util:name_cluster(SourceFirst, "Source"),
    repl_util:name_cluster(AFirst, "SinkA"),
    repl_util:name_cluster(BFirst, "SinkB"),

    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(SourceNodes),
    rt:wait_until_ring_converged(SinkANodes),
    rt:wait_until_ring_converged(SinkBNodes),

    lager:info("Waiting for transfers to complete."),
    rt:wait_until_transfers_complete(SourceNodes),
    rt:wait_until_transfers_complete(SinkANodes),
    rt:wait_until_transfers_complete(SinkBNodes),

    %% get the leader for the source cluster
    lager:info("waiting for leader to converge on the source cluster"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(SourceNodes)),

    %% get the leader for the first sink cluster
    lager:info("waiting for leader to converge on sink cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(SinkANodes)),

    %% get the leader for the second cluster
    lager:info("waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(SinkBNodes)),

    SourceLeader = repl_util:get_leader(SourceFirst),
    ALeader = repl_util:get_leader(AFirst),
    BLeader = repl_util:get_leader(BFirst),

    %% Uncomment the following 2 lines to verify that pre-2.0 versions
    %% of Riak behave as expected if cascading writes are disabled for
    %% the sink clusters.
    %% disable_cascading(ALeader, SinkANodes),
    %% disable_cascading(BLeader, SinkBNodes),

    lager:info("Source Leader: ~p SinkALeader: ~p SinkBLeader: ~p", [SourceLeader, ALeader, BLeader]),
    {SourceLeader, ALeader, BLeader, SourceNodes, SinkANodes, SinkBNodes}.

%% @doc Connect two clusters using a given name.
connect_cluster(Source, Port, Name) ->
    lager:info("Connecting ~p to ~p for cluster ~p.",
               [Source, Port, Name]),
    repl_util:connect_cluster(Source, "127.0.0.1", Port),
    ?assertEqual(ok, repl_util:wait_for_connection(Source, Name)).

%% @doc Connect two clusters for replication using their respective leader nodes.
connect_clusters(SourceLeader, SinkLeader, SinkName) ->
    SinkPort = repl_util:get_port(SinkLeader),
    lager:info("connect source cluster to ~p on port ~p", [SinkName, SinkPort]),
    repl_util:connect_cluster(SourceLeader, "127.0.0.1", SinkPort),
    ?assertEqual(ok, repl_util:wait_for_connection(SourceLeader, SinkName)).

cluster_conf(_CascadingWrites) ->
    [
     {riak_repl,
      [
       %% turn off fullsync
       {fullsync_on_connect, false},
       {fullsync_interval, disabled},
       {max_fssource_cluster, 20},
       {max_fssource_node, 20},
       {max_fssink_node, 20}
      ]}
    ].

deploy_nodes(NumNodes, true) ->
    rt:deploy_nodes(NumNodes, cluster_conf(always), [riak_kv, riak_repl]);
deploy_nodes(NumNodes, false) ->
    rt:deploy_nodes(NumNodes, cluster_conf(never), [riak_kv, riak_repl]).

%% @doc Turn on Realtime replication on the cluster lead by LeaderA.
%%      The clusters must already have been named and connected.
enable_rt(SourceLeader, SourceNodes, SinkName) ->
    repl_util:enable_realtime(SourceLeader, SinkName),
    rt:wait_until_ring_converged(SourceNodes),

    repl_util:start_realtime(SourceLeader, SinkName),
    rt:wait_until_ring_converged(SourceNodes).

%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Start, End, Bucket) ->
    lager:info("Writing ~p keys with bucket name ~p to node ~p.", [End - Start, Bucket, Node]),
    ?assertEqual([],
                 repl_util:do_write(Node, Start, End, Bucket, 1)).

%% @doc Read from cluster a series of keys, asserting a certain number
%%      of errors.
read_from_cluster(Node, Start, End, Errors, ExpectedBucket) ->
    lager:info("Reading ~p keys with bucket name ~p from node ~p.", [End - Start, ExpectedBucket, Node]),
    Res2 = rt:systest_read(Node, Start, End, ExpectedBucket, 1),
    ?assertEqual(Errors, length(Res2)).

enable_bucket_filtering(Node) ->
    rpc:call(Node, riak_repl_console, enable_bucket_filtering, [[]]).

disable_bucket_filtering(Node) ->
    rpc:call(Node, riak_repl_console, disable_bucket_filtering, [[]]).

add_bucket_filtering(Node, Bucket, Destination) ->
    lager:info("Add filtering rule on ~p, ~p -> ~p", [Node, Bucket, Destination]),
    rpc:call(Node, riak_repl_console, add_filtered_bucket, [[Bucket, Destination]]).

remove_bucket_filtering(Node, Bucket, Destination) ->
    lager:info("Remove filtering rule on ~p, ~p -> ~p", [Node, Bucket, Destination]),
    rpc:call(Node, riak_repl_console, remove_filtered_bucket, [[Bucket, Destination]]).

wait_until_pending_count_zero(Nodes) ->
    WaitFun = fun() ->
        {Statuses, _} =  rpc:multicall(Nodes, riak_repl2_rtq, status, []),
        Out = [check_status(S) || S <- Statuses],
        not lists:member(false, Out)
              end,
    ?assertEqual(ok, rt:wait_until(WaitFun)),
    ok.

check_status(Status) ->
    case proplists:get_all_values(consumers, Status) of
        undefined ->
            true;
        [] ->
            true;
        Cs ->
            PendingList = [proplists:lookup_all(pending, C) || {_, C} <- lists:flatten(Cs)],
            PendingCount = lists:sum(proplists:get_all_values(pending, lists:flatten(PendingList))),
            ?debugFmt("RTQ status pending on test node:~p", [PendingCount]),
            PendingCount == 0
    end.
