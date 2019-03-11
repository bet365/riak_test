-module(repl_fs_bucket_filtering).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(SINK_A_BKT, <<"bkt-sink-A">>).
-define(SINK_B_BKT, <<"bkt-sink-B">>).
-define(KEY_COUNT, 501).

config() -> [
        {riak_core,
            [
                {ring_creation_size, 8},
                {default_bucket_props, [{n_val, 1}, {allow_mult, false}]}
            ]
        },
        {riak_repl,
        [
            {fullsync_strategy, keylist},
            {fullsync_on_connect, false},
            {fullsync_interval, disabled},
            {max_fssource_retries, infinity},
            {max_fssource_cluster, 20},
            {max_fssource_node, 20},
            {max_fssink_node, 20}
        ]}].

deploy_nodes(NumNodes) ->
    rt:deploy_nodes(NumNodes, config(), [riak_kv, riak_repl]).

setup() ->
    rt:set_advanced_conf(all, config()),

    %% Build our 3 clusters, repl will be setup like this:
    %%             [ Source ]
    %%              /      \
    %%            /         \
    %%           V           V
    %%       [Sink A]     [Sink B]
    [SourceNodes, SinkNodes] = rt:build_clusters([3, 3]),

    %% Wait for repl to startup on each cluster
    rt:wait_for_cluster_service(SourceNodes, riak_repl),
    rt:wait_for_cluster_service(SinkNodes, riak_repl),

    %% We get the first node of each cluster so we can name them
    SourceFirst = hd(SourceNodes),
    SinkFirst = hd(SinkNodes),

    %% Name the clusters so we can enable repl between them
    repl_util:name_cluster(SourceFirst, "A"),
    repl_util:name_cluster(SinkFirst, "B"),

    %% Wait for the ring to converge on each cluster as we've changed the cluster names
    lager:info("Waiting for ring convergence"),
    rt:wait_until_ring_converged(SourceNodes),
    rt:wait_until_ring_converged(SinkNodes),

    rt:wait_until_transfers_complete(SourceNodes),
    rt:wait_until_transfers_complete(SinkNodes),

    %% Wait for leaders to be decided on each cluster
    ?assertEqual(ok, repl_util:wait_until_leader_converge(SourceNodes)),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(SinkNodes)),

    %% Get each of the leaders
    SourceLeader = repl_util:get_leader(SourceFirst),
    SinkALeader  = repl_util:get_leader(SinkFirst),

    %% Connect the source cluster to the sink nodes and wait for them to connect
    connect_clusters(SourceLeader, {SinkALeader, "B"}),

    {SourceLeader, SourceNodes, SinkALeader, SinkNodes}.

confirm() ->
    SetupData = setup(),
    run_test(SetupData),
    pass.

run_test(ClusterNodes) ->
    {SourceLeader, SourceNodes, SinkALeader, SinkANodes} = ClusterNodes,

    %% For fullsync we have to enable bucket filtering on the sink and source sides
    lager:info("Enabling bucket filtering for source leader ~p", [SourceLeader]),
    enable_bucket_filtering(SourceLeader),
    rt:wait_until_ring_converged(SourceNodes),

    lager:info("Enabling bucket filtering for B nodes, leader: ~p", [SinkALeader]),
    enable_bucket_filtering(SinkALeader),
    rt:wait_until_ring_converged(SinkANodes),

    %% -------------------------------------------------------------------------

    lager:info("Add bucket filtering rules from source to sink nodes"),
    add_bucket_filtering(SourceLeader, ?SINK_B_BKT, "B"),
    rt:wait_until_ring_converged(SourceNodes),

    %% -------------------------------------------------------------------------
    %% Enable: Source -> SinkA
    lager:info("[source] Enabling fullsync between ~p and ~p", [SourceLeader, SinkALeader]),
    repl_util:enable_fullsync(SourceLeader, "B"),

    ?assertEqual(ok, repl_util:wait_for_connection(SourceLeader, "B")),

    rt:wait_until_ring_converged(SourceNodes),

    write_to_cluster(SourceLeader, 1, ?KEY_COUNT, ?SINK_A_BKT),
    write_to_cluster(SourceLeader, ?KEY_COUNT + 1, ?KEY_COUNT * 2, ?SINK_B_BKT),

    R3 = rpc:call(SourceLeader, riak_repl_console, fullsync, [["start"]]),
    ?assertEqual(ok, R3),
    repl_util:wait_until_fullsync_started(SourceLeader),

    read_from_cluster(SourceLeader, 1, ?KEY_COUNT, 0, ?SINK_A_BKT),
    read_from_cluster(SourceLeader, ?KEY_COUNT+1, ?KEY_COUNT*2, 0, ?SINK_B_BKT),

    %% Read keys from Sink's to confirm

    read_from_cluster(SinkALeader, 1, ?KEY_COUNT, 0, ?SINK_A_BKT),
    read_from_cluster(SinkALeader, 1, ?KEY_COUNT, ?KEY_COUNT, ?SINK_B_BKT).

%% utils

connect_clusters(SourceLeader, {SinkALeader, SinkAName}) ->
    BPort = repl_util:get_port(SinkALeader),
    lager:info("connect source cluster to ~p on port ~b", [SinkAName, BPort]),
    repl_util:connect_cluster_by_name(SourceLeader, BPort, "B"),
    ?assertEqual(ok, repl_util:wait_for_connection(SourceLeader, SinkAName)).

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

-spec add_bucket_filtering(Node :: node(), Bucket :: binary(), Destination :: list()) -> ok.
add_bucket_filtering(Node, Bucket, Destination) when is_binary(Bucket) ->
    add_bucket_filtering(Node, binary_to_list(Bucket), Destination);
add_bucket_filtering(Node, Bucket, Destination) when is_list(Destination) andalso is_list(Bucket) ->
    lager:info("Add filtering rule on ~p, ~p -> ~p", [Node, Bucket, Destination]),
    rpc:call(Node, riak_repl_console, add_filtered_bucket, [[Bucket, Destination]]).

remove_bucket_filtering(Node, Bucket, Destination) ->
    lager:info("Remove filtering rule on ~p, ~p -> ~p", [Node, Bucket, Destination]),
    rpc:call(Node, riak_repl_console, remove_filtered_bucket, [[Bucket, Destination]]).