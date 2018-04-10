-module(repl2_multiple_connections).
-behaviour(riak_test).
-export([
  confirm/0
]).
-include_lib("eunit/include/eunit.hrl").


confirm() ->
  [?assertEqual(pass, test(N)) || N <- lists:seq(1,2)],
  pass.

test(1) ->
  RealtimeConnectionRebalancingDelay = 60,
  RealtimeRemovealDelay = 60,
  NodeWaterPolling = 10,
  SinkClusterPolling = 10,
  NumberOfSourceNodes = 4,
  NumberOfSinkNodes = 4,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},

  {SourceLeader, SinkLeader, SourceNodes, _SinkNodes} = make_connected_clusters(ConfVar, SourceSinkSizes),

  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),

  lager:info("\n\n\n\n\n\n\n\n\n"),

  pass;

test(2) ->
  RealtimeConnectionRebalancingDelay = 60,
  RealtimeRemovealDelay = 60,
  NodeWaterPolling = 10,
  SinkClusterPolling = 10,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},

  {SourceLeader, SinkLeader, SourceNodes, _SinkNodes} = make_connected_clusters(ConfVar, SourceSinkSizes),

  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),

  pass.





make_connected_clusters({RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling}, {SourceNodesSize,SinkNodesSize}) ->

  lager:info("Deploy ~p nodes", [SourceNodesSize+SinkNodesSize]),
  lager:info("Deploy Sink Cluster w/size = ~p", [SinkNodesSize]),
  lager:info("Deploy Source Cluster w/size = ~p", [SourceNodesSize]),

  Conf = [
    {riak_repl,
      [
        %% turn off fullsync
        {fullsync_on_connect, false},
        {fullsync_interval, disabled},
        {rt_heartbeat_interval, 120},
        {rt_heartbeat_timeout, 120},
        {realtime_connection_rebalance_max_delay_secs, RealtimeConnectionRebalancingDelay},
        {realtime_connection_removal_delay, RealtimeRemovealDelay},
        {realtime_node_watcher_polling_interval, NodeWaterPolling},
        {realtime_sink_cluster_polling_interval, SinkClusterPolling}
      ]}
  ],

  Nodes = rt:deploy_nodes(SourceNodesSize+SinkNodesSize, Conf, [riak_kv, riak_repl]),

  {SourceNodes, SinkNodes} = lists:split(SourceNodesSize, Nodes),
  lager:info("Source Nodes: ~p", [SourceNodes]),
  lager:info("Sink Nodes: ~p", [SinkNodes]),

  lager:info("Build cluster A"),
  repl_util:make_cluster(SourceNodes),

  lager:info("Build cluster B"),
  repl_util:make_cluster(SinkNodes),

  %% get the leader for the first cluster
  lager:info("waiting for leader to converge on cluster A"),
  ?assertEqual(ok, repl_util:wait_until_leader_converge(SourceNodes)),
  SourceLeader = hd(SourceNodes),

  %% get the leader for the second cluster
  lager:info("waiting for leader to converge on cluster B"),
  ?assertEqual(ok, repl_util:wait_until_leader_converge(SinkNodes)),
  SinkLeader = hd(SinkNodes),

  %% Name the clusters
  repl_util:name_cluster(SourceLeader, "A"),
  rt:wait_until_ring_converged(SourceNodes),

  repl_util:name_cluster(SinkLeader, "B"),
  rt:wait_until_ring_converged(SinkNodes),

  %% Connect for replication
  connect_clusters(SourceLeader, SinkLeader),

  {SourceLeader, SinkLeader, SourceNodes, SinkNodes}.


connect_clusters(LeaderA, LeaderB) ->
  {ok, {_IP, Port}} = rpc:call(LeaderB, application, get_env,
    [riak_core, cluster_mgr]),
  lager:info("connect cluster A:~p to B on port ~p", [LeaderA, Port]),
  repl_util:connect_cluster(LeaderA, "127.0.0.1", Port),
  ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")).

enable_rt(LeaderA, ANodes) ->
  repl_util:enable_realtime(LeaderA, "B"),
  rt:wait_until_ring_converged(ANodes),

  repl_util:start_realtime(LeaderA, "B"),
  rt:wait_until_ring_converged(ANodes).

verify_rt(LeaderA, LeaderB) ->
  TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
    <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
  TestBucket = <<TestHash/binary, "-rt_test_a">>,
  First = 101,
  Last = 200,

  %% Write some objects to the source cluster (A),
  rt:log_to_nodes([LeaderA], "write objects (verify_rt)"),
  lager:info("Writing ~p keys to ~p, which should RT repl to ~p",
    [Last-First+1, LeaderA, LeaderB]),
  ?assertEqual([], repl_util:do_write(LeaderA, First, Last, TestBucket, 2)),

  %% verify data is replicated to B
  rt:log_to_nodes([LeaderA], "read objects (verify_rt)"),
  lager:info("Reading ~p keys written from ~p", [Last-First+1, LeaderB]),
  ?assertEqual(0, repl_util:wait_for_reads(LeaderB, First, Last, TestBucket, 2)).