-module(repl2_multiple_connections).
-behaviour(riak_test).
-export([
  confirm/0
]).
-include_lib("eunit/include/eunit.hrl").


confirm() ->
  [ run_test(N) || N <- lists:seq(1,3)],
  pass.

run_test(N) ->
  lager:info("---------------------------------------"),
  lager:info("--------------- Test ~p ---------------", [N]),
  lager:info("---------------------------------------"),
  ?assertEqual(pass, test(N)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                               Riak Test's                                                            %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Test 1
%% Source Cluster Size = 4
%% Sink Cluster Size = 4
%% Test for 1 unique connection each
%% Test that rtsource_conn pids and addresses match that in data_mgr
test(1) ->
  RealtimeConnectionRebalancingDelay = 60,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 10,
  SinkClusterPolling = 10,
  NumberOfSourceNodes = 4,
  NumberOfSinkNodes = 4,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  pass;

%% Test 2
%% Source Cluster Size = 3
%% Sink Cluster Size = 5
%% Test for 1 unique connection each
%% Test that rtsource_conn pids and addresses match that in data_mgr
test(2) ->
  RealtimeConnectionRebalancingDelay = 60,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 10,
  SinkClusterPolling = 10,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 5,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  pass;


%% Test 3
%% Source Cluster Size = 5
%% Sink Cluster Size = 3
%% Test for 1 unique connection each
%% Test that rtsource_conn pids and addresses match that in data_mgr
test(3) ->
  RealtimeConnectionRebalancingDelay = 60,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 10,
  SinkClusterPolling = 10,
  NumberOfSourceNodes = 5,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  pass.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                         Riak Test Functions                                                          %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                         Helper Functions                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
check_connections({SourceLeader, _SinkLeader, SourceNodes, SinkNodes}) ->
  DataMgrRealtimeConnections = rpc:call(SourceLeader, riak_repl2_rtsource_conn_data_mgr, read, [realtime_connections, "B"]),
  Keys = dict:fetch_keys(DataMgrRealtimeConnections),

  ActualSourceConnectionCounts = lists:sort(count_primary_connections_for_source_nodes(DataMgrRealtimeConnections, Keys, [])),
%%  ActualSinkConnectionCounts = lists:sort(count_primary_connections_for_sink_nodes(DataMgrRealtimeConnections, Keys, [])),
  Inverted = invert_dictionary(DataMgrRealtimeConnections),
  lager:info("INVERTED REALTIME CONNECTIONS ~p", [Inverted]),

  ExpectedSourceConnectionCounts = lists:sort(build_expected_primary_connection_counts(for_source_nodes, SourceNodes, SinkNodes)),
  _ExpectedSinkConnectionCounts = lists:sort(build_expected_primary_connection_counts(for_sink_nodes, SourceNodes, SinkNodes)),

  ConnMgrRealtimeConnections = build_realtime_connections_from_conn_mgr(SourceNodes),
  RtSourceConnRealtimeConnections = build_realtime_connections_from_rtsource_conn(SourceNodes),

  SortedDataMgrRTC = dict_to_sorted_list(DataMgrRealtimeConnections),
  SortedConnMgrRTC = dict_to_sorted_list(ConnMgrRealtimeConnections),
  SortedRtSourceRTC = dict_to_sorted_list(RtSourceConnRealtimeConnections),

  lager:info("sorted realtime connection (data):     ~p", [SortedDataMgrRTC]),
  lager:info("sorted realtime connection (conn_mgr): ~p", [SortedConnMgrRTC]),
  lager:info("sorted realtime connection (rtsource): ~p", [SortedRtSourceRTC]),
  lager:info("actual connection count: ~p", [ActualSourceConnectionCounts]),
  lager:info("expected source connection count: ~p", [ExpectedSourceConnectionCounts]),

  ?assertEqual(SortedDataMgrRTC, SortedConnMgrRTC),
  ?assertEqual(SortedDataMgrRTC, SortedRtSourceRTC),
  ?assertEqual(ExpectedSourceConnectionCounts, ActualSourceConnectionCounts).

build_expected_primary_connection_counts(For, SourceNodes, SinkNodes) ->
  case {SourceNodes, SinkNodes} of
    {undefined, _} ->
      [];
    {_, undefined} ->
      [];
    _ ->
      {M,N} = case For of
                for_source_nodes ->
                  {length(SourceNodes), length(SinkNodes)};
                for_sink_nodes ->
                  {length(SinkNodes), length(SourceNodes)}
              end,
      case M*N of
        0 ->
          [];
        _ ->
          case M >= N of
            true ->
              [1 || _ <-  lists:seq(1,M)];
            false ->
              Base = N div M,
              NumberOfNodesWithOneAdditionalConnection = N rem M,
              NumberOfNodesWithBaseConnections = M - NumberOfNodesWithOneAdditionalConnection,
              [Base+1 || _ <-lists:seq(1,NumberOfNodesWithOneAdditionalConnection)] ++ [Base || _ <- lists:seq(1,NumberOfNodesWithBaseConnections)]
          end
      end
  end.


count_primary_connections_for_source_nodes(_RealtimeConnections, [], List) ->
  List;
count_primary_connections_for_source_nodes(RealtimeConnections, [Key|Keys], List) ->
  NodeConnections = dict:fetch(Key, RealtimeConnections),
  count_primary_connections_for_source_nodes(RealtimeConnections, Keys, List ++ [get_primary_count(NodeConnections,0)]).

get_primary_count([], N) ->
  N;
get_primary_count([{_,Primary}|Rest], N) ->
  case Primary of
    true ->
      get_primary_count(Rest, N+1);
    _ ->
      get_primary_count(Rest, N)
  end.

build_realtime_connections_from_conn_mgr(SourceNodes) ->
  build_realtime_connections_from_conn_mgr_helper(SourceNodes, dict:new()).

build_realtime_connections_from_conn_mgr_helper([], Dict) ->
  Dict;
build_realtime_connections_from_conn_mgr_helper([SourceNode | Rest], Dict) ->
  [{"B",Pid}] = rpc:call(SourceNode, riak_repl2_rtsource_conn_sup, enabled, []),
  Endpoints = rpc:call(SourceNode, riak_repl2_rtsource_conn_mgr, get_endpoints, [Pid]),
  NewDict = dict:store(SourceNode, orddict:fetch_keys(Endpoints), Dict),
  build_realtime_connections_from_conn_mgr_helper(Rest, NewDict).


build_realtime_connections_from_rtsource_conn(SourceNodes) ->
  build_realtime_connections_from_rtsource_conn_helper(SourceNodes, dict:new()).

build_realtime_connections_from_rtsource_conn_helper([], Dict) ->
  Dict;
build_realtime_connections_from_rtsource_conn_helper([SourceNode| Rest], Dict) ->
  ConnsList = [ rpc:call(SourceNode, riak_repl2_rtsource_conn, get_address, [Pid]) ||
    {_,Pid,_,_} <- rpc:call(SourceNode, supervisor, which_children, [riak_repl2_rtsource_conn_2_sup_B])],
  NewDict = dict:store(SourceNode, ConnsList, Dict),
  build_realtime_connections_from_conn_mgr_helper(Rest, NewDict).


dict_to_sorted_list(Dictioanry) ->
  List = dict:to_list(Dictioanry),
  SortKeys = lists:sort(List),
  lists:foldl(fun({K,V}, Acc) -> Acc ++ [{K, lists:sort(V)}] end, [], SortKeys).

invert_dictionary(Dictionary) ->
  Keys = dict:fetch_keys(Dictionary),
  invert_dictionary_helper(Dictionary, Keys, dict:new()).

invert_dictionary_helper(_Dictionary, [], NewDict) ->
  NewDict;
invert_dictionary_helper(Dictionary, [Key|Rest], Dict) ->
  Values = dict:fetch(Key, Dictionary),
  NewDict = invert_dictionary_helper_builder(Key, Values, Dict),
  invert_dictionary_helper(Dictionary, Rest, NewDict).

invert_dictionary_helper_builder(_Source, [], Dict) ->
  Dict;
invert_dictionary_helper_builder(Source, [{Sink,Primary}|Rest], Dict) ->
  NewDict = case Primary of
              true ->
                dict:append(Sink, {Source,Primary}, Dict);
              false ->
                Dict
            end,
  invert_dictionary_helper_builder(Source, Rest, NewDict).
