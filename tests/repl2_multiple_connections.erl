-module(repl2_multiple_connections).
-behaviour(riak_test).
-export([
  confirm/0
]).
-include_lib("eunit/include/eunit.hrl").


confirm() ->
  %% run all tests
%%  [ run_test(N) || N <- lists:seq(3,19)],

  %% run the most necessary tests
%%  [ run_test(N) || N <- [1,2,3,4,5,7,9,11,13,16,18]],

  %% run a specific tests
  run_test(exit_error_data_mgr_not_leader),
  run_test(exit_error_data_mgr_leader),
  run_test(exit_error_conn_mgr_not_leader),
  run_test(exit_error_conn_mgr_leader),
  run_test(exit_error_rtsource_conn_not_leader),
  run_test(exit_error_rtsource_conn_leader),


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
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  timer:sleep(1000),
  check_connections(Nodes),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
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
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  timer:sleep(1000),
  check_connections(Nodes),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
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
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  timer:sleep(1000),
  check_connections(Nodes),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;


test(4) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (4,4)
  SpareNode1 = lists:nth(1,SpareNodes),
  SpareNode2 = lists:nth(2, SpareNodes),
  NewSourceNodes = SourceNodes ++ [SpareNode1],
  NewSinkNodes = SinkNodes ++ [SpareNode2],
  NewSpareNodes = [],
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},

  rt:join(SpareNode1, SourceLeader),
  rt:join(SpareNode2, SinkLeader),
  ?assertEqual(ok, rt:wait_until_no_pending_changes(NewSourceNodes)),
  ?assertEqual(ok, rt:wait_until_no_pending_changes(NewSinkNodes)),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(5) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (4,3)
  SpareNode1 = lists:nth(1,SpareNodes),
  _SpareNode2 = lists:nth(2, SpareNodes),
  NewSourceNodes = SourceNodes ++ [SpareNode1],
  NewSinkNodes = SinkNodes,
  NewSpareNodes = SpareNodes -- [SpareNode1],
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},

  rt:join(SpareNode1, SourceLeader),
  ?assertEqual(ok, rt:wait_until_no_pending_changes(NewSourceNodes)),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(6) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (5,3)
  SpareNode1 = lists:nth(1,SpareNodes),
  SpareNode2 = lists:nth(2, SpareNodes),
  NewSourceNodes = SourceNodes ++ [SpareNode1, SpareNode2],
  NewSinkNodes = SinkNodes,
  NewSpareNodes = [],
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},

  rt:join(SpareNode1, SourceLeader),
  rt:join(SpareNode2, SourceLeader),
  ?assertEqual(ok, rt:wait_until_no_pending_changes(NewSourceNodes)),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(7) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (4,2)
  SpareNode1 = lists:nth(1,SpareNodes),
  SpareNode2 = lists:nth(2, SpareNodes),
  NewSourceNodes = SourceNodes ++ [SpareNode1],
  [OldSink1| NewSinkNodes] = SinkNodes,
  NewSpareNodes = [SpareNode2],
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},

  rt:join(SpareNode1, SourceLeader),
  rt:stop_and_wait(OldSink1),
  ?assertEqual(ok, rt:wait_until_no_pending_changes(NewSourceNodes)),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:start(OldSink1),
  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(8) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (4,1)
  SpareNode1 = lists:nth(1,SpareNodes),
  SpareNode2 = lists:nth(2, SpareNodes),
  NewSourceNodes = SourceNodes ++ [SpareNode1],
  [OldSink1, OldSink2 | NewSinkNodes] = SinkNodes,
  NewSpareNodes = [SpareNode2],
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},

  rt:join(SpareNode1, SourceLeader),
  rt:stop_and_wait(OldSink1),
  rt:stop_and_wait(OldSink2),
  ?assertEqual(ok, rt:wait_until_no_pending_changes(NewSourceNodes)),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:start(OldSink1),
  rt:start(OldSink2),
  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(9) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (2,4)
  SpareNode1 = lists:nth(1,SpareNodes),
  SpareNode2 = lists:nth(2, SpareNodes),

  [OldSourceNode1 | NewSourceNodes] = SourceNodes,
  NewSinkNodes = SinkNodes ++ [SpareNode1],
  NewSpareNodes = [SpareNode2],
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},

  rt:join(SpareNode1, hd(NewSinkNodes)),
  rt:stop_and_wait(OldSourceNode1),
  ?assertEqual(ok, rt:wait_until_no_pending_changes(NewSinkNodes)),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:start(OldSourceNode1),
  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(10) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (1,5)
  SpareNode1 = lists:nth(1,SpareNodes),
  SpareNode2 = lists:nth(2, SpareNodes),

  [OldSourceNode1, OldSourceNode2 | NewSourceNodes] = SourceNodes,
  NewSinkNodes = SinkNodes ++ [SpareNode1, SpareNode2],
  NewSpareNodes = [],
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},

  rt:join(SpareNode1, hd(NewSinkNodes)),
  rt:join(SpareNode2, hd(NewSinkNodes)),
  rt:stop_and_wait(OldSourceNode1),
  rt:stop_and_wait(OldSourceNode2),
  ?assertEqual(ok, rt:wait_until_no_pending_changes(NewSinkNodes)),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:start(OldSourceNode1),
  rt:start(OldSourceNode2),
  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(11) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (2,3)
  _SpareNode1 = lists:nth(1,SpareNodes),
  _SpareNode2 = lists:nth(2, SpareNodes),

  [OldSourceNode1| NewSourceNodes] = SourceNodes,
  NewSinkNodes = SinkNodes,
  NewSpareNodes = SpareNodes,
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},

  rt:stop_and_wait(OldSourceNode1),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:start(OldSourceNode1),
  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(12) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (1,3)
  _SpareNode1 = lists:nth(1,SpareNodes),
  _SpareNode2 = lists:nth(2, SpareNodes),

  [OldSourceNode1, OldSourceNode2| NewSourceNodes] = SourceNodes,
  NewSinkNodes = SinkNodes,
  NewSpareNodes = SpareNodes,
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},


  rt:stop_and_wait(OldSourceNode1),
  rt:stop_and_wait(OldSourceNode2),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:start(OldSourceNode1),
  rt:start(OldSourceNode2),
  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(13) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (2,2)
  _SpareNode1 = lists:nth(1,SpareNodes),
  _SpareNode2 = lists:nth(2, SpareNodes),

  [OldSourceNode1| NewSourceNodes] = SourceNodes,
  [OldSinkNode1 | NewSinkNodes] = SinkNodes,
  NewSpareNodes = SpareNodes,
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},


  rt:stop_and_wait(OldSourceNode1),
  rt:stop_and_wait(OldSinkNode1),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:start(OldSourceNode1),
  rt:start(OldSinkNode1),
  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(14) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (2,1)
  _SpareNode1 = lists:nth(1,SpareNodes),
  _SpareNode2 = lists:nth(2, SpareNodes),

  [OldSourceNode1| NewSourceNodes] = SourceNodes,
  [OldSinkNode1, OldSinkNode2 | NewSinkNodes] = SinkNodes,
  NewSpareNodes = SpareNodes,
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},


  rt:stop_and_wait(OldSourceNode1),
  rt:stop_and_wait(OldSinkNode1),
  rt:stop_and_wait(OldSinkNode2),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:start(OldSourceNode1),
  rt:start(OldSinkNode1),
  rt:start(OldSinkNode2),
  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(15) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (1,2)
  _SpareNode1 = lists:nth(1,SpareNodes),
  _SpareNode2 = lists:nth(2, SpareNodes),

  [OldSourceNode1, OldSourceNode2| NewSourceNodes] = SourceNodes,
  [OldSinkNode1 | NewSinkNodes] = SinkNodes,
  NewSpareNodes = SpareNodes,
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},


  rt:stop_and_wait(OldSourceNode1),
  rt:stop_and_wait(OldSourceNode2),
  rt:stop_and_wait(OldSinkNode1),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:start(OldSourceNode1),
  rt:start(OldSourceNode2),
  rt:start(OldSinkNode1),
  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(16) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (3,4)
  SpareNode1 = lists:nth(1,SpareNodes),
  _SpareNode2 = lists:nth(2, SpareNodes),

  NewSourceNodes = SourceNodes,
  NewSinkNodes = SinkNodes ++ [SpareNode1],
  NewSpareNodes = SpareNodes,
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},

  rt:join(SpareNode1, hd(NewSinkNodes)),
  ?assertEqual(ok, rt:wait_until_no_pending_changes(NewSinkNodes)),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(17) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (3,5)
  SpareNode1 = lists:nth(1,SpareNodes),
  SpareNode2 = lists:nth(2, SpareNodes),

  NewSourceNodes = SourceNodes,
  NewSinkNodes = SinkNodes ++ [SpareNode1, SpareNode2],
  NewSpareNodes = SpareNodes,
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},

  rt:join(SpareNode1, hd(NewSinkNodes)),
  rt:join(SpareNode2, hd(NewSinkNodes)),
  ?assertEqual(ok, rt:wait_until_no_pending_changes(NewSinkNodes)),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(18) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (3,2)
  _SpareNode1 = lists:nth(1,SpareNodes),
  _SpareNode2 = lists:nth(2, SpareNodes),

  NewSourceNodes = SourceNodes,
  [OldSinkNode1 | NewSinkNodes] = SinkNodes,
  NewSpareNodes = SpareNodes,
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},

  rt:stop_and_wait(OldSinkNode1),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:start(OldSinkNode1),
  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(19) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% (3,3) -> (3,1)
  _SpareNode1 = lists:nth(1,SpareNodes),
  _SpareNode2 = lists:nth(2, SpareNodes),

  NewSourceNodes = SourceNodes,
    [OldSinkNode1, OldSinkNode2| NewSinkNodes] = SinkNodes,
  NewSpareNodes = SpareNodes,
  NewNodes = {hd(NewSourceNodes), hd(NewSinkNodes), NewSourceNodes, NewSinkNodes, NewSpareNodes},

  rt:stop_and_wait(OldSinkNode1),
  rt:stop_and_wait(OldSinkNode2),
  timer:sleep(5000),
  check_connections(NewNodes),

  rt:start(OldSinkNode1),
  rt:start(OldSinkNode2),
  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(exit_error_data_mgr_leader) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% kill data manager on leader
  Pid = rpc:call(SourceLeader, erlang, whereis, [riak_repl2_rtsource_conn_data_mgr]),
  lager:info("PID: ~p", [Pid]),
  Exit = rpc:call(SourceLeader, erlang, exit, [Pid, error]),
  lager:info("EXIT CALL: ~p", [Exit]),

  timer:sleep(10000),
  check_connections(Nodes),

  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(exit_error_data_mgr_not_leader) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  [_X, Y | _Rest] = SourceNodes,

  %% kill data manager not on leader
  Pid = rpc:call(Y, erlang, whereis, [riak_repl2_rtsource_conn_data_mgr]),
  lager:info("PID: ~p", [Pid]),
  Exit = rpc:call(Y, erlang, exit, [Pid, error]),
  lager:info("EXIT CALL: ~p", [Exit]),

  timer:sleep(10000),
  check_connections(Nodes),

  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(exit_error_conn_mgr_not_leader) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  [_X, Y | _Rest] = SourceNodes,

  %% kill conn mgr not on leader
  [{"B",Pid}] = rpc:call(Y, riak_repl2_rtsource_conn_sup, enabled, []),
  lager:info("PID: ~p", [Pid]),
  Exit = rpc:call(Y, erlang, exit, [Pid, error]),
  lager:info("EXIT CALL: ~p", [Exit]),

  timer:sleep(10000),
  check_connections(Nodes),

  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(exit_error_conn_mgr_leader) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% kill conn mgr on leader
  [{"B",Pid}] = rpc:call(SourceLeader, riak_repl2_rtsource_conn_sup, enabled, []),
  lager:info("PID: ~p", [Pid]),
  Exit = rpc:call(SourceLeader, erlang, exit, [Pid, error]),
  lager:info("EXIT CALL: ~p", [Exit]),

  timer:sleep(10000),
  check_connections(Nodes),

  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  pass;

test(exit_error_rtsource_conn_not_leader) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  [_X, Y | _Rest] = SourceNodes,

  %% kill rtsource conn not on leader
  [{"B",ConnMgrPid}] = rpc:call(Y, riak_repl2_rtsource_conn_sup, enabled, []),
  [Pid] = rpc:call(Y, riak_repl2_rtsource_conn_mgr, get_rtsource_conn_pids, [ConnMgrPid]),
  lager:info("PID: ~p", [Pid]),
  Exit = rpc:call(Y, erlang, exit, [Pid, error]),
  lager:info("EXIT CALL: ~p", [Exit]),

  timer:sleep(10000),
  check_connections(Nodes),

  [Pid2] = rpc:call(Y, riak_repl2_rtsource_conn_mgr, get_rtsource_conn_pids, [ConnMgrPid]),
  ?assertNotEqual(Pid, Pid2),
  Result = try rpc:call(Y, sys, get_state, [Pid]) of
             {badrpc,{'EXIT',{noproc,{sys,get_state,[Pid]}}}} ->
               pass;
             State ->
               lager:info("PID IS STILL ACTIVE -> State: ~p", [State]),
               fail
           catch
             _Type:_Error ->
               pass
           end,

  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  Result;

test(exit_error_rtsource_conn_leader) ->
  RealtimeConnectionRebalancingDelay = 2,
  RealtimeRemovealDelay = 0,
  NodeWaterPolling = 1,
  SinkClusterPolling = 1,
  NumberOfSourceNodes = 3,
  NumberOfSinkNodes = 3,
  ConfVar = {RealtimeConnectionRebalancingDelay, RealtimeRemovealDelay, NodeWaterPolling, SinkClusterPolling},
  SourceSinkSizes = {NumberOfSourceNodes, NumberOfSinkNodes},
  Nodes = make_connected_clusters(ConfVar, SourceSinkSizes),
  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes} = Nodes,
  enable_rt(SourceLeader, SourceNodes),
  verify_rt(SourceLeader, SinkLeader),
  check_connections(Nodes),

  %% kill rtsource conn on  leader
  [{"B",ConnMgrPid}] = rpc:call(SourceLeader, riak_repl2_rtsource_conn_sup, enabled, []),
  [Pid] = rpc:call(SourceLeader, riak_repl2_rtsource_conn_mgr, get_rtsource_conn_pids, [ConnMgrPid]),
  lager:info("PID: ~p", [Pid]),
  Exit = rpc:call(SourceLeader, erlang, exit, [Pid, error]),
  lager:info("EXIT CALL: ~p", [Exit]),

  timer:sleep(10000),
  check_connections(Nodes),

  [Pid2] = rpc:call(SourceLeader, riak_repl2_rtsource_conn_mgr, get_rtsource_conn_pids, [ConnMgrPid]),
  ?assertNotEqual(Pid, Pid2),

  Result = try rpc:call(SourceLeader, sys, get_state, [Pid]) of
             {badrpc,{'EXIT',{noproc,{sys,get_state,[Pid]}}}} ->
               pass;
            State ->
              lager:info("PID IS STILL ACTIVE -> State: ~p", [State]),
              fail
          catch
            _Type:_Error ->
              pass
          end,

  timer:sleep(3000),
  rt:clean_cluster(SourceNodes),
  rt:clean_cluster(SinkNodes),
  rt:clean_cluster(SpareNodes),
  Result.
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

  Nodes = rt:deploy_nodes(8, Conf, [riak_kv, riak_repl]),

  CheckSize = 8 >= SourceNodesSize+SinkNodesSize,
  ?assertEqual(true, CheckSize),


  {SourceNodes, Spare} = lists:split(SourceNodesSize, Nodes),
  {SinkNodes, SpareNodes} = lists:split(SinkNodesSize, Spare),

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

  {SourceLeader, SinkLeader, SourceNodes, SinkNodes, SpareNodes}.


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
check_connections({SourceLeader, _SinkLeader, SourceNodes, SinkNodes, _SpareNodes}) ->
  % ----------------------------------------------------------------------------------------------------------%
  %                                         Source Tests                                                      %
  % ----------------------------------------------------------------------------------------------------------%
  DataMgrRealtimeConnections = rpc:call(SourceLeader, riak_repl2_rtsource_conn_data_mgr, read, [realtime_connections, "B"]),
  ConnMgrRealtimeConnections = build_realtime_connections_from_conn_mgr(SourceNodes),
  RtSourceConnRealtimeConnections = build_realtime_connections_from_rtsource_conn(SourceNodes),

  SourceRTCKeys = dict:fetch_keys(DataMgrRealtimeConnections),
  ActualSourceConnectionCounts = lists:sort(count_primary_connections(DataMgrRealtimeConnections, SourceRTCKeys, [])),
  ExpectedSourceConnectionCounts = lists:sort(build_expected_primary_connection_counts(for_source_nodes, SourceNodes, SinkNodes)),

  SortedDataMgrRTC = dict_to_sorted_list(DataMgrRealtimeConnections),
  SortedConnMgrRTC = dict_to_sorted_list(ConnMgrRealtimeConnections),
  SortedRtSourceRTC = dict_to_sorted_list(RtSourceConnRealtimeConnections),

  lager:info("sorted realtime connection (data_mgr):     ~p", [SortedDataMgrRTC]),
  lager:info("sorted realtime connection (conn_mgr): ~p", [SortedConnMgrRTC]),
  lager:info("sorted realtime connection (rtsource): ~p", [SortedRtSourceRTC]),
  lager:info("(actual) connection count: ~p", [ActualSourceConnectionCounts]),
  lager:info("(expected) source connection count: ~p", [ExpectedSourceConnectionCounts]),

  ?assertEqual(SortedDataMgrRTC, SortedConnMgrRTC),
  ?assertEqual(SortedDataMgrRTC, SortedRtSourceRTC),
  ?assertEqual(ExpectedSourceConnectionCounts, ActualSourceConnectionCounts),

  % ----------------------------------------------------------------------------------------------------------%
  %                                         Sink Tests                                                        %
  % ----------------------------------------------------------------------------------------------------------%
  InvertedDataMgrRealtimeConnections = invert_dictionary(DataMgrRealtimeConnections),
  InvertedRtSinkRealtimeConnections = build_inverted_realtime_connections_from_rtsink(SourceNodes, SinkNodes),


  SinkRTCKeys = dict:fetch_keys(InvertedDataMgrRealtimeConnections),
  ActualSinkConnectionCounts = lists:sort(count_primary_connections(InvertedDataMgrRealtimeConnections, SinkRTCKeys, [])),
  ExpectedSinkConnectionCounts = lists:sort(build_expected_primary_connection_counts(for_sink_nodes, SourceNodes, SinkNodes)),


  lager:info("inverted realtime connections (data_mgr) ~p", [InvertedDataMgrRealtimeConnections]),
  lager:info("inverted realtime connections (conn_mgr) ~p", [InvertedRtSinkRealtimeConnections]),
  lager:info("(actual) sink connection count ~p", [ActualSinkConnectionCounts]),
  lager:info("(expected) sink connection count ~p", [ExpectedSinkConnectionCounts]),

  ?assertEqual(ExpectedSinkConnectionCounts, ActualSinkConnectionCounts),
  ok.


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
              [Base+1 || _ <-lists:seq(1,NumberOfNodesWithOneAdditionalConnection)] ++ [Base || _
                <- lists:seq(1,NumberOfNodesWithBaseConnections)]
          end
      end
  end.


count_primary_connections(_ConnectionDictionary, [], List) ->
  List;
count_primary_connections(ConnectionDictionary, [Key|Keys], List) ->
  NodeConnections = dict:fetch(Key, ConnectionDictionary),
  count_primary_connections(ConnectionDictionary, Keys, List ++ [get_primary_count(NodeConnections,0)]).

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
  NewDict = dict:store(SourceNode, dict:fetch_keys(Endpoints), Dict),
  build_realtime_connections_from_conn_mgr_helper(Rest, NewDict).










build_realtime_connections_from_rtsource_conn(SourceNodes) ->
  build_realtime_connections_from_rtsource_conn_helper(SourceNodes, dict:new()).

build_realtime_connections_from_rtsource_conn_helper([], Dict) ->
  Dict;
build_realtime_connections_from_rtsource_conn_helper([SourceNode| Rest], Dict) ->
  [{"B",ConnMgrPid}] = rpc:call(SourceNode, riak_repl2_rtsource_conn_sup, enabled, []),
  RtSourceConnPids = rpc:call(SourceNode, riak_repl2_rtsource_conn_mgr, get_rtsource_conn_pids, [ConnMgrPid]),
  ConnsList = [ rpc:call(SourceNode, riak_repl2_rtsource_conn, get_address, [Pid]) || Pid <- RtSourceConnPids],
  NewDict = dict:store(SourceNode, ConnsList, Dict),
  build_realtime_connections_from_conn_mgr_helper(Rest, NewDict).










build_inverted_realtime_connections_from_rtsink(SourceNodes, SinkNodes) ->
  Dict1 = build_rtsink_dictionary(SinkNodes, dict:new()),
  Dict2 = build_rtsource_dictionary(SourceNodes, dict:new()),
  make_realtime_connection_data(Dict1,Dict2).


build_rtsink_dictionary([], Dict) ->
  Dict;
build_rtsink_dictionary([Sink|Rest], Dict) ->
  SinkPids = rpc:call(Sink, riak_repl2_rtsink_conn_sup, started, []),
  Key = rpc:call(Sink, app_helper, get_env, [riak_core, cluster_mgr]),
  build_rtsink_dictionary(Rest, build_dict_with_rtsink_peernames(SinkPids, Sink, Key, Dict)).

build_dict_with_rtsink_peernames([], _, _, Dict) ->
  Dict;
build_dict_with_rtsink_peernames([Pid|Rest], Sink, Key, Dict) ->
  Peername = rpc:call(Sink, riak_repl2_rtsink_conn, get_peername, [Pid]),
  NewDict = dict:append(Key, Peername, Dict),
  build_dict_with_rtsink_peernames(Rest, Sink, Key, NewDict).



build_rtsource_dictionary([], Dict) ->
  Dict;
build_rtsource_dictionary([SourceNode| Rest], Dict) ->
  [{"B",ConnMgrPid}] = rpc:call(SourceNode, riak_repl2_rtsource_conn_sup, enabled, []),
  PeernamePrimary = [
    rpc:call(SourceNode, riak_repl2_rtsource_conn, get_socketname_primary, [Pid]) ||
    Pid <- rpc:call(SourceNode, riak_repl2_rtsource_conn_mgr, get_rtsource_conn_pids, [ConnMgrPid])],
  build_rtsource_dictionary(Rest, build_dict_with_rtsource_node_names(PeernamePrimary, SourceNode, Dict)).

build_dict_with_rtsource_node_names([], _, Dict) ->
  Dict;
build_dict_with_rtsource_node_names([{Peername, Primary}|Rest], SourceNode, Dict) ->
  NewDict = dict:store(Peername, {SourceNode, Primary}, Dict),
  build_dict_with_rtsource_node_names(Rest, SourceNode, NewDict).


make_realtime_connection_data(SinkDict, SourceDict) ->
  RealtimeConnections = dict:new(),
  Keys = dict:fetch_keys(SinkDict),
  populate_realtime_connections(Keys, SinkDict, SourceDict, RealtimeConnections).

populate_realtime_connections([], _, _, RealtimeConnections) ->
  RealtimeConnections;
populate_realtime_connections([Key|Rest], SinkDict, SourceDict, RealtimeConnections) ->
  Peernames = dict:fetch(Key, SinkDict),
  NewRealtimeConnections = populate_realtime_connections_helper({Key, Peernames}, SourceDict, RealtimeConnections),
  populate_realtime_connections(Rest, SinkDict, SourceDict, NewRealtimeConnections).

populate_realtime_connections_helper({_, []}, _, RealtimeConnections) ->
  RealtimeConnections;
populate_realtime_connections_helper({SinkIPPort, [Key|Rest]}, SourceDict, RealtimeConnections) ->
  {SourceNode, Primary} = dict:fetch(Key, SourceDict),
  NewRealtimeConnections = dict:append(SinkIPPort, {SourceNode, Primary}, RealtimeConnections),
  populate_realtime_connections_helper({SinkIPPort, Rest}, SourceDict, NewRealtimeConnections).