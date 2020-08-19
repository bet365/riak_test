%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%% @doc Run a hot backup and a restore
%%
%% Confirm that if the backend is not leveled, then not_supported is
%% returned as expeceted

-module(hotbackup_leveled).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

% I would hope this would come from the testing framework some day
% to use the test in small and large scenarios.
-define(DEFAULT_RING_SIZE, 32).
-define(NUM_NODES, 5).
-define(NUM_KEYS_PERNODE, 5000).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).
-define(DELTA_COUNT, 10).
-define(VAL_FLAG1, "U1").
-define(VAL_FLAG2, "U2").
-define(CFG_NOAAE,
        [{riak_kv,
          [
           {anti_entropy, {off, []}},
           {tictacaae_active, passive}
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).

confirm() ->
    Nodes0 = rt:build_cluster(?NUM_NODES, ?CFG_NOAAE),
    ok = hot_backup(Nodes0, true),
    ok = rt:clean_cluster(Nodes0),

    Nodes1 = rt:build_cluster(?NUM_NODES, ?CFG_NOAAE),
    ok = hot_backup(Nodes1, false),
    pass.

hot_backup(Nodes, FullCoverage) ->
    
    KeyLoadFun = 
        fun(Node, KeyCount) ->
            KVs = test_data(KeyCount,
                                KeyCount + ?NUM_KEYS_PERNODE,
                                list_to_binary(?VAL_FLAG1)),
            ok = write_data(Node, KVs),
            KeyCount + ?NUM_KEYS_PERNODE + 1
        end,

    KeyCount= ?NUM_KEYS_PERNODE * length(Nodes),
    lists:foldl(KeyLoadFun, 1, Nodes),
    lager:info("Loaded ~w objects", [KeyCount]),

    check_objects(hd(Nodes), 1, KeyCount, ?VAL_FLAG1),

    KVBackend = proplists:get_value(backend, riak_test_runner:metadata()),
    test_by_backend(KVBackend, Nodes, FullCoverage).


test_by_backend(undefined, Nodes, FullCoverage) ->
    test_by_backend(bitcask, Nodes, FullCoverage);
test_by_backend(bitcask, Nodes, _FC) ->
    not_supported_test(Nodes);
test_by_backend(eleveldb, Nodes, _Fc) ->
    not_supported_test(Nodes);
test_by_backend(CapableBackend, Nodes, FullCoverage) ->
    lager:info("Clean backup folder if present"),
    rt:clean_data_dir(Nodes, "backup"),
    {CoverNumber, RVal} =
        case FullCoverage of
            true -> {?N_VAL, 2};
            false -> {1, 1}
        end,

    KeyCount= ?NUM_KEYS_PERNODE * length(Nodes),
    lager:info("Testing capable backend ~w", [CapableBackend]),
    {ok, C} = riak:client_connect(hd(Nodes)),

    lager:info("Backup to self to fail"),
    {ok, false} =
        riak_client:hotbackup("./data/leveled/", ?N_VAL, CoverNumber, C),

    lager:info("Backup all nodes to succeed"),
    {ok, true} =
        riak_client:hotbackup("./data/backup/", ?N_VAL, CoverNumber, C),
    
    lager:info("Change some keys"),
    Changes2 = test_data(1, ?DELTA_COUNT, list_to_binary(?VAL_FLAG2)),
    ok = write_data(hd(Nodes), Changes2),
    check_objects(hd(Nodes), 1, ?DELTA_COUNT, ?VAL_FLAG2),

    lager:info("Stop the primary cluster and start from backup"),
    lists:foreach(fun rt:stop_and_wait/1, Nodes),
    rt:clean_data_dir(Nodes, backend_dir()),
    rt:restore_data_dir(Nodes, backend_dir(), "backup/"),
    lists:foreach(fun rt:start_and_wait/1, Nodes),

    rt:wait_for_cluster_service(Nodes, riak_kv),

    lager:info("Confirm changed objects are unchanged"),
    check_objects(hd(Nodes), 1, ?DELTA_COUNT, ?VAL_FLAG1, RVal),
    lager:info("Confirm last 5K unchanged objects are unchanged"),
    check_objects(hd(Nodes), KeyCount - 5000, KeyCount, ?VAL_FLAG1, RVal),
    ok.



not_supported_test(Nodes) ->
    {ok, C} = riak:client_connect(hd(Nodes)),
    lager:info("Backup all nodes to fail"),
    {ok, false} = riak_client:hotbackup("./data/backup/", ?N_VAL, ?N_VAL, C),
    ok.


to_key(N) ->
    list_to_binary(io_lib:format("K~6..0B", [N])).

test_data(Start, End, V) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, <<K/binary, V/binary>>} || K <- Keys].

write_data(Node, KVs) ->
    write_data(Node, KVs, []).

write_data(Node, KVs, Opts) ->
    PB = rt:pbc(Node),
    [begin
         O =
         case riakc_pb_socket:get(PB, ?BUCKET, K) of
             {ok, Prev} ->
                 riakc_obj:update_value(Prev, V);
             _ ->
                 riakc_obj:new(?BUCKET, K, V)
         end,
         ?assertMatch(ok, riakc_pb_socket:put(PB, O, Opts))
     end || {K, V} <- KVs],
    riakc_pb_socket:stop(PB),
    ok.


check_objects(Node, KCStart, KCEnd, VFlag) ->
    check_objects(Node, KCStart, KCEnd, VFlag, 2).

check_objects(Node, KCStart, KCEnd, VFlag, RVal) ->
    V = list_to_binary(VFlag),
    PBC = rt:pbc(Node),
    Opts = [{notfound_ok, false}, {r, RVal}],
    CheckFun = 
        fun(K, Acc) ->
            Key = to_key(K),
            case riakc_pb_socket:get(PBC, ?BUCKET, Key, Opts) of
                {ok, Obj} ->
                    RetValue = riakc_obj:get_value(Obj),
                    ?assertMatch(RetValue, <<Key/binary, V/binary>>),
                    Acc;
                {error, notfound} ->
                    lager:error("Search for Key ~w not found", [K]),
                    [K|Acc]
            end
        end,
    MissedKeys = lists:foldl(CheckFun, [], lists:seq(KCStart, KCEnd)),
    ?assertMatch([], MissedKeys),
    riakc_pb_socket:stop(PBC),
    true.

backend_dir() ->
    TestMetaData = riak_test_runner:metadata(),
    KVBackend = proplists:get_value(backend, TestMetaData),
    backend_dir(KVBackend).

backend_dir(undefined) ->
    %% riak_test defaults to bitcask when undefined
    backend_dir(bitcask);
backend_dir(bitcask) ->
    "bitcask";
backend_dir(eleveldb) ->
    "leveldb";
backend_dir(leveled) ->
    "leveled".