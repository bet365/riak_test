%%%-----------------------------------------------------------------------------
%%% @doc
%%% Verifying the AAE deletion and expiry functionality
%%% When a key is deleted it recieves an expiry, and is deleted in the
%%% hashtree and ignored when retrieving keys from bitcask.
%%% @end
%%%-----------------------------------------------------------------------------
-module(verify_aae_expiry).
-export([confirm/0, verify_aae_expiry/1,exchange_segment/3]).
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 8).
-define(AAE_THROTTLE_LIMITS, [{-1, 0}, {100, 10}]).
-define(CONFIG,
    [{riak_kv,
        [
            {anti_entropy, {on, []}},
            {anti_entropy_build_limit, {100, 1000}},
            {anti_entropy_concurrency, 100},
            {anti_entropy_expire, 24 * 60 * 60 * 1000}, % Not for now!
            {anti_entropy_tick, 500},
            {aae_throttle_limits, ?AAE_THROTTLE_LIMITS},
            {backend_reap_threshold, 60*60}
        ]},
        {riak_core,
            [
                {ring_creation_size, ?DEFAULT_RING_SIZE}
            ]}]
).
-define(NUMBER_OF_NODES, 3).
-define(N_VAL, 3).

confirm() ->
    Nodes = rt:build_cluster(?NUMBER_OF_NODES, ?CONFIG),
    rt:wait_until_transfers_complete(Nodes),
    verify_throttle_config(Nodes),
    verify_aae_expiry(Nodes),
    pass.

verify_throttle_config(Nodes) ->
    lists:foreach(
        fun(Node) ->
            ?assert(rpc:call(Node,
                riak_kv_entropy_manager,
                is_aae_throttle_enabled,
                [])),
            ?assertMatch(?AAE_THROTTLE_LIMITS,
                rpc:call(Node,
                    riak_kv_entropy_manager,
                    get_aae_throttle_limits,
                    []))
        end,
        Nodes).

verify_aae_expiry(Nodes) ->
    Node1 = hd(Nodes),

    % Verify that AAE eventually upgrades to version 0
    wait_until_hashtree_upgrade(Nodes),

    T = ets:new(ets_table, [named_table, duplicate_bag]),

    %%% {Bucket, Number to create, Number to delete}
    TestDataOptions =
        [{<<"test-bucket-1">>,1000,100},
         {<<"test-bucket-2">>,2000,250},
         {<<"test-bucket-3">>,3000,600},
         {<<"test-bucket-4">>,4000,1500},
         {<<"test-bucket-5">>,1500,1}],

    lager:info("Creating Test Data"),
    {ToInsert, ToDelete} = make_test_data(TestDataOptions),
    ets_insert(T,ToInsert,?N_VAL),

    lager:info("Writing Test Data to Riak Nodes"),
    write_data(Node1,ToInsert,[{n_val,1}]),
    verify_data(Node1,ToInsert),

    lager:info("Delete the keys from riak"),
    ets_delete(T, ToDelete),

    delete_kvs(Node1, ToDelete),

    ToVerify = get_remaining(T),

    verify_data(Node1,ToVerify),

    stop_exchanges(Nodes) ,
    reset_anti_entropy_expire(Nodes),
    HashtreePids = retrieve_hashtree_pids(Node1),
    HashtreesIds = get_node_hashtrees(HashtreePids),
    lager:info("Get data from the hashtrees"),
    IndexTables = get_key_hashes(HashtreesIds),
    %% spawn off and insert the into ets

    wait_until_ets_full(IndexTables),

    HashTables = [HT || {_,HT} <- IndexTables],
    ets_compare(HashTables, T),

    pass.

wait_until_hashtree_upgrade(Nodes) ->
    lager:info("Verifying AAE hashtrees eventually all upgrade to version 0"),
    rt:wait_until(fun() -> all_hashtrees_upgraded(Nodes) end).

all_hashtrees_upgraded(Nodes) when is_list(Nodes) ->
    [Check|_] = lists:usort([all_hashtrees_upgraded(Node) || Node <- Nodes]),
    Check;

all_hashtrees_upgraded(Node) when is_atom(Node) ->
    case rpc:call(Node, riak_kv_entropy_manager, get_version, []) of
        0 ->
            Trees = rpc:call(Node, riak_kv_entropy_manager, get_trees_version, []),
            case [Idx || {Idx, undefined} <- Trees] of
                [] ->
                    true;
                _ ->
                    false
            end;
        _ ->
            false
    end.

make_test_data(TestDataOptions) ->
    lists:foldl(
        fun({Bucket,NumCreate,NumToBeDeleted},{ToInsertAcc, ToDeleteAcc}) ->
            % make kvs
            KeyValues = test_data(1,NumCreate),
            ToBeDeleted = pick_keys_to_delete(KeyValues, NumToBeDeleted),
            {[{Bucket,KeyValues}|ToInsertAcc],
                [{Bucket,ToBeDeleted}|ToDeleteAcc]}
        end, {[],[]},TestDataOptions).

test_data(Start, End) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, K} || K <- Keys].

to_key(N) ->
    list_to_binary(io_lib:format("K~4..0B", [N])).

ets_insert(_, _, 0) ->
    ok;
ets_insert(T, Data, HowManyTimes) ->
    lists:foreach(
        fun({Bucket, KVs}) ->
            ets:insert(T, [{{Bucket, K}, V} || {K,V} <- KVs])
        end, Data),
    ets_insert(T,Data,HowManyTimes-1).

write_data(Node, ToInsert, Opts) ->
    lists:foreach(
        fun({Bucket, KVs}) ->
            write_data(Node, Bucket, KVs, Opts)
        end, ToInsert).
write_data(Node, Bucket, KVs, Opts) ->
    PB = rt:pbc(Node),
    [begin
         O =
             case riakc_pb_socket:get(PB, Bucket, K) of
                 {ok, Prev} ->
                     riakc_obj:update_value(Prev, V);
                 _ ->
                     riakc_obj:new(Bucket, K, V)
             end,
         ?assertMatch(ok, riakc_pb_socket:put(PB, O, Opts))
     end || {K, V} <- KVs],
    riakc_pb_socket:stop(PB),
    ok.

% @doc Verifies that the data is eventually restored to the expected set.
verify_data(Node, ToVerify) ->
    lists:foreach(
        fun({Bucket, KVs}) ->
            verify_data(Node, Bucket, KVs)
        end, ToVerify).
verify_data(Node, Bucket, KeyValues) ->
    lager:info("Verify all replicas are eventually correct"),
    PB = rt:pbc(Node),
    CheckFun =
        fun() ->
            Matches = [verify_replicas(Node, Bucket, K, V, ?N_VAL)
                || {K, V} <- KeyValues],
            CountTrues = fun(true, G) -> G+1; (false, G) -> G end,
            NumGood = lists:foldl(CountTrues, 0, Matches),
            Num = length(KeyValues),
            case Num == NumGood of
                true -> true;
                false ->
                    lager:info("Data not yet correct: ~p mismatches",
                        [Num-NumGood]),
                    false
            end
        end,
    MaxTime = rt_config:get(rt_max_wait_time),
    Delay = 2000, % every two seconds until max time.
    Retry = MaxTime div Delay,
    case rt:wait_until(CheckFun, Retry, Delay) of
        ok ->
            lager:info("Data is now correct. Yay!");
        fail ->
            lager:error("AAE failed to fix data"),
            ?assertEqual(aae_fixed_data, aae_failed_to_fix_data)
    end,
    riakc_pb_socket:stop(PB),
    ok.

verify_replicas(Node, B, K, V, N) ->
    Replies = [rt:get_replica(Node, B, K, I, N)
        || I <- lists:seq(1,N)],
    Vals = [merge_values(O) || {ok, O} <- Replies],
    Expected = [V || _ <- lists:seq(1, N)],
    Vals == Expected.

merge_values(O) ->
    Vals = riak_object:get_values(O),
    lists:foldl(fun(NV, V) ->
        case size(NV) > size(V) of
            true -> NV;
            _ -> V
        end
                end, <<>>, Vals).

pick_keys_to_delete(KVs, ToDelete) ->
    RandomisedList = [X||{_,X} <- lists:sort([{random:uniform(), N}||N <- KVs])],
    {DeleteTheseKeys, _} = lists:split(ToDelete, RandomisedList),
    DeleteTheseKeys.

ets_delete(T, ToDelete) ->
    lists:foreach(
        fun({Bucket, KVs}) ->
            [ets:delete(T, {Bucket, K}) || {K, _} <- KVs]
        end, ToDelete).

delete_kvs(Node, ToDelete) ->
    lists:foreach(
        fun({Bucket, KVs}) ->
            delete_kvs(Node, Bucket, KVs)
        end, ToDelete).
delete_kvs(Node, Bucket, KVs) ->
    PB = rt:pbc(Node),
    lists:foreach(fun({K,_}) -> riakc_pb_socket:delete(PB, Bucket, K, [{n_val,1}]) end,KVs).

get_remaining(T) ->
    List = ets:tab2list(T),
    lists:foldl(
        fun({{B, K}, V},Acc) ->
            case lists:keyfind(B,1,Acc) of
                false ->
                    [{B,[{K,V}]}|Acc];
                {B, KVs} ->
                    lists:keyreplace(B,1,Acc,{B,[{K,V}|KVs]})
            end
        end, [], List).


stop_exchanges(Nodes) ->
    lager:info("Stop exchanges"),
    rpc:multicall(Nodes, riak_kv_entropy_manager, set_mode, [manual]),
    rpc:multicall(Nodes, riak_kv_entropy_manager, cancel_exchanges,[]).

reset_anti_entropy_expire(Nodes) ->
    lager:info("Make sure the anti_entropy expire time is significant"),
    rpc:multicall(Nodes, application, set_env, [riak_kv, anti_entropy_expire, 24 * 60 * 60 * 1000]).

retrieve_hashtree_pids(Node) ->
    {ok, R} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    Owners = riak_core_ring:all_owners(R),
    Pids = get_hashtree_pids(Owners),
    Pids.

get_hashtree_pids(Owners) ->
    lists:foldl(fun({Index, Node}, Acc) ->
        case rpc:call(Node, riak_kv_vnode, hashtree_pid, [Index]) of
            {ok, Pid} ->
                [{Pid, Node}|Acc];
            _ ->
                error
        end
                end, [], Owners).

get_node_hashtrees(HashtreePids) ->
    lists:map(
        fun({Pid, _}) ->
            Trees = riak_kv_index_hashtree:get_trees({test,Pid}),
            [{Index,Pid} || {Index, _TreeState} <- Trees]
        end, HashtreePids).

get_key_hashes(HashtreesIds) ->
    lists:foldl(fun({Index, Pid}, Acc) ->
            [exchange_segment(Index,Pid)|Acc]
                end, [], lists:flatten(HashtreesIds)).

exchange_segment(Index,Pid) ->
    {P,_} = Index, I = integer_to_list(P), N = "hashtree_"++I,
    Q = list_to_atom(N),
    T = ets:new(Q, [public, {write_concurrency,true},{read_concurrency,true}]),
    Proc = spawn(verify_aae_expiry, exchange_segment, [Index, Pid, T]),
    {Proc,T}.

exchange_segment(Index,Pid,T) ->
    lists:foreach(fun(N) ->
        case riak_kv_index_hashtree:exchange_segment(Index,N,Pid) of
            [] -> ok;
            KHs ->
                Keys = decode_keys(KHs),
                ets:insert(T, Keys)
        end
                  end, lists:seq(1,1024*1024)).

decode_keys(KHs) ->
    List = [begin {_,_,SK} = hashtree:safe_decode(K), binary_to_term(SK) end || {K,_} <- KHs],
    separate_kvs(List).

separate_kvs(KeyHashes) ->
    [{{B,K},K} || {B,K} <- KeyHashes].

ets_compare(IndexTables, NormalTable) ->
    List1 = ets:tab2list(NormalTable),
    lager:info("Control ETS Size = ~p", [length(List1)]),
    SList1 = lists:sort(List1),
    Final = lists:foldl(
                fun(T,Acc) ->
                    List = ets:tab2list(T),
                    lager:info("HastreeList : ~p",[length(List)]),
                    List++Acc
                end, [], IndexTables),
    lager:info("Hashtree ETS total Size : ~p", [length(Final)]),
    SList2 = lists:sort(Final),
    ?assert(length(List1)==length(Final)),
    ?assert(SList1 == SList2).

wait_until_ets_full(Tables) ->
    lager:info("Wait until processes finished uploading into ets"),
    lager:info("This could take some time ~10 mins"),
    [ets_table_full(Process) || {Process,_} <- Tables].

ets_table_full(Process) ->
    rt:wait_until(fun() -> is_process_alive(Process) == false end, 1000, 10000).
