%%%-------------------------------------------------------------------
%%% @doc
%%% Verifying the AAE deletion and expiry functionality
%%% When a key is deleted it recieves an expiry, and is deleted in the
%%% hashtree and ignored when retrieving keys.
%%% @end
%%%-------------------------------------------------------------------
-module(verify_aae_expiry).

-include_lib("eunit/include/eunit.hrl").

-export([verify/0]).

%% Config MACROS
%% riak_core
-define(RING_SIZE,           {ring_creation_size,8}).
%% riak_kv
-define(AAE_BUILD_LIMIT,     {anti_entropy_build_limit, {100,1000}}).
-define(AAE_CONCURRENCY,     {anti_entropy,concurrency,100}).
-define(AAE_EXPIRY,          {anti_entropy_expire, 24*60*60*1000}). %% a day
-define(AAE_TICK,            {anti_entropy_tick, 20}).
-define(THROTTLE_LIMITS,     [{-1,0},{100,10}]).
-define(AAE_THROTTLE_LIMITS, {aae_throttle_limits, ?THROTTLE_LIMITS}).

-define(CONFIG, [
    {riak_kv,
    [{anti_entropy, {on,[]}},
        ?AAE_BUILD_LIMIT,
        ?AAE_CONCURRENCY,
        ?AAE_EXPIRY,
        ?AAE_TICK,
        ?AAE_THROTTLE_LIMITS]},
    {riak_core, [?RING_SIZE]}]).

-define(NUMBER_OF_NODES, 4).
-define(NUMBER_OF_KEYS, 1000).
-define(BUCKET, <<"test-bucket">>).
-define(N_VAL, 3).

verify() ->
    %%%%%%%%%%%%%%% Set up cluster %%%%%%%%%%%%%%%%%%%%
    %% Build the cluster with the config given
    Nodes = rt:build_cluster(?NUMBER_OF_NODES, ?CONFIG),
    %% make sure the cnofig is set on all the nodes
    %% set aee_config
    verify_config_on_nodes(Nodes),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    wait_until_hashtree_upgrade(Nodes),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    KeyValues = key_values(1,N=?NUMBER_OF_KEYS),
    %% put data into riak
    Clients = write_kvs(KeyValues,Nodes),

    [C1|_] = Clients,
    replicas_with_less_than_n_val(KeyValues,C1),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    KeyValues2 = [{K,<<V/binary, "x">>} || {K,V} <- KeyValues],
    replicas_with_less_than_n_mods(KeyValues2, C1),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    KeyValues3 = key_values(N+1,M = N+?NUMBER_OF_KEYS),  % next 1000
    replicas_with_less_than_n_writes(C1, KeyValues3),
    %%% as above but for this second set of KVs,
    KeyValues4 = [{K,<<V/binary, "y">>} || {K,V} <- KeyValues3],
    replicas_with_less_than_n_mods(KeyValues4, C1),

    %% on to the next set
    ?debugFmt("Writing in the next 1000 objects"),
    KeyValues5 = key_values(M+1, M+?NUMBER_OF_KEYS),
    {C,N1} = C1,
    write_data(KeyValues5, C),

    % Test recovery from single partition loss.
    {PNuke, NNuke} = choose_partition_to_nuke(N1, ?BUCKET, KeyValues5),
    test_single_partition_loss(NNuke, PNuke, KeyValues5),

    % Test recovery from losing AAE data
    test_aae_partition_loss(NNuke, PNuke, KeyValues5),

    % Test recovery from losing both AAE and KV data
    test_total_partition_loss(NNuke, PNuke, KeyValues5),

    % Make sure AAE repairs die down.
    wait_until_no_aae_repairs(Nodes),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    AllKeyValues = KeyValues++KeyValues2++KeyValues3++
                   KeyValues4++KeyValues5,
    ToBeDeleted = pick_a_bunch_of_random_keys(AllKeyValues),
    NumDeleted = delete_kvs(ToBeDeleted, Clients),

    rebuild_tree_in_one_node(N1),
    NumRepairs = wait_until_no_aae_repairs(Nodes),

    ?assertEqual(NumDeleted, NumRepairs),
    pass.

choose_partition_to_nuke(Node, Bucket, KeyValues) ->
    Preflists = [get_preflist(Node, Bucket, K) || {K, _} <- KeyValues],
    PCounts = lists:foldl(fun acc_preflists/2, dict:new(), Preflists),
    CPs = [{C, P} || {P, C} <- dict:to_list(PCounts)],
    {_, MaxP} = lists:max(CPs),
    MaxP.

test_single_partition_loss(Node, Partition, KeyValues)
    when is_atom(Node), is_integer(Partition) ->
    ?debugFmt("Verify recovery from the loss of partition ~p", [Partition]),
    wipe_out_partition(Node, Partition),
    restart_vnode(Node, riak_kv, Partition),
    verify_data(Node, KeyValues).

test_aae_partition_loss(Node, Partition, KeyValues)
    when is_atom(Node), is_integer(Partition) ->
    ?debugFmt("Verify recovery from the loss of AAE data for partition ~p", [Partition]),
    wipe_out_aae_data(Node, Partition),
    restart_vnode(Node, riak_kv, Partition),
    verify_data(Node, KeyValues).

test_total_partition_loss(Node, Partition, KeyValues)
    when is_atom(Node), is_integer(Partition) ->
    ?debugFmt("Verify recovery from the loss of AAE and KV data for partition ~p", [Partition]),
    wipe_out_partition(Node, Partition),
    wipe_out_aae_data(Node, Partition),
    restart_vnode(Node, riak_kv, Partition),
    verify_data(Node, KeyValues).

wipe_out_partition(Node, Partition) ->
    ?debugFmt("Wiping out partition ~p in node ~p", [Partition, Node]),
    rt:clean_data_dir(Node, dir_for_partition(Partition)),
    ok.

dir_for_partition(Partition) ->
    TestMetaData = riak_test_runner:metadata(),
    KVBackend = proplists:get_value(backend, TestMetaData),
    BaseDir = base_dir_for_backend(KVBackend),
    filename:join([BaseDir, integer_to_list(Partition)]).

base_dir_for_backend(undefined) ->
    base_dir_for_backend(bitcask);
base_dir_for_backend(bitcask) ->
    "bitcask";
base_dir_for_backend(eleveldb) ->
    "leveldb".

wipe_out_aae_data(Node, Partition) ->
    ?debugFmt("Wiping out AAE data for partition ~p in node ~p", [Partition, Node]),
    rt:clean_data_dir(Node, "anti_entropy/"++integer_to_list(Partition)),
    ok.

restart_vnode(Node, Service, Partition) ->
    VNodeName = list_to_atom(atom_to_list(Service) ++ "_vnode"),
    {ok, Pid} = rpc:call(Node, riak_core_vnode_manager, get_vnode_pid,
        [Partition, VNodeName]),
    ?assert(rpc:call(Node, erlang, exit, [Pid, kill_for_test])),
    Mon = monitor(process, Pid),
    receive
        {'DOWN', Mon, _, _, _} ->
            ok
    after
        rt_config:get(rt_max_wait_time) ->
            lager:error("VNode for partition ~p did not die, the bastard",
                [Partition]),
            ?assertEqual(vnode_killed, {failed_to_kill_vnode, Partition})
    end,
    {ok, NewPid} = rpc:call(Node, riak_core_vnode_manager, get_vnode_pid,
        [Partition, VNodeName]),
    ?debugFmt("Vnode for partition ~p restarted as ~p",
        [Partition, NewPid]).

get_preflist(Node, B, K) ->
    DocIdx = rpc:call(Node, riak_core_util, chash_key, [{B, K}]),
    PlTagged = rpc:call(Node, riak_core_apl, get_primary_apl, [DocIdx, ?N_VAL, riak_kv]),
    Pl = [E || {E, primary} <- PlTagged],
    Pl.

acc_preflists(Pl, PlCounts) ->
    lists:foldl(fun(Idx, D) ->
        dict:update(Idx, fun(V) -> V+1 end, 0, D)
                end, PlCounts, Pl).

replicas_with_less_than_n_writes({C, N}, KVs) ->
    ?debugFmt("Writing ~p objects with N=1, AAE esures they end up with ~p replicas",
        [length(KVs)]),
    write_data(KVs, C, [{n_val,1}]),
    verify_data(KVs, N).

verify_config_on_nodes(Nodes) ->
    lists:foreach(fun(Node) ->
        ?assertMatch(true,
            rpc:call(Node, riak_kv_entropy_manager,is_aae_throttle_enabled,[])),
        ?assertMatch(?THROTTLE_LIMITS,
            rpc:call(Node, riak_kv_entropy_manager,get_aae_throttle_limits,[]))
                  end, Nodes).

wait_until_hashtree_upgrade(Nodes) ->
    ?debugFmt("Verifying AAE hashtrees eventually all upgrade to version 0"),
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

replicas_with_less_than_n_val(KeyVals, {C,N}) ->
    ?debugFmt("Writing ~p objects with N=1, AAE makes sure they end up with :
                ~p replicas", [length(KeyVals), ?N_VAL]),
    write_data(KeyVals, C, [{n_val,1}]),
    verify_data(KeyVals, N).

write_data(KeyVals, C) ->
    write_data(KeyVals, C, []).
write_data(KeyVals, C, Opts) ->
    [?assertMatch(ok, C:put(riak_object:new(?BUCKET, K, V, Opts))) || {K,V} <- KeyVals].

verify_data(KeyVals, Node) ->
    ?debugFmt("Verify the replicas are eventually correct"),
    Fun = fun() ->
        Matches = [verify_replicas(Node, ?BUCKET, K, V, ?N_VAL) || {K, V} <- KeyVals],
        CountTrues = fun(true, G) -> G+1; (false, G) -> G end,
        GoodNum = lists:foldl(CountTrues, 0, Matches),
        Num = length(KeyVals),
            case Num == GoodNum of
                true -> true;
                false -> ?debugFmt("Data not correct: ~p mismatches", [Num-GoodNum]),
                    false
            end
          end,
    Maxtime = rt_config:get(rt_max_wait_time),
    Delay = 2000,
    Retry = Maxtime div Delay ,
    Response =
        case rt:wait_until(Fun, Retry, Delay) of
            ok ->
                ?debugFmt("Data is good"), ok;
            fail ->
                ?debugFmt("AAE failed to fix data."), fail
        end,
    ?assertEqual(Response, ok),
    ok.

verify_replicas(Node, Buck, Key, Val, N_Val) ->
    Replies = [rt:get_replica(Node, Buck, Key, Val, N_Val) || _ <- lists:seq(1,N_Val)],
    Vals = [merge_vals(Z) || {ok, Z} <- Replies],
    Expected = [Val || _ <- lists:seq(1, N_Val)],
    ?assertEqual(Vals, Expected).

merge_vals(Z) ->
    Vals = riak_object:get_values(Z),
    lists:foldl(fun(NV, V) ->
        case size(NV) > size(V) of
            true -> NV;
            false -> V
        end
                end, <<>>, Vals).

replicas_with_less_than_n_mods(KeyVals, {C,N}) ->
    ?debugFmt("Modify only the one replica for ~p Objects, AAE should
    ensure all replicas end up modified", [length(KeyVals)]),
    write_data(KeyVals, C, [{n_val,1}]),
    verify_data(KeyVals, N).

rebuild_tree_in_one_node(Node) ->
    rpc:call(Node, application, set_env, [riak_kv, anti_entropy_expire, 15*1000]).

key_values(Min,Max) ->
    [{key(N),val(N)} || N <- lists:seq(Min,Max)].

key(N) -> list_to_binary(io_lib:format("K~4..0B", [N])).
val(N) -> list_to_binary(io_lib:format("V~4..0B", [N])).

write_kvs(KeyValues, Nodes) ->
    lists:map(fun(Node) ->
        {ok, C} = riak:client_connect(Node),
        [?assertMatch(ok,C:put(riak_object:new(?BUCKET, K, V))) || {K,V} <- KeyValues],
        {C, Node}
                  end, Nodes).

pick_a_bunch_of_random_keys(KVs) ->
    N = random:uniform(?NUMBER_OF_KEYS),
    {DeleteTheseKeys, _} = lists:split(N, KVs),
    DeleteTheseKeys.

delete_kvs(DeleteKeys, Clients) ->
    ?debugFmt("Delete a bunch of keys in each of the nodes"),
    Deleted =
    lists:foreach(fun({C,_}) ->
        [?assertMatch(ok,C:delete(?BUCKET, K)) || {K,_} <- DeleteKeys]
                  end, Clients),
    length(Deleted).

wait_until_no_aae_repairs(Nodes) ->
    ?debugFmt("Verifying AAE repairs go away without activity"),
    rt:wait_until(fun() -> no_aae_repairs(Nodes) end).

no_aae_repairs(Nodes) when is_list(Nodes) ->
    MaxCount = max_aae_repairs(Nodes),
    ?debugFmt("Max AAE repair count across the board is ~p", [MaxCount]),
    MaxCount == 0.

max_aae_repairs(Nodes) when is_list(Nodes) ->
    MaxCount = lists:max([max_aae_repairs(Node) || Node <- Nodes]),
    MaxCount;
max_aae_repairs(Node) when is_atom(Node) ->
    Info = rpc:call(Node, riak_kv_entropy_info, compute_exchange_info, []),
    LastCounts = [Last || {_, _, _, {Last, _, _, _}} <- Info],
    MaxCount = lists:max(LastCounts),
    MaxCount.





