-module(verify_bitcask_229_data_integrity).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").



%% plan setup a 1 node cluster
%% copy and paste from test_data dir, data from 2.0.6 and 2.2.8 into the data dir of the 1 node cluster
%% load the data, verify that we can read all data
%% simple test, but needs to be done!


confirm() ->

    %% copy test data to riak data dir
    lager:info("COPYING data over from test_data directory to data directory for dev1"),
    copy_bitcask_test_data(),

    %% make a one node cluster
    lager:info("MAKING a one node cluster"),
    Node = make_one_node_cluster(),

    %% check we can read all of the data
    lager:info("CHECKING the values are correct from 2.0.6 and 2.2.8 version of data"),
    ?assertEqual(true, check_data_values(Node, <<"value-">>)),

    %% over write all the data
    lager:info("OVERWRITTING the data to all be 2.2.9 version of the data (it is actually the same as 2.2.8 just different logic)"),
    overwrite_data(Node, <<"overwritten-value-">>),

    %% check we can read all of the data again
    lager:info("CHECKING the overwritten data has taken effect and no old values are present"),
    ?assertEqual(true, check_data_values(Node, <<"overwritten-value-">>)),

    %% trigger a merge
    lager:info("WAITING for merge to take place"),
    rpc:call(Node, application, set_env, [bitcask, merge_window, always], 10000),
    timer:sleep(20000),
    lager:info("MERGE should now be complete"),

    %% stop the node
    lager:info("STOPPING Node ~p", [Node]),
    rt:stop_and_wait(Node),

    %% start the node
    lager:info("STARTING Node ~p", [Node]),
    rt:start(Node),
    rt:wait_until_ring_converged([Node]),
    rt:wait_until_transfers_complete([Node]),

    lager:info("CHECKING the data after the merge, ensuring there is no old data"),
    ?assertEqual(true, check_data_values(Node, <<"overwritten-value-">>)),

    pass.






check_data_values(Node, ValuePrefix) ->
    check_data(Node, <<"bucket-206">>, ValuePrefix) and
        check_data(Node, <<"bucket-228">>, ValuePrefix).

check_data(Node, Bucket, ValuePrefix) ->
    {ok, C} = riak:client_connect(Node),
    lists:foldl(
        fun(N, Res)->
            NN = integer_to_binary(N),
            {ok, O} = C:get(Bucket, <<"key-", NN/bitstring>>),
            ActualValue = riak_object:get_value(O),
            ExpectedValue = <<ValuePrefix/bitstring, NN/bitstring>>,
            (ActualValue == ExpectedValue) and Res
        end,
        true, lists:seq(1, 10000)).

overwrite_data(Node, ValuePrefix) ->
    overwrite_data(Node, <<"bucket-206">>, ValuePrefix),
    overwrite_data(Node, <<"bucket-228">>, ValuePrefix).

overwrite_data(Node, Bucket, ValuePrefix) ->
    {ok, C} = riak:client_connect(Node),
    lists:foreach(
        fun(N)->
            NN = integer_to_binary(N),
            Key = <<"key-", NN/bitstring>>,
            {ok, O} = C:get(Bucket, Key),
            Vclock = riak_object:vclock(O),
            NewObj = riak_object:new(Bucket, Key, <<ValuePrefix/bitstring, NN/bitstring>>, Vclock),
            C:put(NewObj)
        end,
        lists:seq(1, 10000)).




copy_bitcask_test_data() ->
    TestDir = rtdev:get_riak_test_data_dir() ++ "/riak-2.2.9-test-data/bitcask",
    DataDir = rtdev:get_riak_data_dir("dev1"),
    Command = io_lib:format("cp -r ~s ~s", [TestDir, DataDir]),
    os:cmd(Command),
    ok.





make_one_node_cluster() ->
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
                    {backend_reap_threshold, 60},
                    {override_capability,
                        [{default_bucket_props_hash, [{use, [consistent, datatype, n_val, allow_mult, last_write_wins]}]}]}
                ]
            },

            {riak_kv,
                [

                    {override_capability, [{object_hash_version, [{use, legacy}]}]},
                    {bitcask_merge_check_interval, 1000},
                    {bitcask_merge_check_jitter, 0}

                ]
            },

            {bitcask,
                [
                    {merge_window, never}
                ]
            }
        ],

    Nodes = rt:deploy_nodes(8, Conf, [riak_kv, riak_repl]),
    Node = hd(Nodes),
    lager:info("Build 1 node cluster"),
    rt:wait_until_ring_converged(Nodes),
    rt:wait_until_transfers_complete(Nodes),
    Node.
