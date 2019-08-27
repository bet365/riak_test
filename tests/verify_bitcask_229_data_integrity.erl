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
    copy_bitcask_test_data(),

    timer:sleep(60000),


    %% make a one node cluster
    make_one_node_cluster(),

    %% check we can read all of the data
    check_data_values(),

    pass.







check_data_values() ->
    ok.





copy_bitcask_test_data() ->
    DataDir = rtdev:get_riak_data_dir("dev1"),
    TestDir = rtdev:get_riak_test_data_dir() ++ "riak-2.2.9-test-data/bitcask",
    os:cmd(io_lib:format("cp -r ~s ~s", [DataDir, TestDir])),
    ok.





make_one_node_cluster() ->
    Conf = [
        {riak_repl,
            [
                %% turn off fullsync
                {backend_reap_threshold, 60},
                {default_bucket_props, [{n_val, 1}, {allow_mult, false}]},
                {override_capability,
                    [{default_bucket_props_hash, [{use, [consistent, datatype, n_val, allow_mult, last_write_wins]}]}]}
            ]
        },

        {riak_kv,
            [

                {override_capability, [{object_hash_version, [{use, legacy}]}]}

            ]
        },

        {riak_core,
            [
                {ring_creation_size,8}
            ]
        }
    ],

    Nodes = rt:deploy_nodes(8, Conf, [riak_kv, riak_repl]),
    Node = hd(Nodes),
    lager:info("Build 1 node cluster"),
    rt:wait_until_ring_converged(Nodes),
    rt:wait_until_transfers_complete(Nodes),
    Node.
