-module(verify_bitcask_merge_with_object_expiratrion).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").


confirm() ->

    %% 1. make cluster
    %% allow mult = false
    %% merge_policy = never
    %% max file size = 500KB
    Cluster = make_cluster(),
%%    timer:sleep(60000),

    %% 2. Placement
    lager:info("start placement"),
    placement(Cluster),
    timer:sleep(10000),

    %% trigger merges by  stopping and starting the nodes
    [rt:stop(Node) || Node <- Cluster],
    [rt:start(Node) || Node <- Cluster],
    set_config(Cluster),
    lager:info("waiting for leader to converge on cluster 1"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(Cluster)),
    timer:sleep(240000),

    %% check the integrity of the data
    AllData = get_all_data(Cluster),
    check_data(AllData),
    check_data(AllData).




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

make_cluster() ->
    Conf = [

        {riak_kv,
            [
                {backend_reap_threshold, 10}
            ]}
    ],
    Nodes = rt:deploy_nodes(8, Conf, [riak_kv, riak_repl]),
    lager:info("Build cluster 1"),
    repl_util:make_cluster(Nodes),

    lager:info("waiting for leader to converge on cluster 1"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(Nodes)),

    set_config(Nodes),

    Nodes.

set_config(Nodes) ->
    DefaultBucketProps =
        [
            {allow_mult,false},
            {basic_quorum,false},
            {big_vclock,50},
            {chash_keyfun,{riak_core_util,chash_std_keyfun}},
            {dvv_enabled,false},
            {dw,quorum},
            {last_write_wins,false},
            {linkfun,{modfun,riak_kv_wm_link_walker,mapreduce_linkfun}},
            {n_val,3},
            {notfound_ok,true},
            {old_vclock,86400},
            {postcommit,[]},
            {pr,0},
            {precommit,[]},
            {pw,0},
            {r,quorum},
            {rw,quorum},
            {small_vclock,50},
            {w,quorum},
            {young_vclock,20}],

    [rpc:call(Node, application, set_env, [riak_core, default_bucket_props, DefaultBucketProps]) || Node <- Nodes],
    [rpc:call(Node, application, set_env, [bitcask, merge_window, always]) || Node <- Nodes],
    [rpc:call(Node, application, set_env, [bitcask, max_file_size, 512000]) || Node <- Nodes].

placement(Nodes) ->
    {ok, C} = riak:client_connect(hd(Nodes)),
    F = fun(N) ->
        Res = [
            C:put(riak_object:new(<<"bucket">>, <<N:32/integer>>, <<"not value">>)),
%%            timer:sleep(20),
            C:put(riak_object:new(<<"bucket">>, <<N:32/integer>>, <<"value">>))],
%%            timer:sleep(20),
        case (N rem 2) of
            0 ->
                [C:delete(<<"bucket">>, <<N:32/integer>>) | Res];
            1-> Res
        end
        end,
    Out = [F(N) || N <- lists:seq(0, 50000)],
    Out1 = lists:flatten(Out),
    Out2 = [X || X <- Out1, X/=ok],
    lager:info("Placement ~p ", [Out2]).


get_all_data(Nodes) ->
    rt:wait_for_service(hd(Nodes), riak_kv),
    {ok, C} = riak:client_connect(hd(Nodes)),
    [C:get(<<"bucket">>, <<N:32/integer>>) || N <- lists:seq(1, 50000)].


check_data(Data) ->
    NotFound = [X|| X <- Data, X == {error, notfound}],
    Found =  [X|| X <- Data, X /= {error, notfound}],
    FoundValues = [begin {ok, Obj} = X, {check_object(Obj), Obj} end || X <- Found],
    CorrectValues = [ Obj || {Check, Obj} <- FoundValues, Check == true],
    IncorrectValues = [ Obj || {Check, Obj} <- FoundValues, Check == false],

    lager:info("length of data ~p", [length(Data)]),
    lager:info("length of Founds ~p", [length(Found)]),
    lager:info("length of NotFounds ~p", [length(NotFound)]),
    lager:info("length of CorrectValues ~p", [length(CorrectValues)]),
    lager:info("length of IncorrectValues ~p", [length(IncorrectValues)]),

    pass.


check_object(Obj) ->
    case riak_object:get_value(Obj) of
        <<"value">> -> true;
        _ -> false
    end.