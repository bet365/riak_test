-module(riak_core_apl_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_core_apl_orig).


%%return_cut_apl(DocIdx, _N, Service) ->
%%    ?I_INFO("Intercept is returning the head of the preflist"),
%%    ?M:get_apl_orig(DocIdx, 2, Service).

return_cut_primary_apl(DocIdx, _N, Service) ->
    ?I_INFO("Intercept is returning 2 of the preflist elements"),
    ?M:get_primary_apl_orig(DocIdx, 2, Service).

return_cut_apl_ann(DocIdx, _N, UpNodes) ->
    ?I_INFO("Intercept is returning 2 of the preflist elements"),
    ?M:get_apl_ann_orig(DocIdx, 2, UpNodes).

%%get_apl(DocIdx, N, Service) ->
%%    ?I_INFO("Intercept is returning full preflist"),
%%    ?M:get_apl_orig(DocIdx, N, Service).

get_primary_apl(DocIdx, N, Service) ->
    ?I_INFO("Intercept is returning full preflist"),
    ?M:get_primary_apl_orig(DocIdx, N, Service).

get_apl_ann(DocIdx, N, UpNodes) ->
    ?I_INFO("Intercept is returning full preflist"),
    ?M:get_apl_ann_orig(DocIdx, N, UpNodes).