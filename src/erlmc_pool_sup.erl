%% Copyright (c) 2010
%% Stephan Maka <stephan@spaceboyz.net>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
-module(erlmc_pool_sup).

-behaviour(supervisor).

%% API
-export([start_link/3, next_conn/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================
start_link(Amount, Host, Port) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [Amount, Host, Port]).

next_conn() ->
    {_, _, SS} = now(),
    Children = supervisor:which_children(?SERVER),
    {_, Child, _, _} = lists:nth((SS rem length(Children)) + 1, Children),
    {ok, Child}.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([Amount, Host, Port]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Children = [{{kumo, I},
		 {erlmc_conn, start_link, [[Host, Port]]},
		 permanent, 1000, worker, [erlmc_conn]}
		|| I <- lists:seq(1, Amount)],

    {ok, {SupFlags, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
