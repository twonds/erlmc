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
%%
%% http://code.google.com/p/memcached/wiki/MemcacheBinaryProtocol
%% @doc a binary protocol memcached client
-module(erlmc).

%% api callbacks
-export([get/1, add/2, add/3, set/2, set/3, set/4,
		 replace/2, replace/3, delete/1, increment/4, decrement/4,
		 append/2, prepend/2]).

-include("erlmc.hrl").

-define(TIMEOUT, 60000).

%%--------------------------------------------------------------------
%%% API
%%--------------------------------------------------------------------
get(Key) ->
    call_pool(#request{op_code=?OP_GetK, key=Key}).

add(Key, Value) ->
    add(Key, Value, 0).
	
add(Key, Value, Expiration)
  when is_binary(Value), is_integer(Expiration) ->
    call_pool(#request{op_code = ?OP_Add,
		       extras = <<0:32, Expiration:32>>,
		       key = Key,
		       value = Value}).

set(Key, Value) ->
    set(Key, Value, 0, 0).

set(Key, Value, Expiration) ->
    set(Key, Value, Expiration, 0).

set(Key, Value, Expiration, CAS)
  when is_binary(Value), is_integer(Expiration) ->
    call_pool(#request{op_code = ?OP_Set,
		       extras = <<0:32, Expiration:32>>,
		       cas = CAS,
		       key = Key,
		       value = Value}).

replace(Key, Value) ->
    replace(Key, Value, 0).
	
replace(Key, Value, Expiration)
  when is_binary(Value), is_integer(Expiration) ->
    call_pool(#request{op_code = ?OP_Replace,
		       extras = <<0:32, Expiration:32>>,
		       key = Key,
		       value = Value}).
    
delete(Key) ->
    call_pool(#request{op_code = ?OP_Delete,
		       key = Key}).

increment(Key, Value, Initial, Expiration)
  when is_binary(Value), is_binary(Initial), is_integer(Expiration) ->
    call_pool(#request{op_code = ?OP_Increment,
		       extras = <<Value:64, Initial:64, Expiration:32>>,
		       key = Key}).

decrement(Key, Value, Initial, Expiration)
  when is_binary(Value), is_binary(Initial), is_integer(Expiration) ->
    call_pool(#request{op_code=?OP_Decrement,
		       extras = <<Value:64, Initial:64, Expiration:32>>,
		       key = Key}).

append(Key, Value) when is_binary(Value) ->
    call_pool(#request{op_code = ?OP_Append,
		       key = Key,
		       value = Value}).

prepend(Key, Value) when is_binary(Value) ->
    call_pool(#request{op_code = ?OP_Prepend,
		       key = Key,
		       value = Value}).

call_pool(Req) ->
    {ok, Conn} = erlmc_pool_sup:next_conn(),
    case gen_server:call(Conn, {req, Req}) of
	{reply, #response{status = ?STATUS_OK, value = Value, cas = CAS}} ->
	    {ok, Value, CAS};
	{reply, #response{status = Status}} ->
	    {error, status_id(Status)};
	{error, Reason} ->
	    {error, Reason}
    end.

status_id(0) -> ok;
status_id(1) -> not_found;
status_id(2) -> exists;
status_id(3) -> too_large;
status_id(4) -> invalid_arg;
status_id(5) -> not_stored;
status_id(6) -> non_numeric;
status_id(_) -> unknown.
