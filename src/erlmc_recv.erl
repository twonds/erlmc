%% Copyright (c) 2009 
%% Jacob Vorreuter <jacob.vorreuter@gmail.com>
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
-module(erlmc_recv).

-export([start_link/2]).
-include("erlmc.hrl").

start_link(Socket, ReplyCallback) ->
    Pid = spawn_link(
	    fun() ->
		    loop(Socket, ReplyCallback)
	    end),
    {ok, Pid}.

loop(Socket, ReplyCallback) ->
    Reply = recv(Socket),
    ReplyCallback(Reply),
    ?MODULE:loop(Socket, ReplyCallback).

recv(Socket) ->
    case recv_header(Socket) of
	{error, Err} ->
	    {error, Err};
	HdrResp ->
	    recv_body(Socket, HdrResp)
    end.

recv_header(Socket) ->
    decode_response_header(recv_bytes(Socket, 24)).
  
recv_body(Socket, #response{key_size = KeySize, extras_size = ExtrasSize, body_size = BodySize}=Resp) ->
    decode_response_body(recv_bytes(Socket, BodySize), ExtrasSize, KeySize, Resp).
    
decode_response_header({error, Err}) -> {error, Err};
decode_response_header(<<16#81:8, Opcode:8, KeySize:16, ExtrasSize:8, DataType:8, Status:16, BodySize:32, Opaque:32, CAS:64>>) ->
    #response{
        op_code = Opcode, 
        data_type = DataType, 
        status = Status, 
        opaque = Opaque, 
        cas = CAS, 
        key_size = KeySize,
        extras_size = ExtrasSize,
        body_size = BodySize
    }.
    
decode_response_body({error, Err}, _, _, _) -> {error, Err};
decode_response_body(Bin, ExtrasSize, KeySize, Resp) ->
    <<Extras:ExtrasSize/binary, Key:KeySize/binary, Value/binary>> = Bin,
    Resp#response{
        extras = Extras,
        key = Key,
        value = Value
    }.

recv_bytes(_, 0) -> <<>>;
recv_bytes(Socket, NumBytes) ->
    case gen_tcp:recv(Socket, NumBytes) of
        {ok, Bin} -> Bin;
        Err -> Err
    end.
