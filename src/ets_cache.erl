
%%%-------------------------------------------------------------------
%%%
%%% File:      ets_cache.erl
%%%
%%% @author    Ricardo Tomé Gonçalves <tome.wave@gmail.com>
%%% @maintainer Tomasz Szarstuk <szarsti@gmail.com>
%%%
%%% @doc  
%%% ets_cache is very(!) simple in-memory cache, using ETS tables in Erlang. 
%%%  You can create a cache with a maximum number of elements in it, and when 
%%%  this limit is exceed, the oldest element is eliminated.
%%% @end  
%%%
%%%
%%% The MIT License (MIT)
%%% Copyright (C) 2013
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
%%%
%%%-------------------------------------------------------------------

-module(ets_cache).
-behaviour(gen_server).

-export([   start_link/1,
            start_link/2,
            put/3,
            put/4,
            get/2,
            get/3,
            through/3
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([cache/0]).

%% @doc Record for ets_cache. 
%% It has 2 ETS tables, a maximum size and the current size.
-record(cache, {
    max_size :: non_neg_integer(),
    table :: ets:tab(),
    itable :: ets:tab()
}).

%% @doc Row with a key, a value and last modified timestamp
-record(row, {
    key :: key(),
    ts :: timestamp(),
    value :: value()
}).

%% @doc Inverted Row, ordered by timestamp to always know the oldest key.
-record(irow, {
    ts :: timestamp() | '_',
    key :: key()
}).

%% @doc The key name where the current cache size is stored.
-define(CACHE_SIZE, cache_size).

-type ets_cache() :: #cache{}.
-type key() :: any().
-type value() :: any().
-type timestamp() :: pos_integer().
-type cache() :: pid().

%% ===================================================================
%% API Function Exports
%% ===================================================================

%% @doc Creates a new named cache with a maximum size.
-spec start_link(atom(), non_neg_integer()) -> {ok, cache()} | {error, _}.
start_link(Name, Size) ->
    gen_server:start_link({local, Name}, ?MODULE, [Size], []).

%% @doc Creates a new cache with a maximum size.
-spec start_link(non_neg_integer()) -> {ok, cache()} | {error, _}.
start_link(Size) ->
    gen_server:start_link(?MODULE, [Size], []).

%% @doc Inserts data in cache with the current timestamp.
-spec put(cache(), key(), value()) -> true.
put(Cache, Key, Value) ->
    put(Cache, Key, Value, timestamp()).

%% @doc Inserts data in cache with a given timestamp.
-spec put(cache(), key(), value(), timestamp()) -> true.
put(Cache, Key, Value, Now) ->
    gen_server:call(Cache, {put, Key, Value, Now}).

%% @doc Gets data given the key.
-spec get(ets_cache(), key()) ->  {ok, value()} | not_found | expired.
get(Cache, Key) ->
    get(Cache, Key, undefined).

%% @doc Gets data given the key, if not expired according to the timeout.
-spec get(ets_cache(), key(), (undefined | timestamp())) -> {ok, value()} | not_found | expired.
get(Cache, Key, Timeout) ->
    gen_server:call(Cache, {get, Key, Timeout}).

%% @doc Read through cache utility. Looks up cached value and runs function only if not found.
-spec through(ets_cache(), key(), fun(() -> value())) -> value().
through(Cache, Key, Fun) ->
    through(Cache, Key, Fun, undefined).

-spec through(ets_cache(), key(), fun(() -> value()), (undefined | timestamp())) -> value().
through(Cache, Key, Fun, Timeout) ->
    case get(Cache, Key, Timeout) of
        {ok, Value} -> Value;
        not_found -> 
            Value = Fun(),
            put(Cache, Key, Value),
            Value
    end.

%% ===================================================================
%% gen_server Function Exports
%% ===================================================================

-spec init(non_neg_integer()) -> {ok, ets_cache()}.
init([Max]) ->
    Table = ets:new(table, [set,private,{keypos,#row.key}]),
    ITable = ets:new(itable, [ordered_set,private,{keypos,#irow.ts}]),
    ets:insert(Table, {dummy, ?CACHE_SIZE, 0}),
    {ok, #cache{max_size=Max, table=Table, itable=ITable}}.

handle_call({put, Key, Value, Now}, _From, State) ->
    {reply, handle_put(State, Key, Value, Now), State};

handle_call({get, Key, Timeout}, _From, State) ->
    {reply, handle_get(State, Key, Timeout), State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(any(), ets_cache()) -> ok.
terminate(_reason, #cache{table=Tab, itable=ITab}) ->
    ets:delete(Tab),
    ets:delete(ITab),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Internal Function Definitions
%% ===================================================================

-spec handle_put(ets_cache(), key(), value(), timestamp()) -> true.
handle_put(#cache{max_size=MaxSize, table=Tab, itable=ITab}, Key, Value, Now) ->
    % try to insert data in primary table -> O(1)
    case ets:insert_new(Tab, #row{key = Key, ts = Now, value = Value}) of
        % key already exists
        false ->
            % insert data in primary table -> O(1)
            ets:insert(Tab, #row{key = Key, ts = Now, value = Value}),
            % delete current timestamp for the key
            ets:match_delete(Tab, #irow{ts='_', key=Key});
        % the key was new and it was inserted
        true ->
            % get the cache's current size
            CurrentSize = ets:lookup_element(Tab, ?CACHE_SIZE, 3),
            % test if max size of cache has been reached
            case CurrentSize >= MaxSize of
                % there is sufficient size
                false ->
                    ets:update_counter(Tab, ?CACHE_SIZE, 1);
                % need to prune data
                true ->
                    % get oldest timestamp
                    case ets:slot(ITab, 0) of
                        [RIT] -> %% delete timestamp
                                 ets:delete(ITab, RIT#irow.ts),
                                 %% delete key
                                 ets:delete(Tab, RIT#irow.key);
                        '$end_of_table' -> ok
                    end
            end
    end,
    % insert new timestamp for the key -> O(log(N))
    ets:insert(ITab, #irow{ts = Now, key = Key}).

-spec handle_get(ets_cache(), key(), (undefined | timestamp())) -> {ok, value()} | not_found | expired.
handle_get(#cache{table=Tab}, Key, Timeout) ->
    % lookup key
    case ets:lookup(Tab, Key) of
        % found key/value
        [Row] ->
            % test timeout
            case Timeout =/= undefined of
                true ->
                    Now = timestamp(),
                    CompareTime = Row#row.ts + Timeout * 1000,
                    % compare timeout to own timestamp
                    case CompareTime =< Now of
                        false -> {ok, Row#row.value};
                        true  -> expired
                    end;
                false -> {ok, Row#row.value}
            end;
        [] ->
            not_found
    end.


-define(DAYS_FROM_GREGORIAN_BASE_TO_EPOCH, (1970*365+478)).
-define(SECONDS_FROM_GREGORIAN_BASE_TO_EPOCH,
    (?DAYS_FROM_GREGORIAN_BASE_TO_EPOCH * 24*60*60)).
    %% == calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}})

%% @doc Returns the current timestamp.
%% Same as calendar:datetime_to_gregorian_seconds(erlang:universaltime()),
%% but significantly faster.
-spec timestamp() -> timestamp().
timestamp() ->
    {MegaSeconds, Seconds, _} = os:timestamp(),
    ?SECONDS_FROM_GREGORIAN_BASE_TO_EPOCH + MegaSeconds*1000000 + Seconds.



%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TEST_SIZE, 500).

ets_cache_test_() ->
    {setup,
        fun start/0,
        fun stop/1,
        fun(C) ->
            {inparallel,
                [ get_put(C) || _ <- lists:seq(1, 20)] ++
                [ pruning(C) || _ <- lists:seq(1, 20)] 
            }
        end
    }.

start() -> new(?TEST_SIZE).
stop(C) -> destroy(C).

get_put(C) ->
    % Put values
    P1 = put(C, key1, v1),
    P2 = put(C, key2, v2),
    P3 = put(C, key3, v3),
    P4 = put(C, key3, v33),
    %% Get values
    G1 = get(C, key1),
    G2 = get(C, key2),
    G3 = get(C, key3, 0),
    G4 = get(C, key3),
    G5 = get(C, key3, 999999),
    [
    ?_assert(P1),
    ?_assert(P2),
    ?_assert(P3),
    ?_assert(P4),
    ?_assertEqual({ok, v1} , G1),
    ?_assertEqual({ok, v2}  , G2),
    ?_assertEqual(expired   , G3),
    ?_assertEqual({ok, v33} , G4),
    ?_assertEqual({ok, v33} , G5)
    ].

pruning(C) ->
    FoundFun    =
        fun(A) ->
            case A of
                        {ok,_} -> true;
                        not_found -> false
            end
        end,
    NotFoundFun = fun(A) -> not FoundFun(A) end,
    Puts =      [ put(C, "k" ++ integer_to_list(I), <<"">>, I)
                || I <- lists:seq(1, ?TEST_SIZE*2)],
    GetsF =     [ FoundFun( get(C, "k" ++ integer_to_list(I)))
                || I <- lists:seq(?TEST_SIZE+1, ?TEST_SIZE*2)],
    GetsNF =    [ NotFoundFun( get(C, "k" ++ integer_to_list(I)))
                || I <- lists:seq(1, ?TEST_SIZE)],
    [?_assert(Op) || Op <- (Puts ++ GetsF ++ GetsNF)].

-endif.
