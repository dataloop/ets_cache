


-module(etscache).
-export([new/1, put_new/3, update/3, get/2, test/0]).

-type key() :: binary().
-type value() :: binary().


%% Opaque container for the cache, a.k.a. etscache()
-record(cache, {
	maxsize :: integer(),
%	size :: integer(),
	table :: tid(),
	itable :: tid()
}).

-record(r_table, {
	key :: key(),
	ts :: integer(),
	value :: value()
}).

-record(r_itable, {
	ts :: integer(),
	key :: key()
}).


%-type etscache() :: #cache{}.
-define(KEY_SIZE, key_size).


new(Maxsize) -> 
	Table = ets:new(table, [set,private,{keypos,2}]),
	ITable = ets:new(itable, [ordered_set,private,{keypos,2}]),
	ets:insert(Table, {dummy, ?KEY_SIZE, 0}),
	#cache{maxsize = Maxsize, table=Table, itable=ITable}.
	
	

%% insert NEW data in cache 
put_new(_Cache=#cache{maxsize=Msize, table=Tab,itable=ITab}, Key, Value) ->
	Bin = list_to_binary(Value),
	[{_,?KEY_SIZE, CurrentSize}] = ets:lookup(Tab, ?KEY_SIZE),
    io:format("Current size ~p~n", [CurrentSize]),
%%	Size = size(Bin),
	Size = 1,
	Time = timestamp(),
	%% test if max size of cache has been reached
	case CurrentSize >= Msize of
		%% there is sufficient size
		false ->
        	io:format("there is sufficient size~n", []),
			ets:update_counter(Tab, ?KEY_SIZE, Size);
			%Cache#cache{size = Tsize + Size};
		%% need to prune data
		true ->
        	io:format("there is NOT sufficient size~n", []),
			%% get oldest timestamp
			PrunedKey = ets:first(ITab),
			%% get key of the oldest timestamp
			[RIT] = ets:lookup(ITab, PrunedKey),
			%% delete timestamp
			ets:delete(ITab, PrunedKey),
			%% delete key
        	io:format("Deleting:~p~n", [RIT#r_itable.key]),
			ets:delete(Tab, RIT#r_itable.key)
	end,
	%% insert data in primary table
	case ets:insert_new(Tab, #r_table{key = Key, ts = Time, value = Bin}) of
		false -> 
			{error, "Key already exists"};
		%% insert data in inverse table
		true -> 
        	%io:format("inserting ~p~nAnd ", [#r_table{key = Key, ts = Time, value = Bin}]),
			ets:insert(ITab, #r_itable{ts = Time, key = Key}),
        	%io:format("~p~n", [#r_itable{ts = Time, key = Key}]),
			ok
	end.



%% insert (probably not new) data
update(_Cache=#cache{table=Tab, itable=ITab}, Key, Value) ->
	Bin = list_to_binary(Value),
	Time = timestamp(),
	%% insert data in primary table
	ets:insert(Tab, #r_table{key = Key, ts = Time, value = Bin}),
	%% delete timestamp for the key
	Num = ets:select_delete(ITab,[{ #r_itable{ts='_', key='$1'}, [], [{'==', '$1', Key}]}]),
	io:format("Num of deletes = ~p~n", [Num]),
	%% insert data in inverse table
	ets:insert(ITab, #r_itable{ts = Time, key = Key}).



get(#cache{table=Tab}, Key) ->
	 %% Look up the person in the named table person,
%	S = ets:info(Tab, size),
%	io:format("Size = ~p~n", [S]),
    case ets:lookup(Tab, Key) of
        [Rtable] ->
			Value = binary_to_list(Rtable#r_table.value),
            io:format("We found it, key= ~p value=~p~n", [Key,Value]),
			{ok, Value};
        [] ->
            io:format("No person with ID = ~p~n", [Key]),
			not_found
    end.

% @private
timestamp() ->
%    calendar:datetime_to_gregorian_seconds(erlang:universaltime()).
	{Mega,Sec,Micro} = erlang:now(),
	(Mega*1000000+Sec)*1000000+Micro.

%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%

% erl -noshell -s etscache test -s init stop

test() ->
	C = etscache:new(3),      %start takes 1 argument, the maximum cache size in bytes
	TabList = ets:tab2list(C#cache.table),
	io:format("~n~nLista: ~p~n", [TabList]),
	ok = etscache:put_new(C, key1, "v1"), 
	{error,_,_} = etscache:put_new(C, key1, "cenas"), 
	ok = etscache:put_new(C, key2, "v2"), 
	ok = etscache:put_new(C, key3, "v3"), 
	ok = etscache:put_new(C, key4, "v4"), 
	ok = etscache:put_new(C, key5, "v5"), 
	ok = etscache:put_new(C, key6, "v6"), 
	ok = etscache:put_new(C, key7, "v7"), 
	ok = etscache:put_new(C, key2, "v2"), 
	ok = etscache:put_new(C, key3, "v3"), 
	etscache:update(C, key7, "v77"), 
	ok = etscache:put_new(C, key9, "v9"), 
	etscache:update(C, key9, "v99"), 
	etscache:update(C, key9, "v999"), 
	etscache:update(C, key9, "v9999"), 
	etscache:update(C, key9, "v99999"), 
	etscache:update(C, key3, "v3333"), 
	ok = etscache:put_new(C, key10, "v100"), 
%	{ok, _} = etscache:get(C4, key2),
%	{ok, _} = etscache:get(C4, key3),
%	not_found = etscache:get(C4, key),
	TabList2 = ets:tab2list(C#cache.table),
	TabList3 = ets:tab2list(C#cache.itable),
	io:format("~n~nLista: ~p~n~nAnd ~p~n", [TabList2,TabList3]),
	get(C, key10),
	get(C, key9),
	get(C, key100).
	
	
	
	
	
	
	