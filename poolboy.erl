%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy).
-behaviour(gen_server).

-export([checkout/1, checkout/2, checkout/3, checkin/2, transaction/2,
         transaction/3, child_spec/2, child_spec/3, start/1, start/2,
         start_link/1, start_link/2, stop/1, status/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TIMEOUT, 5000).

-record(state, {
    supervisor :: pid(),%worker的supervisor进程的pid
    %可分配worker的数组
    workers :: [pid()],
    %如果池已满并且溢出数已达到最大溢出数，则把申请worker的pid和监控这个pid的监控引用存到waiting这个队列里
    %以等待已使用worker的释放
    waiting :: queue(),
    %该ets表里保存已分配worker和使用该worer的应用进程信息：{Pid, Ref}, Pid 是worker进程pid，Ref是使用此worker的应用进程的监控引用
    monitors :: ets:tid(),
    size = 5 :: non_neg_integer(),%进程池大小
    overflow = 0 :: non_neg_integer(),%已经溢出的worker数
    max_overflow = 10 :: non_neg_integer()%最大溢出的worker数
}).

-spec checkout(Pool :: node()) -> pid().
checkout(Pool) ->
    checkout(Pool, true).

-spec checkout(Pool :: node(), Block :: boolean()) -> pid() | full.
checkout(Pool, Block) ->
    checkout(Pool, Block, ?TIMEOUT).

-spec checkout(Pool :: node(), Block :: boolean(), Timeout :: timeout())
    -> pid() | full.
checkout(Pool, Block, Timeout) ->
    try
        gen_server:call(Pool, {checkout, Block}, Timeout)
    catch
        Class:Reason ->
            gen_server:cast(Pool, {cancel_waiting, self()}),
            erlang:raise(Class, Reason, erlang:get_stacktrace())
    end.

-spec checkin(Pool :: node(), Worker :: pid()) -> ok.
checkin(Pool, Worker) when is_pid(Worker) ->
    gen_server:cast(Pool, {checkin, Worker}).

-spec transaction(Pool :: node(), Fun :: fun((Worker :: pid()) -> any()))
    -> any().
transaction(Pool, Fun) ->
    transaction(Pool, Fun, ?TIMEOUT).

-spec transaction(Pool :: node(), Fun :: fun((Worker :: pid()) -> any()), 
    Timeout :: timeout()) -> any().
transaction(Pool, Fun, Timeout) ->
    Worker = poolboy:checkout(Pool, true, Timeout),
    try
        Fun(Worker)
    after
        ok = poolboy:checkin(Pool, Worker)
    end.

-spec child_spec(Pool :: node(), PoolArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(Pool, PoolArgs) ->
    child_spec(Pool, PoolArgs, []).

-spec child_spec(Pool :: node(),
                 PoolArgs :: proplists:proplist(),
                 WorkerArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(Pool, PoolArgs, WorkerArgs) ->
    {Pool, {poolboy, start_link, [PoolArgs, WorkerArgs]},
     permanent, 5000, worker, [poolboy]}.

-spec start(PoolArgs :: proplists:proplist())
    -> gen:start_ret().
start(PoolArgs) ->
    start(PoolArgs, PoolArgs).

-spec start(PoolArgs :: proplists:proplist(),
            WorkerArgs:: proplists:proplist())
    -> gen:start_ret().
start(PoolArgs, WorkerArgs) ->
    start_pool(start, PoolArgs, WorkerArgs).

-spec start_link(PoolArgs :: proplists:proplist())
    -> gen:start_ret().
start_link(PoolArgs)  ->
    %% for backwards compatability, pass the pool args as the worker args as well
    start_link(PoolArgs, PoolArgs).

-spec start_link(PoolArgs :: proplists:proplist(),
                 WorkerArgs:: proplists:proplist())
    -> gen:start_ret().
start_link(PoolArgs, WorkerArgs)  ->
    start_pool(start_link, PoolArgs, WorkerArgs).

-spec stop(Pool :: node()) -> ok.
stop(Pool) ->
    gen_server:call(Pool, stop).

-spec status(Pool :: node()) -> {atom(), integer(), integer(), integer()}.
status(Pool) ->
    gen_server:call(Pool, status).

init({PoolArgs, WorkerArgs}) ->
    process_flag(trap_exit, true),
    Waiting = queue:new(),
    Monitors = ets:new(monitors, [private]),
    init(PoolArgs, WorkerArgs, #state{waiting = Waiting, monitors = Monitors}).

init([{worker_module, Mod} | Rest], WorkerArgs, State) when is_atom(Mod) ->
    %启动worker的sup监控，simple_one_for_one模式，Sup是sup监控进程的pid
    {ok, Sup} = poolboy_sup:start_link(Mod, WorkerArgs),
    init(Rest, WorkerArgs, State#state{supervisor = Sup});
init([{size, Size} | Rest], WorkerArgs, State) when is_integer(Size) ->
    init(Rest, WorkerArgs, State#state{size = Size});
init([{max_overflow, MaxOverflow} | Rest], WorkerArgs, State) when is_integer(MaxOverflow) ->
    init(Rest, WorkerArgs, State#state{max_overflow = MaxOverflow});
init([_ | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State);
init([], _WorkerArgs, #state{size = Size, supervisor = Sup} = State) ->
    %使用supervisor:start_child(Sup, [])产生Size个worker，worker的pid放在Workers数组里
    %并使用link()把所有worker都监控起来，如果worker退出则会收到'EXIT'信号
    Workers = prepopulate(Size, Sup),
    {ok, State#state{workers = Workers}}.

handle_cast({checkin, Pid}, State = #state{monitors = Monitors}) ->
    case ets:lookup(Monitors, Pid) of
        [{Pid, Ref}] ->
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
            NewState = handle_checkin(Pid, State),
            {noreply, NewState};
        [] ->
            {noreply, State}
    end;

handle_cast({cancel_waiting, Pid}, State) ->
    Waiting = queue:filter(fun ({{P, _}, _}) -> P =/= Pid end, State#state.waiting),
    {noreply, State#state{waiting = Waiting}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call({checkout, Block}, {FromPid, _} = From, State) ->
    #state{supervisor = Sup,
           workers = Workers,
           monitors = Monitors,
           overflow = Overflow,
           max_overflow = MaxOverflow} = State,
    case Workers of
        %池还有剩余
        [Pid | Left] ->
            %把请求worker的进程监控起来，防止使用worker的进程还没有checkin就死掉
            %erlang:monitor,如果监控的进程死掉，会收到'DOWN'消息
            Ref = erlang:monitor(process, FromPid),
            %把worker的pid和使用该worker的监控引用都存到Monitors这个ets表里
            true = ets:insert(Monitors, {Pid, Ref}),
            {reply, Pid, State#state{workers = Left}};
            %最大溢出数大于当前溢出数，则创建一个新的worker,
            %并且把worker的pid和使用该worker的监控引用也都存到Monitors这个ets表里
        [] when MaxOverflow > 0, Overflow < MaxOverflow ->
            {Pid, Ref} = new_worker(Sup, FromPid),
            true = ets:insert(Monitors, {Pid, Ref}),
            {reply, Pid, State#state{overflow = Overflow + 1}};
        [] when Block =:= false ->
            {reply, full, State};
        [] ->
            %如果池里没有剩余，并且溢出数已达到最大溢出数，则不分配新的worker
            %把申请worker的pid和监控这个pid的监控引用存到waiting这个队列里,以等待已使用worker的释放
            Ref = erlang:monitor(process, FromPid),
            Waiting = queue:in({From, Ref}, State#state.waiting),
            {noreply, State#state{waiting = Waiting}}
    end;

handle_call(status, _From, State) ->
    #state{workers = Workers,
           monitors = Monitors,
           overflow = Overflow} = State,
    StateName = state_name(State),
    {reply, {StateName, length(Workers), Overflow, ets:info(Monitors, size)}, State};
handle_call(get_avail_workers, _From, State) ->
    Workers = State#state.workers,
    {reply, Workers, State};
handle_call(get_all_workers, _From, State) ->
    Sup = State#state.supervisor,
    WorkerList = supervisor:which_children(Sup),
    {reply, WorkerList, State};
handle_call(get_all_monitors, _From, State) ->
    Monitors = ets:tab2list(State#state.monitors),
    {reply, Monitors, State};
handle_call(stop, _From, State) ->
    true = exit(State#state.supervisor, shutdown),
    {stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
    Reply = {error, invalid_message},
    {reply, Reply, State}.

%已分配worker的应用进程或在waiting队列里等待分配worker的应用进程如果死掉，则会收到'DOWN'消息
handle_info({'DOWN', Ref, _, _, _}, State) ->
    case ets:match(State#state.monitors, {'$1', Ref}) of
        %如果已使用worker，则把worker回收
        [[Pid]] ->
            true = ets:delete(State#state.monitors, Pid),
            NewState = handle_checkin(Pid, State),
            {noreply, NewState};
        %如果没有使用worker，而是在Waiting队列里排队，则从等待队列里删掉
        [] ->
            Waiting = queue:filter(fun ({_, R}) -> R =/= Ref end, State#state.waiting),
            {noreply, State#state{waiting = Waiting}}
    end;
%worker死掉的处理
handle_info({'EXIT', Pid, _Reason}, State) ->
    #state{supervisor = Sup,
           monitors = Monitors} = State,
    case ets:lookup(Monitors, Pid) of
        %如果是已被分配的worker
        [{Pid, Ref}] ->
            %删除对应用进程的监控
            true = erlang:demonitor(Ref),
            %Monitors表里删除该pid
            true = ets:delete(Monitors, Pid),
            NewState = handle_worker_exit(Pid, State),
            {noreply, NewState};
        %未分配的worker死掉，先从workers数组里删掉，再重新new一个
        [] ->
            case lists:member(Pid, State#state.workers) of
                true ->
                    W = lists:filter(fun (P) -> P =/= Pid end, State#state.workers),
                    {noreply, State#state{workers = [new_worker(Sup) | W]}};
                false ->
                    {noreply, State}
            end
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start_pool(StartFun, PoolArgs, WorkerArgs) ->
    case proplists:get_value(name, PoolArgs) of
        undefined ->
            gen_server:StartFun(?MODULE, {PoolArgs, WorkerArgs}, []);
        Name ->
            gen_server:StartFun(Name, ?MODULE, {PoolArgs, WorkerArgs}, [])
    end.

new_worker(Sup) ->
    {ok, Pid} = supervisor:start_child(Sup, []),
    true = link(Pid),
    Pid.

new_worker(Sup, FromPid) ->
    Pid = new_worker(Sup),
    Ref = erlang:monitor(process, FromPid),
    {Pid, Ref}.

dismiss_worker(Sup, Pid) ->
    true = unlink(Pid),
    supervisor:terminate_child(Sup, Pid).

prepopulate(N, _Sup) when N < 1 ->
    [];
prepopulate(N, Sup) ->
    prepopulate(N, Sup, []).

prepopulate(0, _Sup, Workers) ->
    Workers;
prepopulate(N, Sup, Workers) ->
    prepopulate(N-1, Sup, [new_worker(Sup) | Workers]).

%回收worker
handle_checkin(Pid, State) ->
    #state{supervisor = Sup,
           waiting = Waiting,
           monitors = Monitors,
           overflow = Overflow} = State,
    case queue:out(Waiting) of
        %如果waiting队列不为空，则把worker分配给请求进程
        {{value, {{FromPid, _} = From, _}}, Left} ->
            Ref = erlang:monitor(process, FromPid),
            true = ets:insert(Monitors, {Pid, Ref}),
            gen_server:reply(From, Pid),
            State#state{waiting = Left};
        %如果waiting队列为空，Overflow不为0，则销毁woker
        {empty, Empty} when Overflow > 0 ->
            ok = dismiss_worker(Sup, Pid),
            State#state{waiting = Empty, overflow = Overflow - 1};
        {empty, Empty} ->
            Workers = [Pid | State#state.workers],
            State#state{workers = Workers, waiting = Empty, overflow = 0}
    end.
%已分配worker退出的处理
handle_worker_exit(Pid, State) ->
    #state{supervisor = Sup,
           monitors = Monitors,
           overflow = Overflow} = State,
    %从waiting队列里取一个应用进程，把new的worker分配给它
    case queue:out(State#state.waiting) of
        {{value, {{FromPid, _} = From, _}}, LeftWaiting} ->
            MonitorRef = erlang:monitor(process, FromPid),
            %NewWorker = new_worker(State#state.supervisor),
            NewWorker = new_worker(Sup),
            true = ets:insert(Monitors, {NewWorker, MonitorRef}),
            %reply,回应gen_server:call,把new的worker分配给应用进程
            gen_server:reply(From, NewWorker),
            State#state{waiting = LeftWaiting};
        %如果没有wait的应用进程，并且进程溢出数大于零，则将溢出数减一
        {empty, Empty} when Overflow > 0 ->
            State#state{overflow = Overflow - 1, waiting = Empty};
        %如果没有wait的应用进程，并且没有进程溢出，则new一个worker，并添加到workers数组中,
        %并把原worker的pid在workers中过滤了一下
        {empty, Empty} ->
            Workers =
                [new_worker(Sup)
                 | lists:filter(fun (P) -> P =/= Pid end, State#state.workers)],
            State#state{workers = Workers, waiting = Empty}
    end.

state_name(State = #state{overflow = Overflow}) when Overflow < 1 ->
    #state{max_overflow = MaxOverflow, workers = Workers} = State,
    case length(Workers) == 0 of
        true when MaxOverflow < 1 -> full;
        true -> overflow;
        false -> ready
    end;
state_name(#state{overflow = MaxOverflow, max_overflow = MaxOverflow}) ->
    full;
state_name(_State) ->
    overflow.
