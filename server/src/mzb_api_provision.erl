-module(mzb_api_provision).

-export([
    provision_nodes/2,
    clean_nodes/3,
    ensure_file_content/5,
    ensure_dir/4
]).

-include_lib("mzbench_language/include/mzbl_types.hrl").
-include_lib("mzbench_utils/include/localhost.hrl").

-define(MICROSEC_IN_SEC, 1000000).

provision_nodes(Config, Logger) ->
    #{
        provision_nodes := ProvisionNodes,
        director_host := DirectorHost,
        worker_hosts := WorkerHosts,
        user_name := UserName,
        env := Env
    } = Config,

    UniqHosts = lists:usort([DirectorHost|WorkerHosts]),
    Logger(info, "Provisioning ~p nodes with config:~n~p", [length(UniqHosts), Config]),
    RootDir = mzb_api_bench:remote_path("", Config),
    ok = ensure_dir(UserName, UniqHosts, RootDir, Logger),

    _TimeDifferences = ntp_check(UserName, UniqHosts, Logger),

    NodeDeployPath = mzb_api_paths:node_deployment_path(),

    catch mzb_subprocess:remote_cmd(
        UserName, UniqHosts,
        mzb_string:format(
            "ps -ef | grep beam | grep -v grep | grep -v mzbench_api && ~ts/mzbench/bin/mzbench stop; true",
            [NodeDeployPath]),
        [], Logger),

    case ProvisionNodes of
        true ->
            ok = install_node(UniqHosts, Config, Logger),
            install_workers(UniqHosts, Config, Logger, Env);
        _ -> 
            ok
    end,
    DirectorNode = nodename(director_sname(Config), 0),
    WorkerNodes = [nodename(worker_sname(Config), N) || N <- lists:seq(1, length(WorkerHosts))],
    Nodes = [DirectorNode | WorkerNodes],
    ensure_vm_args([DirectorHost|WorkerHosts], Nodes, Config, Logger),
    _ = mzb_subprocess:remote_cmd(
        UserName,
        [DirectorHost|WorkerHosts],
        mzb_string:format("cd ~ts && ~ts/mzbench/bin/mzbench start", [RootDir, NodeDeployPath]),
        [],
        Logger),
    NodePids = mzb_subprocess:remote_cmd(
        UserName,
        [DirectorHost|WorkerHosts],
        mzb_string:format("cd ~ts && ~ts/mzbench/bin/mzbench getpid", [RootDir, NodeDeployPath]),
        [],
        Logger),
    InterconnectPorts = mzb_lists:pmap(fun(H) ->
        [Res] = release_rpcterms(UserName, H, RootDir, "mzb_interconnect_sup", "get_port", Logger),
        erlang:list_to_integer(Res) end, WorkerHosts),

    {lists:zip(Nodes, [DirectorHost|WorkerHosts]), InterconnectPorts, NodePids, get_management_port(Config, Logger)}.

get_management_port(Config = #{director_host:= DirectorHost, user_name:= UserName}, Logger) ->
    [Res] = release_rpcterms(UserName, DirectorHost, mzb_api_bench:remote_path("", Config),
        "mzb_management_tcp_protocol", "get_port", Logger),
    Logger(info, "Management port: ~ts", [Res]),
    erlang:list_to_integer(Res).

release_rpcterms(UserName, Host, ConfigPath, Module, Function, Logger) ->
    mzb_subprocess:remote_cmd(
        UserName,
        [Host],
        mzb_string:format("cd ~ts && ~ts/mzbench/bin/mzbench",
            [ConfigPath, mzb_api_paths:node_deployment_path()]),
        ["rpcterms", Module, Function],
        Logger, []).

% couldn't satisfy both erl 18 and 19 dialyzers, spec commented
%-spec clean_nodes(#{director_host:=_, purpose:=atom() | binary() | [atom() | [any()] | char()], user_name:=_, worker_hosts:=_, _=>_}, fun((_,_,_) -> any())) -> ok.
clean_nodes(NodePids, Config, Logger) ->
    #{
        user_name:= UserName,
        director_host:= DirectorHost,
        worker_hosts:= WorkerHosts} = Config,
    RootDir = mzb_api_bench:remote_path("", Config),
    Codes = mzb_subprocess:remote_cmd(
        UserName,
        [DirectorHost|WorkerHosts],
        mzb_string:format("cd ~ts; timeout 30s ~ts/mzbench/bin/mzbench stop > /dev/null 2>&1; echo $?",
            [RootDir, mzb_api_paths:node_deployment_path()]),
        [],
        Logger),
    _ = kill_nodes(NodePids, [DirectorHost|WorkerHosts], Codes, UserName, Logger),
    % TODO: Uncomment to clean
    % length(RootDir) > 1 andalso mzb_subprocess:remote_cmd(UserName, [DirectorHost|WorkerHosts], mzb_string:format("rm -rf ~ts", [RootDir]), [], Logger),
    ok.

kill_nodes([], _, _, _, _) -> ok;
kill_nodes(_, [], _, _, _) -> ok;
kill_nodes(_, _, [], _, _) -> ok;
kill_nodes([Pid | NodePids], [H|Hosts], [Code|StopResults], UserName, Logger) ->
    IsStoppedAlready =
        try erlang:list_to_integer(Code) of
            0 -> true;
            _ -> false
        catch
            _:Error ->
                lager:error("Bad node stop code: ~p~nReason: ~p", [Code, Error]),
                false
        end,

    case IsStoppedAlready of
        false -> mzb_subprocess:remote_cmd(UserName, [H],
                    mzb_string:format("kill -9 ~p; true", [Pid]), [], Logger);
        true -> ok
    end,
    kill_nodes(NodePids, Hosts, StopResults, UserName, Logger).

ntp_check(_, [H], Logger) ->
    Logger(info, "There's only one host, no need to make ntp check", []),
    [{H, 0}];
ntp_check(UserName, Hosts, Logger) ->
    case application:get_env(mzbench_api, ntp_max_timediff_s, undefined) of
        undefined -> [{H, undefined} || H <- Hosts];
        MaxTimeDiff ->
            NTPRes = mzb_subprocess:remote_cmd(UserName, Hosts, "ntpdate -q pool.ntp.org", [], Logger),
            Offsets = lists:map(
                    fun(X) ->
                        [_, T | _] = lists:reverse(string:tokens(X, " \n")),
                        {F, []} = string:to_float(T),
                        erlang:round(?MICROSEC_IN_SEC*F)
                    end, NTPRes),
            TimeDiff = lists:max(Offsets) - lists:min(Offsets),
            Logger(info, "NTP time diffs are: ~p, max distance is ~p microsecond", [Offsets, TimeDiff]),
            case ?MICROSEC_IN_SEC * MaxTimeDiff >= TimeDiff of
                true -> ok;
                false ->
                    Logger(error, "NTP CHECK FAILED, max time different is ~p microseconds", [TimeDiff]),
                    erlang:error({ntp_check_failed, TimeDiff})
            end,
            lists:zip(Hosts, Offsets)
    end.

nodename(Name, N) ->
    erlang:list_to_atom(mzb_string:format("~ts_~b@127.0.0.1", [Name, N])).

ensure_vm_args(Hosts, Nodenames, Config, Logger) ->
    _ = mzb_lists:pmap(
        fun ({H, N}) ->
            ensure_file_content([H], vm_args_content(N, Config), "vm.args", Config, Logger)
        end, lists:zip(Hosts, Nodenames)),
    ok.

ensure_file_content(Hosts, Content, Filepath,
                    #{user_name:= UserName} = Config, Logger) ->
    Localfile = mzb_file:tmp_filename(),
    Remotefile =
        case Filepath of
            "~/" ++ _ -> Filepath;
            _ -> mzb_api_bench:remote_path(Filepath, Config)
        end,
    Logger(debug, "Ensure file content on hosts: ~p~nLocal filename: ~p~nContent: ~ts~nRemote path: ~p", [Hosts, Localfile, Content, Remotefile]),
    ok = file:write_file(Localfile, Content),
    ok = ensure_file(UserName, Hosts, Localfile, Remotefile, Logger),
    ok = file:delete(Localfile).

ensure_file(_UserName, [Host], LocalPath, RemotePath, Logger) when ?IS_LOCALHOST(Host) ->
    Logger(info, "[ COPY ] ~ts -> ~ts", [LocalPath, RemotePath]),
    {ok, _} = file:copy(LocalPath, RemotePath),
    ok;
ensure_file(UserName, Hosts, LocalPath, RemotePath, Logger) ->
    UserNameParam =
        case UserName of
            undefined -> "";
            _ -> mzb_string:format("~ts@", [UserName])
        end,
    Logger(info, "[ SCP ] ~ts -> ~ts<HOST>:~ts~n  for ~p", [LocalPath, UserNameParam, RemotePath, Hosts]),
    _ = mzb_lists:pmap(
        fun (Host) ->
            mzb_subprocess:exec_format("scp -o StrictHostKeyChecking=no ~ts ~ts~ts:~ts",
                [LocalPath, UserNameParam, Host, RemotePath], [], mzb_api_app:default_logger())
        end, Hosts),
    ok.

-spec ensure_dir(undefined | string(), [string()], string(), fun((atom(), string(), [term()]) -> ok)) -> ok.
ensure_dir(_User, [Local], Dir, Logger) when Local == "localhost"; Local == "127.0.0.1" ->
    Logger(info, "[ MKDIR ] ~ts", [Dir]),
    % The trailing slash is needed, otherwise it will only
    % create Dir's parent, but not Dir itself
    ok = filelib:ensure_dir(Dir ++ "/");
ensure_dir(User, Hosts, Dir, Logger) ->
    _ = mzb_subprocess:remote_cmd(User, Hosts, "mkdir", ["-p", Dir], Logger, []),
    ok.

director_sname(#{id:= Id}) -> "mzb_director" ++ integer_to_list(Id).
worker_sname(#{id:= Id})   -> "mzb_worker" ++ integer_to_list(Id).

vm_args_content(NodeName, #{node_log_port:= LogPort, node_management_port:= Port,
    vm_args:= ConfigArgs, node_log_user_port:= LogUserPort,
    node_interconnect_port:= InterconnectPort} = Config) ->
    UpdateIntervalMs = mzb_bc:maps_get(metric_update_interval_ms, Config, undefined),
    LoadWorkersSubdirs = case application:get_env(mzbench_api, load_workers_subdirs, false) of
        false -> false;
        _ -> true
    end,
    NewArgs =
        [mzb_string:format("-name ~ts", [NodeName]),
         mzb_string:format("-mzbench node_management_port ~b", [Port]),
         mzb_string:format("-mzbench node_log_port ~b", [LogPort]),
         mzb_string:format("-mzbench load_workers_subdirs ~ts", [LoadWorkersSubdirs]),
         mzb_string:format("-mzbench node_log_user_port ~b", [LogUserPort]),
         mzb_string:format("-mzbench node_interconnect_port ~b", [InterconnectPort])] ++
        [mzb_string:format("-mzbench metric_update_interval_ms ~b", [UpdateIntervalMs]) || UpdateIntervalMs /= undefined],

    mzb_string:format(string:join([A ++ "~n" || A <- NewArgs ++ ConfigArgs], ""), []).

get_host_os_id(UserName, Hosts, Logger) ->
    OSIds = mzb_subprocess:remote_cmd(UserName, Hosts, "uname -sr", [], Logger, []),
    [string:to_lower(mzb_string:char_substitute(lists:flatten(ID), $ , $-)) || ID <- OSIds].

get_host_erts_version(UserName, Hosts, Logger) ->
    Versions = mzb_subprocess:remote_cmd(UserName, Hosts,
        "erl -noshell -eval 'io:fwrite(\\\"~ts\\\", [erlang:system_info(version)]).' -s erlang halt",
        [], Logger, []),
    [lists:flatten(V) || V <- Versions].

get_host_system_id(UserName, Hosts, Logger) ->
    OSIds = get_host_os_id(UserName, Hosts, Logger),
    ERTSVersions = get_host_erts_version(UserName, Hosts, Logger),
    [{H, mzb_string:format("~ts_erts-~ts", [Id, ERTS])} || {H, Id, ERTS} <- lists:zip3(Hosts, OSIds, ERTSVersions)].

download_file(User, Host, FromFile, ToFile, Logger) ->
    _ = case Host of
        Local when Local == "localhost"; Local == "127.0.0.1" ->
            Logger(info, "[ COPY ] ~ts <- ~ts", [ToFile, FromFile]),
            {ok, _} = file:copy(FromFile, ToFile);
        _ ->
            UserNameParam =
                case User of
                    undefined -> "";
                    _ -> mzb_string:format("~ts@", [User])
                end,
            % we can't copy to target file directly because there might be several processes doing it
            % instead we copy to tmp file and after that we atomically rename it to target file
            % also we can't create tmp file in /tmp because it is often being
            % mounted to a different fs than target file and it makes file:rename to fail
            TmpFile = mzb_file:tmp_filename(filename:dirname(ToFile)),
            _ = mzb_subprocess:exec_format("scp -o StrictHostKeyChecking=no ~ts~ts:~ts ~ts",
                [UserNameParam, [Host], FromFile, TmpFile], [], Logger),
            Logger(info, "[ MV ] ~ts -> ~ts", [TmpFile, ToFile]),
            case file:rename(TmpFile, ToFile) of
                ok -> ok;
                { error, Reason } ->
                    if Reason =:= exdev ->
                        Logger(info, "[ MV ] Start copy ~ts -> ~ts", [TmpFile, ToFile]),
                        { ok , _ } = file:copy(TmpFile, ToFile),
                        Logger(info, "[ MV ] Delete ~ts ", [ TmpFile ]),
                        file:delete(TmpFile);
                       true ->
                        Logger(error, "Can't rename file ~ts to file ~ts with reason ~p", [ TmpFile, ToFile, Reason ]),
                        error({ cant_rename, Reason })
                    end
            end
    end,
    ok.

package_version(InstallSpec, Logger) ->
    case InstallSpec of
        #git_install_spec{repo = Repo, branch = Branch} ->
            mzb_git:get_git_short_sha1(Repo, Branch, Logger);
        #rsync_install_spec{} ->
            {A, B, C} = os:timestamp(),
            mzb_string:format("~p.~p.~p", [A, B, C])
    end.

-spec install_package([string()], string(), install_spec(), string(), term(), fun()) -> ok.
install_package(Hosts, PackageName, InstallSpec, InstallationDir, Config, Logger) ->
    Version = case application:get_env(mzbench_api, auto_update_deployed_code, enable) of
                enable -> package_version(InstallSpec, Logger);
                _ -> "someversion"
    end,
    #{user_name:= User} = Config,
    PackagesDirs = mzb_api_paths:tgz_packages_dirs(),
    HostsAndOSs = case application:get_env(mzbench_api, custom_os_code_builds, enable) of
                        enable -> case InstallSpec of
                                    #git_install_spec{build = "local"} -> [{H, "noarch"} || H <- Hosts];
                                    _ -> get_host_system_id(User, Hosts, Logger)
                                end;
                        _ -> [{H, "someos"} || H <- Hosts]
    end,
    ok = filelib:ensure_dir(hd(PackagesDirs) ++ "/"),
    UniqueOSs = lists:usort([OS || {_Host, OS} <- HostsAndOSs]),
    NeededTarballs =
        [{OS, find_package(PackagesDirs, mzb_string:format("~ts-~ts-~ts.tgz", [PackageName, Version, OS]))}
        || OS <- UniqueOSs],
    MissingTarballs = [{OS, T} || {OS, T} <- NeededTarballs, not filelib:is_file(T)],
    Logger(info, "Missing tarballs: ~p", [MissingTarballs]),
    OSsWithMissingTarballs = case [OS || {OS, _} <- MissingTarballs] of
        ["noarch"] -> Logger(info, "Building package ~ts on api server", [PackageName]),
                      [TarballPath] = [T || {_, T} <- MissingTarballs],
                      build_package_on_host("127.0.0.1", User, TarballPath, InstallSpec, Logger),
                      [];
                 L -> L
        end,
    _ = mzb_lists:pmap(fun({Host, OS}) ->
            {OS, LocalTarballPath} = lists:keyfind(OS, 1, NeededTarballs),
            RemoteTarballPath = mzb_file:tmp_filename() ++ ".tgz",
            ExtractDir = mzb_file:tmp_filename(),
            try
                case lists:member(OS, OSsWithMissingTarballs) of
                    true ->
                        build_package_on_host(Host, User, RemoteTarballPath, InstallSpec, Logger),
                        case lists:keyfind(OS, 2, HostsAndOSs) of
                            {Host, OS} ->
                                Logger(info, "Downloading package ~ts from ~ts", [PackageName, Host]),
                                download_file(User, Host, RemoteTarballPath, LocalTarballPath, Logger);
                            _ ->
                                ok
                        end;
                    false ->
                        ensure_file(User, [Host], LocalTarballPath, RemoteTarballPath, Logger)
                end,
                % Extract tgz to tmp directory and then rsync it to the installation directory in order to prevent
                % different nodes provisioning to affect each other (if we ran several nodes on one host)
                InstallationCmd = mzb_string:format("mkdir -p ~ts && cd ~ts && tar xzf ~ts && mkdir -p ~ts && rsync -aW ~ts/ ~ts",
                    [ExtractDir, ExtractDir, RemoteTarballPath, InstallationDir, ExtractDir, InstallationDir]),
                _ = mzb_subprocess:remote_cmd(User, [Host], InstallationCmd, [], Logger)
            after
                % TODO: Uncomment to clean
                % RemoveCmd = mzb_string:format("rm -rf ~ts; rm -rf ~ts; true", [RemoteTarballPath, ExtractDir]),
                % _ = mzb_subprocess:remote_cmd(User, [Host], RemoveCmd, [], Logger)
                ok
            end
        end,
        HostsAndOSs),
    ok.

find_package(PackagesDirs, PackageName) ->
    lists:foldl(
        fun(Dir, LastKnown) ->
            Current = filename:join(Dir, PackageName),
            case filelib:is_file(Current) of
                true -> Current;
                false -> LastKnown
            end
        end, filename:join(hd(PackagesDirs), PackageName), PackagesDirs).

build_package_on_host(Host, User, RemoteTarballPath, InstallSpec, Logger) ->
    DeploymentDirectory = mzb_file:tmp_filename(),
    case InstallSpec of
        #git_install_spec{repo = GitRepo, branch = GitBranch, dir = GitSubDir} ->
            Cmd = mzb_string:format("git clone ~ts deployment_code && cd deployment_code && git checkout ~ts && cd ./~ts", [GitRepo, GitBranch, GitSubDir]),
            GenerationCmd = mzb_string:format("mkdir -p ~ts && cd ~ts && ~ts "
                                          "&& DIAGNOSTIC=1 make generate_tgz && mv *.tgz ~ts",
                                          [DeploymentDirectory, DeploymentDirectory,
                                           Cmd, RemoteTarballPath]),
            _ = mzb_subprocess:remote_cmd(User, [Host], GenerationCmd, [], Logger);
        #rsync_install_spec{remote = Remote, excludes = Excludes, dir = SubDir} ->
            RemoteAbs = filename:absname(Remote),
            TargetFolder = DeploymentDirectory ++ "/deployment_code",
            RSyncExcludes =  mzb_binary:join([[<<"--exclude=">>, E] || E <- Excludes], <<" ">>),
            MainFolder = TargetFolder ++ "/" ++ SubDir,
            case ?IS_LOCALHOST(Host) of
                true ->
                    mzb_subprocess:exec_format([
                        {"cd ~ts && ./scripts/prepare_sources.sh", [ RemoteAbs ]},
                        {"mkdir -p ~ts", [ TargetFolder ]},
                        {"rsync -aWl -v ~ts ~ts/prepared_sources/ ~ts/node/", [ RSyncExcludes, RemoteAbs, TargetFolder ] }
                    ], Logger),
                    %%mzb_subprocess:exec_format([
                    Cmd = mzb_string:format([
                        {"cd ~ts && DIAGNOSTIC=1 make generate_tgz && mv -v *.tgz ~ts", [MainFolder, RemoteTarballPath] }
                    %%], [],Logger),
                    ]),
                    Logger(info, "[ EXEC ] ~ts", [ Cmd ]),
                    Output = os:cmd(Cmd),
                    Logger(info, "~ts", [ Output ]),
                    io:format("-------------------------------------------------~n~ts", [ Output ]);
                false ->
                    RemHost = mzb_binary:merge([User, <<"@">>, Host, <<":">>, TargetFolder]),
                    mzb_subprocess:exec_format([
                        {"cd ~ts && ./scripts/prepare_sources.sh", [ RemoteAbs ]},
                        {"rsync -aWl --rsync-path='mkdir -p ~ts && rsync' ~ts ~ts/prepared_sources/ ~ts/node/", [ TargetFolder, RSyncExcludes, Remote, RemHost ] },
                        {"cd ~ts && DIAGNOSTIC=1 make generate_tgz && mv -v *.tgz ~ts", [MainFolder, SubDir, RemoteTarballPath] }
                    ], Logger)
            end
    end,
    ok.

install_node(Hosts, #{node_install_spec:= InstallSpec} = Config, Logger) ->
    install_package(
        Hosts,
        "node",
        InstallSpec,
        application:get_env(mzbench_api, node_deployment_path, ""),
        Config,
        Logger).

-spec get_worker_name(install_spec()) -> string().
get_worker_name(#git_install_spec{repo = GitRepo, dir = GitSubDir}) ->
    Base = filename:basename(GitSubDir),
    case re:replace(Base, "\\W", "", [global, {return, list}]) of
        [] -> filename:basename(GitRepo, ".git");
        BaseSanitized -> BaseSanitized
    end;
get_worker_name(#rsync_install_spec{remote = Remote, dir = ""}) -> filename:basename(Remote);
get_worker_name(#rsync_install_spec{dir = SubDir}) -> filename:basename(SubDir).

install_worker(Hosts, InstallSpec, Config, Logger) ->
    WorkerName = get_worker_name(InstallSpec),
    install_package(Hosts, WorkerName, InstallSpec, application:get_env(mzbench_api, worker_deployment_path, ""), Config, Logger).

install_workers(Hosts, #{script:= Script} = Config, Logger, Env) ->
    #{ body := Body } = Script,
    AST = mzbl_script:read_from_string(binary_to_list(Body)),
    NormEnv = mzbl_script:normalize_env(Env),
    _ = [install_worker(Hosts, IS, Config, Logger) || IS <- mzbl_script:extract_install_specs(AST, NormEnv)],
    ok.
