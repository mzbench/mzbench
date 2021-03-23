-module(mz_prometheus_metrics).

%% API
-export([
  erlang_metrics/1,
  mnesia_metrics/1,
  cowboy_metrics/1,
  linux_process_metrics/1
]).

erlang_metrics(Host) ->
  [
    { group, "Erlang node:" ++ Host, [
      { graph, #{ title => "Memory", unit => "bytes", metrics => prepare(Host, [
         "erlang_vm_memory_system_bytes_total{usage=\"atom\"}"
         "erlang_vm_memory_system_bytes_total{usage=\"binary\"}",
         "erlang_vm_memory_system_bytes_total{usage=\"code\"}",
         "erlang_vm_memory_system_bytes_total{usage=\"ets\"}",
         "erlang_vm_memory_system_bytes_total{usage=\"other\"}"
      ])}},
      { graph, #{ title => "Atoms", unit => "count", metrics => prepare(Host, [
        "erlang_vm_atom_count",
        "erlang_vm_atom_limit"
      ])}},
      { graph, #{ title => "Memory atoms", unit => "bytes", metrics => prepare(Host, [
        "erlang_vm_memory_atom_bytes_total{usage=\"used\"}",
        "erlang_vm_memory_atom_bytes_total{usage=\"free\"}"
      ])}},
      { graph, #{ title => "Processes", unit => "count", metrics => prepare(Host, [
        "erlang_vm_process_count"
        "erlang_vm_process_limit"
      ])}},
      { graph, #{ title => "Processes memory", unit => "bytes", metrics => prepare(Host, [
        "erlang_vm_memory_bytes_total{kind=\"processes\"}",
        "erlang_vm_memory_processes_bytes_total{usage=\"used\"}",
        "erlang_vm_memory_processes_bytes_total{usage=\"free\"}"
      ])}}
    ]}
  ].
mnesia_metrics(Host) ->
  [
    { group, "Mnesia node:" ++ Host, [
      { graph, #{ title => "Memory", unit => "bytes", metrics => prepare(Host, [
        "erlang_mnesia_memory_usage_bytes"
      ])}},
      { graph, #{ title => "Transactions", unit => "count", metrics => prepare(Host, [
        "erlang_mnesia_held_locks",
        "erlang_mnesia_lock_queue",
        "erlang_mnesia_transaction_participants",
        "erlang_mnesia_transaction_coordinators",
        "erlang_mnesia_failed_transactions",
        "erlang_mnesia_committed_transactions",
        "erlang_mnesia_logged_transactions",
        "erlang_mnesia_restarted_transactions"
      ])}}
    ]}
  ].

cowboy_metrics(Host) ->
  [
    { group, "Cowboy application on node:" ++ Host, [
      { graph, #{ title => "HTTP/Websockets", unit => "count", metrics => prepare(Host, [
        "cowboy_protocol_upgrades_total",
        "cowboy_requests_total",
        "cowboy_spawned_processes_total"
      ])}},
      { graph, #{ title => "Errors", unit => "count", metrics => prepare(Host, [
        "cowboy_early_errors_total",
        "cowboy_errors_total"
      ])}}
      % cowboy_request_duration_seconds histogram
      % cowboy_receive_body_duration_seconds histogram
    ]}
  ].

linux_process_metrics(Host) ->
  [
    { group, "Process of Linux:" ++ Host, [
      { graph, #{ title => "Open file descriptors", unit => "count", metrics => prepare(Host, [
        "process_open_fds",
        "process_max_fds"
      ])}},
      { graph, #{ title => "Uptime", unit => "seconds", metrics => prepare(Host, [
        "process_uptime_seconds"
      ])}},
      { graph, #{ title => "Threads", unit => "count", metrics => prepare(Host, [
        "process_threads_total"
      ])}},
      { graph, #{ title => "Memory", unit => "bytes", metrics => prepare(Host, [
        "process_virtual_memory_bytes",
        "process_resident_memory_bytes",
        "process_max_resident_memory_bytes"
      ])}},
      { graph, #{ title => "CPU", unit => "seconds", metrics => prepare(Host, [
        "process_cpu_seconds_total{kind=\"utime\"}",
        "process_cpu_seconds_total{kind=\"stime\"}"
      ])}},
      { graph, #{ title => "Page/Swap/Context", unit => "count", metrics => prepare(Host, [
        "process_noio_pagefaults_total",
        "process_io_pagefaults_total",
        "process_swaps_total",
        "process_signals_delivered_total",
        "process_voluntary_context_switches_total",
        "process_involuntary_context_switches_total"
      ])}},
      { graph, #{ title => "Disk", unit => "count", metrics => prepare(Host, [
        "process_disk_reads_total",
        "process_disk_writes_total"
      ])}}
    ]}
  ].

%% Internal

prepare(Host, MetricList) ->
  H = Host ++ ".",
  lists:map(fun(Name) ->
    { H ++ Name, gauge}
  end, MetricList).
