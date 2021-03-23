# Prometheus metrics receiver

This is special worker which is receiving metrics by HTTP from tested service.

You can declare this code in your bdl:
```bdl
pool(size = 1,
    worker_type = mz_prometheus_metric_receiver):
        metric_enable(["erlang", "cowboy", "mnesia", "linux_process"])
        load("http://8.8.8.8:8080/your_metrics_page")
```

## Implemented metrics

Names of metrics taken from 
1. [prometheus_client](https://github.com/deadtrickster/prometheus.erl#erlang-vm--otp-collectors)
2. [prometheus cowboy metrics](https://github.com/deadtrickster/prometheus-cowboy/blob/master/doc/prometheus_cowboy2_instrumenter.md)
3. [prometheus process colletor](https://github.com/deadtrickster/prometheus_process_collector)

All added metrics are gauge as this worker doesn't collects real data, but just pushes into mzbench already aggregated data.