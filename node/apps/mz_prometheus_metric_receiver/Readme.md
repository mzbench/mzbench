# Prometheus metrics receiver

This is special worker which is receiving metrics by HTTP from tested service.
It receives data in [Prometheus metrics format](https://prometheus.io/docs/instrumenting/exposition_formats/).
There is no any grouping and it means each line of metric data will be one metric in MZBench.

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

## BDL commands of worker

* ```load(URL)``` -- take metrics from this URL at every 1 second. The name of receiver is host of URL.

* ```load(Name, URL)``` -- take metrics from this URL at every 1 second. Name is name of receiver in metrics area.

*  ```metric_enable(ListOfMetricsGroups)``` -- enable batch of metrics for receiver.

## List of available metric groups

* erlang -- all metrics related of tested Erlang node.
* cowboy -- all metrics related of Cowboy handlers 
* mnesia -- all metrics related of Mnesia
* linux_process -- all metrics related of Linux process of tested service. 

## How i can add new metrics ? 

You needs add them in code of your worker. Receiver can receive data, but declaration of these metrics should be in your code.

