# Introduction

This connector is used to received [NetFlow](https://en.wikipedia.org/wiki/NetFlow) data from network devices in real time.


# Configuration

## NetFlowSourceConnector

```properties
name=connector1
tasks.max=1
connector.class=com.github.jcustenborder.kafka.connect.netflow.NetFlowSourceConnector

# Set these required values
topic.data=
topic.template=
```

| Name           | Description | Type   | Default | Valid Values                     | Importance |
|----------------|-------------|--------|---------|----------------------------------|------------|
| topic.data     |             | string |         |                                  | high       |
| topic.template |             | string |         |                                  | high       |
| listen.address |             | string | 0.0.0.0 |                                  | medium     |
| listen.port    |             | int    | 12345   | ValidPort{start=1025, end=65535} | medium     |