# kafka-auto-scaler

A standalone Go controller that scales a target Kubernetes Deployment based on
Kafka consumer group lag. No KEDA, no third-party autoscaling framework —
just `client-go` and a poll loop.

- lag == 0 → scale target Deployment to 0 replicas
- lag > 0  → scale to `ceil(lag / LAG_PER_REPLICA)`, capped at `MAX_REPLICAS`

## Layout

- `main.go` — config, reconcile loop
- `kafka.go` — consumer group lag calculation (sarama)
- `scale.go` — reads/writes the Deployment's `scale` subresource (client-go)
- `manifests/kafka.yaml` — single-node Kafka (KRaft, no Zookeeper)
- `manifests/consumer-deployment.yaml` — sample consumer Deployment, starts at 0 replicas

## Prerequisites

- Go 1.21+
- Docker
- [kind](https://kind.sigs.k8s.io/) and `kubectl` (a `minikube` cluster works too — just adjust the commands below)

## Run it

**1. Create a local cluster and deploy Kafka + the sample consumer**

```
kind create cluster --name kafka-autoscaler
kubectl apply -f manifests/kafka.yaml
kubectl apply -f manifests/consumer-deployment.yaml
kubectl wait --for=condition=Ready pod -l app=kafka --timeout=120s
```

**2. Create the topic the consumer/controller will watch**

```
kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh \
  --create --topic orders --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

**3. Expose Kafka to your host** (separate terminal, leave running)

```
kubectl port-forward svc/kafka 9094:9094
```

**4. Run the controller** (separate terminal)

```
go run . 
```

It uses your current kubeconfig context and these defaults (override via env vars):

| Env var                | Default                | Meaning                                  |
|-------------------------|-------------------------|-------------------------------------------|
| `KUBECONFIG`            | `~/.kube/config`       | kubeconfig path                          |
| `TARGET_NAMESPACE`      | `default`              | namespace of the target Deployment       |
| `TARGET_DEPLOYMENT`     | `kafka-consumer`       | Deployment to scale                      |
| `KAFKA_BROKERS`         | `localhost:9092`       | comma-separated broker list              |
| `KAFKA_TOPIC`           | `orders`               | topic to watch                           |
| `KAFKA_CONSUMER_GROUP`  | `kafka-consumer-group` | consumer group to measure lag for        |
| `POLL_INTERVAL`         | `10s`                  | reconcile interval                       |
| `MAX_REPLICAS`          | `5`                    | replica ceiling                          |
| `LAG_PER_REPLICA`       | `100`                  | messages of lag handled per replica      |

For this setup, since Kafka is only reachable on the port-forwarded external
listener:

```
KAFKA_BROKERS=localhost:9094 go run .
```

You should see:

```
kafka-auto-scaler started: topic=orders group=kafka-consumer-group target=default/kafka-consumer poll=10s max_replicas=5 lag_per_replica=100
lag=0 current_replicas=0 desired_replicas=0
```

## Verify 0-to-N scaling

In another terminal, produce messages to build up lag:

```
for i in $(seq 1 250); do echo "order-$i"; done | \
  kubectl exec -i deploy/kafka -- /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders
```

Watch the Deployment scale up on the controller's next poll:

```
kubectl get deploy kafka-consumer -w
```

With the defaults above, 250 messages of lag yields `ceil(250/100) = 3` replicas.
The controller log will show:

```
lag=250 current_replicas=0 desired_replicas=3
scaled default/kafka-consumer from 0 to 3 replicas
```

## Verify N-to-0 scaling

The sample consumer Deployment actually consumes from the topic, so lag drains
to 0 on its own — the controller then scales it back down on its next poll:

```
lag=0 current_replicas=3 desired_replicas=0
scaled default/kafka-consumer from 3 to 0 replicas
```

## Teardown

```
kind delete cluster --name kafka-autoscaler
```
