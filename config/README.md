# Kubernetes deployment

The Kafka stack uses standard Kubernetes resources and requires no CRDs or
operator. Kafka 4.3.2 runs as three combined KRaft brokers/controllers, each with
a 10Gi persistent volume. The existing replication factors and minimum in-sync
replica settings are preserved. Clients still use
`kafka-cluster-kafka-bootstrap:9092`; Schema Registry still uses
`schema-registry:8081`.

## Prerequisites

- A default StorageClass that can provision three 10Gi ReadWriteOnce volumes,
  or a `storageClassName` set in `kafka.yaml`'s volume claim template.
- Enough capacity for three Kafka pods and one Connect pod, each requesting
  500m CPU and 1536Mi memory, plus Schema Registry and HBase.
- HBase already running, with its ZooKeeper quorum reachable as `zookeeper:2181`
  in the same namespace. `zookeeper-values.yaml` remains Helm values for that
  separate service; it is not a Kubernetes manifest and is not applied below.
- Permission to pull the custom Connect image from GHCR. Configure an
  `imagePullSecrets` entry in the Connect Deployment if the package is private.

These manifests create fresh Kafka PVCs. They do not migrate data from existing
Strimzi volumes. If an operator-managed installation already exists, plan its
data migration and resolve overlapping resource names before applying these
files. Keep its original PVCs until the migration is verified.

## Build the Connect image

The previous image used a Strimzi base. Rebuild the updated Dockerfile, which
includes the existing HBase connector and Avro converter plugins:

```sh
# Run from config/.
docker build -t ghcr.io/goldeneas/bigdata-gaming/kafka-connect:latest ./kafka-connect
docker push ghcr.io/goldeneas/bigdata-gaming/kafka-connect:latest
```

Alternatively, the existing **Publish Kafka Connect image** GitHub Actions
workflow builds the updated Dockerfile and publishes `latest` on the default
branch, along with a commit-specific tag. For a reproducible deployment, replace
the Connect Deployment's image with the newly published SHA tag or digest.
Use the rebuilt image rather than the old pinned image.

## Deploy

Run from `config/`, selecting the namespace where HBase and ZooKeeper run:

```sh
kubectl apply -n <namespace> -k .
kubectl rollout status -n <namespace> statefulset/kafka-cluster-kafka --timeout=10m
kubectl rollout status -n <namespace> deployment/schema-registry --timeout=10m
kubectl rollout status -n <namespace> deployment/kafka-connect-cluster --timeout=10m
kubectl wait -n <namespace> --for=condition=complete job/kafka2hbase-connector --timeout=15m
kubectl logs -n <namespace> job/kafka2hbase-connector
```

`kubectl kustomize .` renders the manifests without contacting the cluster.
The Kustomization includes Kafka, Schema Registry, Connect, and connector
registration. Individual YAML files can also be applied in that order.

The registration Job retries while Connect starts, then sends an idempotent
`PUT /connectors/kafka2hbase-connector/config`. It preserves the six tasks,
`taxis,flights` topics, Avro settings, and HBase options. Job completion confirms
that Connect accepted the configuration. Check the connector and task states
separately, especially if HBase is still starting:

```sh
kubectl port-forward -n <namespace> service/kafka-connect-cluster-connect-api 8083:8083
# In another terminal:
curl http://localhost:8083/connectors/kafka2hbase-connector/status
```

## Updates and operations

After changing the connector ConfigMap, recreate its completed Job to submit
the new configuration:

```sh
kubectl delete job -n <namespace> kafka2hbase-connector --ignore-not-found
kubectl apply -n <namespace> -f kafka-connect/kafka2hbase-connector.yaml
```

Worker and broker ConfigMap changes require a rollout restart of the respective
Deployment or StatefulSet. Keep the Kafka cluster ID unchanged while reusing its
PVCs. The controller quorum is fixed to node IDs 0, 1, and 2; changing the replica
count alone does not reconfigure that quorum.

There is no topic/user operator or continuous connector reconciliation. Use the
Kafka CLI for topics and ACLs, and the Connect REST API or registration Job for
connectors. The existing internal plaintext listener remains on port 9092.
Kafka itself does not use ZooKeeper; the HBase connector still does.

Configuration references: [Apache Kafka Docker image](https://kafka.apache.org/43/getting-started/docker/),
[KRaft](https://kafka.apache.org/43/operations/kraft/), and
[Connect](https://kafka.apache.org/43/kafka-connect/user-guide/).
