# HBase + HDFS (without operators)

## 1. App versions

Apache ZooKeeper: 3.9.1 (bitnamilegacy/zookeeper:3.9.1-debian-12-r13)
Apache Hadoop HDFS: 3.3.6 (apache/hadoop:3.3.6)
Apache HBase: 2.6.6 + Apache Phoenix 5.3.1 (oci.stackable.tech/sdp/hbase:2.6.6-stackable26.7.0, without operator)

## 2. Files

Resources sized for normal use (single RegionServer, single DataNode): ~2.3 CPU and ~12.25Gi RAM requests, 11 CPU limits, memory limit = request, ~57Gi PVC in total.
For a local microk8s run, lower HBASE_HEAPSIZE / HADOOP_HEAPSIZE_MAX, resources and PVC sizes if the machine has less memory.

| Component | Heap | CPU req / limit | Memory (req = limit) | PVC |
|---|---|---|---|---|
| ZooKeeper | 512m | 100m / 1 | 768Mi | 2Gi |
| NameNode | 1g | 250m / 1 | 1536Mi | 5Gi |
| DataNode | 1g | 250m / 2 | 1536Mi | 30Gi (all HBase data) |
| HBase Master | 1g | 500m / 2 | 1536Mi | - |
| HBase RegionServer | 4g | 1 / 4 | 6Gi | - |
| HBase REST | 512m | 200m / 1 | 1Gi | - |

HBase pods have no PVC: tables (HFiles) and WAL are stored in HDFS (hbase.rootdir), i.e. on the DataNode PVC.

zookeeper.yaml - ZooKeeper for HBase, 1 replica
hdfs.yaml - HDFS: core-site.xml and hdfs-site.xml in a ConfigMap, 1 NameNode, 1 DataNode (replication 1)
hbase.yaml - hbase-site.xml, HBase Master, RegionServer, REST
hdfs-cli.yaml - pod with the hdfs dfs client, same config as HDFS (optional, after hdfs.yaml)

## 3. How to - local (microk8s)

microk8s kubectl apply -f zookeeper.yaml
microk8s kubectl apply -f hdfs.yaml
microk8s kubectl apply -f hbase.yaml
microk8s kubectl get pods,pvc

## 4. How to - sdu cluster (kubectl)

kubectl apply -f zookeeper.yaml
kubectl apply -f hdfs.yaml
kubectl apply -f hbase.yaml
kubectl get pods,pvc

### 4.1 Startup order

The NameNode formats itself on first start. DataNode and HBase retry their connections until NameNode and ZooKeeper are up.
TODO: initContainer in HBase waiting for namenode:9000 and zookeeper:2181

### 4.2 Check

kubectl rollout status statefulset/zookeeper
kubectl rollout status statefulset/namenode
kubectl rollout status statefulset/datanode
kubectl rollout status statefulset/hbase-master
kubectl rollout status statefulset/hbase-regionserver
kubectl rollout status deployment/hbase-rest

## 5. Usage

### 5.1 HDFS

kubectl apply -f hdfs-cli.yaml
kubectl exec -it hdfs-cli -- hdfs dfs -ls /
kubectl exec -it hdfs-cli -- hdfs dfs -ls /hbase
kubectl port-forward svc/namenode 9870:9870

### 5.2 HBase

kubectl exec -it hbase-master-0 -- hbase shell

Phoenix - WEB_STAT example (creates a table, loads a CSV, runs queries):

kubectl exec hbase-master-0 -- /stackable/phoenix/bin/psql.py zookeeper:2181:/hbase /stackable/phoenix/examples/WEB_STAT.sql /stackable/phoenix/examples/WEB_STAT.csv /stackable/phoenix/examples/WEB_STAT_QUERIES.sql

REST - from inside the pod (the image has curl):

kubectl exec deploy/hbase-rest -- curl -s -H "Accept: application/json" http://localhost:8080/version/cluster
kubectl exec deploy/hbase-rest -- curl -s -H "Accept: application/json" http://localhost:8080/

REST from your own machine - port-forward in a separate terminal, then curl http://localhost:8080:

kubectl port-forward svc/hbase-rest 8080:8080


## 6. Uninstall (kubectl)

Reverse order:

kubectl delete -f hdfs-cli.yaml --ignore-not-found
kubectl delete -f hbase.yaml
kubectl delete -f hdfs.yaml
kubectl delete -f zookeeper.yaml

PVCs are kept after the StatefulSets are deleted (persistentVolumeClaimRetentionPolicy: Retain), so a re-apply reuses the data. To wipe the data, delete them by hand:

kubectl delete pvc data-zookeeper-0 data-namenode-0 data-datanode-0

Check:

kubectl get pvc
