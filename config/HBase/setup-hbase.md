# HBase + HDFS (without operators)

## 1. App versions

Apache ZooKeeper: 3.9.1 (bitnamilegacy/zookeeper:3.9.1-debian-12-r13)
Apache Hadoop HDFS: 3.3.6 (apache/hadoop:3.3.6)
Apache HBase: 2.6.6 + Apache Phoenix 5.3.1 (oci.stackable.tech/sdp/hbase:2.6.6-stackable26.7.0, without operator)

## 2. Files

Resources set to the minimum for a demo: 0.5 CPU and ~2.9Gi RAM requests, ~4.3Gi RAM limits, ~1.5Gi PVC in total.

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

PVCs are deleted together with the StatefulSets (persistentVolumeClaimRetentionPolicy: Delete). Check:

kubectl get pvc
