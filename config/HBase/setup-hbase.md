# HBase + HDFS on microk8s (Stackable Data Platform 26.7)

Based on https://docs.stackable.tech/home/stable/hbase/getting_started/
Namespaces: operators -> operators, HBase stack (ZooKeeper, HDFS, HBase) -> hbase.
Bitnami charts are not used - Bitnami now publishes only legacy images (bitnamilegacy).

## 1. App versions

Stackable operators: 26.7.0
Apache ZooKeeper: 3.9.5
Apache Hadoop HDFS: 3.5.0
Apache HBase: 2.6.6

## 2. How to - microk8s

Uses microk8s kubectl / microk8s helm.

### 2.1 Preparation (once)

microk8s status --wait-ready
microk8s enable dns hostpath-storage helm

### 2.2 Operators

microk8s uses a non-standard kubelet dir, so secret-operator and listener-operator need kubeletDir set (a /var/lib/kubelet symlink is not enough - the CSI drivers fail to register).

microk8s kubectl create namespace operators
microk8s helm install --wait -n operators zookeeper-operator oci://oci.stackable.tech/sdp-charts/zookeeper-operator --version 26.7.0 &
microk8s helm install --wait -n operators hdfs-operator oci://oci.stackable.tech/sdp-charts/hdfs-operator --version 26.7.0 &
microk8s helm install --wait -n operators commons-operator oci://oci.stackable.tech/sdp-charts/commons-operator --version 26.7.0 &
microk8s helm install --wait -n operators secret-operator oci://oci.stackable.tech/sdp-charts/secret-operator --version 26.7.0 --set csiNodeDriver.kubeletDir=/var/snap/microk8s/common/var/lib/kubelet &
microk8s helm install --wait -n operators listener-operator oci://oci.stackable.tech/sdp-charts/listener-operator --version 26.7.0 --set kubeletDir=/var/snap/microk8s/common/var/lib/kubelet &
microk8s helm install --wait -n operators hbase-operator oci://oci.stackable.tech/sdp-charts/hbase-operator --version 26.7.0 &
wait

### 2.3 Clusters (order: ZooKeeper -> znodes -> HDFS -> HBase, this will change a lot)

microk8s kubectl create namespace hbase
microk8s kubectl -n hbase apply -f zk-values.yaml
microk8s kubectl -n hbase apply -f znode-values.yaml
microk8s kubectl -n hbase apply -f hdfs-values.yaml
microk8s kubectl -n hbase apply -f hbase-values.yaml

### 2.4 Check

microk8s kubectl -n hbase wait --for=condition=Available --timeout=15m zookeepercluster/simple-zk hdfscluster/simple-hdfs hbasecluster/simple-hbase
microk8s kubectl -n hbase get pods,pvc

## 3. How to - kubectl

### 3.2 Operators

kubectl create namespace operators
helm install --wait -n operators zookeeper-operator oci://oci.stackable.tech/sdp-charts/zookeeper-operator --version 26.7.0 &
helm install --wait -n operators hdfs-operator oci://oci.stackable.tech/sdp-charts/hdfs-operator --version 26.7.0 &
helm install --wait -n operators commons-operator oci://oci.stackable.tech/sdp-charts/commons-operator --version 26.7.0 &
helm install --wait -n operators secret-operator oci://oci.stackable.tech/sdp-charts/secret-operator --version 26.7.0 --set csiNodeDriver.kubeletDir=/var/snap/microk8s/common/var/lib/kubelet &
helm install --wait -n operators listener-operator oci://oci.stackable.tech/sdp-charts/listener-operator --version 26.7.0 --set kubeletDir=/var/snap/microk8s/common/var/lib/kubelet &
helm install --wait -n operators hbase-operator oci://oci.stackable.tech/sdp-charts/hbase-operator --version 26.7.0 &
wait

### 3.3 Clusters (order: ZooKeeper -> znodes -> HDFS -> HBase)

kubectl create namespace hbase
kubectl -n hbase apply -f zk-values.yaml
kubectl -n hbase apply -f znode-values.yaml
kubectl -n hbase apply -f hdfs-values.yaml
kubectl -n hbase apply -f hbase-values.yaml

### 3.4 Check

kubectl -n hbase wait --for=condition=Available --timeout=15m zookeepercluster/simple-zk hdfscluster/simple-hdfs hbasecluster/simple-hbase
kubectl -n hbase get pods,pvc
kubectl -n hbase exec -it simple-hbase-master-default-0 -- /stackable/hbase/bin/hbase shell

The operators watch all namespaces, so each further tech stack can get its own namespace without reinstalling them.

## 4. Usage (kubectl)

Based on the Stackable getting started guides (docs 26.7), adapted to namespace hbase.

### 4.1 HBase via Apache Phoenix

Phoenix is bundled in the HBase image, psql.py is run from the REST server pod. Tutorial example - creates table WEB_STAT, loads a CSV and runs sample queries:

kubectl -n hbase exec simple-hbase-restserver-default-0 -- /stackable/phoenix/bin/psql.py /stackable/phoenix/examples/WEB_STAT.sql /stackable/phoenix/examples/WEB_STAT.csv /stackable/phoenix/examples/WEB_STAT_QUERIES.sql

Expected result at the end:

HO                    TOTAL_ACTIVE_VISITORS
EU                                      150
NA                                        1

Own SQL/CSV files - copy them into the pod first, then pass them to psql.py the same way:

kubectl cp -n hbase ./my.sql simple-hbase-restserver-default-0:/tmp/my.sql
kubectl -n hbase exec simple-hbase-restserver-default-0 -- /stackable/phoenix/bin/psql.py /tmp/my.sql

### 4.2 HBase via REST API

Cluster version, cluster status, list of tables (Phoenix tables like WEB_STAT and SYSTEM.* show up here too):

kubectl -n hbase exec simple-hbase-restserver-default-0 -- curl -s -H "Accept: application/json" "http://simple-hbase-restserver-default-headless:8080/version/cluster"
kubectl -n hbase exec simple-hbase-restserver-default-0 -- curl -s -H "Accept: application/json" "http://simple-hbase-restserver-default-headless:8080/status/cluster"
kubectl -n hbase exec simple-hbase-restserver-default-0 -- curl -s -H "Accept: application/json" "http://simple-hbase-restserver-default-headless:8080/"

Create table users with column family cf:

kubectl -n hbase exec simple-hbase-restserver-default-0 -- curl -s -XPUT -H "Accept: text/xml" -H "Content-Type: text/xml" "http://simple-hbase-restserver-default-headless:8080/users/schema" -d '<TableSchema name="users"><ColumnSchema name="cf" /></TableSchema>'

## 5. Uninstall (kubectl)

kubectl config use-context microk8s

### 5.1 Clusters (reverse order)

kubectl -n hbase delete -f hbase-values.yaml --wait
kubectl -n hbase delete -f hdfs-values.yaml --wait
kubectl -n hbase delete -f znode-values.yaml --wait
kubectl -n hbase delete -f zk-values.yaml --wait

### 5.2 Namespace

Also removes PVCs. The operators don't delete them, and leftover PVCs would reattach old data.

kubectl delete namespace hbase

### 5.3 Operators

helm uninstall -n operators hbase-operator
helm uninstall -n operators listener-operator
helm uninstall -n operators secret-operator
helm uninstall -n operators commons-operator
helm uninstall -n operators hdfs-operator
helm uninstall -n operators zookeeper-operator
kubectl delete namespace operators
