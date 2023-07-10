# LimaCharlie Adapter + Kubernetes Pods

This is a simple container running designed to run as a Daemon Set in Kubernetes
to ingest all the Pod logs as sensors into LimaCharlie.

This container is available on Docker Hub as `refractionpoint/lc-adapter-k8s-pods`.

## Usage

User needs to define the following environment variables:
* `OID` = The LimaCharlie Organization ID
* `IKEY` = The LimaCharlie Installation Key
* `NAME` = A unique name for the sensor
* `K8S_POD_LOGS` = The path to the pod logs directory (usually `/var/log/pods` within the container)

Example command line with Docker:
```bash
docker run -it -e OID=aaaaaaaa-bfa1-bbbb-cccc-138cd51389cd -e IKEY=aaaaaaaa-9ae6-bbbb-cccc-5e42b854adf5 -e NAME=k8s-pods -e K8S_POD_LOGS=/k8s-pod-logs --mount type=bind,source=/var/log/pods,target=/k8s-pod-logs,readonly refractionpoint/lc-adapter-k8s-pods
```

Example kubernetes daemon set:
```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: lc-adapter-k8s-pods
  namespace: default
spec:
  minReadySeconds: 30
  selector:
    matchLabels:
      name: lc-adapter-k8s-pods
  template:
    metadata:
      labels:
        name: lc-adapter-k8s-pods
    spec:
      containers:
      - image: refractionpoint/lc-adapter-k8s-pods
        name: lc-adapter-k8s-pods
        volumeMounts:
        - mountPath: /k8s-pod-logs
          name: pod-logs
        env:
        - name: K8S_POD_LOGS
          value: /k8s-pod-logs
        - name: OID
          value: aaaaaaaa-bfa1-bbbb-cccc-138cd51389cd
        - name: IKEY
          value: aaaaaaaa-9ae6-bbbb-cccc-5e42b854adf5
        - name: NAME
          value: k8s-pods
      volumes:
      - hostPath:
          path: /var/log/pods
        name: pod-logs
  updateStrategy:
    rollingUpdate:
      maxUnavailable: 1
    type: RollingUpdate
```