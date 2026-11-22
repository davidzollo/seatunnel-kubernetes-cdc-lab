# Prepare the SeaTunnel 3.1.0 job image

Run these steps from the extracted walkthrough directory. Create and verify the dedicated `st-a02-310` kind cluster before loading images into it.

1. Download the full Apache SeaTunnel 3.1.0 binary distribution from the [Apache release page](https://seatunnel.apache.org/download/). Verify the published [signature](https://downloads.apache.org/seatunnel/3.1.0/apache-seatunnel-3.1.0-bin.tar.gz.asc) and [SHA-512 digest](https://downloads.apache.org/seatunnel/3.1.0/apache-seatunnel-3.1.0-bin.tar.gz.sha512). Put the verified file at `build/apache-seatunnel-3.1.0-bin.tar.gz`.
2. Download the two Apache SeaTunnel connector JARs and the MySQL JDBC driver, then build the image:

```bash
mkdir -p build
curl -fL https://repo.maven.apache.org/maven2/org/apache/seatunnel/connector-cdc-mysql/3.1.0/connector-cdc-mysql-3.1.0.jar -o build/connector-cdc-mysql-3.1.0.jar
curl -fL https://repo.maven.apache.org/maven2/org/apache/seatunnel/connector-jdbc/3.1.0/connector-jdbc-3.1.0.jar -o build/connector-jdbc-3.1.0.jar
curl -fL https://repo.maven.apache.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar -o build/mysql-connector-j-8.4.0.jar
docker build -t st-a02-k8s:3.1.0 build/
```

3. Load both images into the named kind cluster:

```bash
docker pull mysql:8.0.46
kind load docker-image mysql:8.0.46 st-a02-k8s:3.1.0 --name st-a02-310
```

The images listed by `docker images` on the host are not necessarily available to Kubernetes nodes. In a remote or managed cluster, push the job image to a registry your nodes can access, update `image` in `seatunnel.yaml`, and configure registry authentication if required.

## If the kind loader cannot import the images

The validation node used containerd configuration version 4, which the kind 0.30 image loader did not support. We imported the images through the node’s own containerd client without changing network, proxy, or access settings:

```bash
docker save mysql:8.0.46 st-a02-k8s:3.1.0 | \
  docker exec -i st-a02-310-control-plane \
  ctr -n k8s.io images import --platform linux/arm64 --snapshotter overlayfs -
```

This fallback is specific to the isolated cluster and its ARM64/overlayfs setup. Verify the node name, architecture, and snapshotter before using it. For a real deployment, prefer a registry the nodes can reach or a kind version compatible with the node.
