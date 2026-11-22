# 镜像准备

先在本审阅包目录中执行 `mkdir -p build`。从 Apache SeaTunnel 官方下载页取得 3.1.0 二进制包及签名/摘要，验证后放入 `build/`，命名为 `apache-seatunnel-3.1.0-bin.tar.gz`。本文使用独立测试集群 `st-a02-310`；先按正文显式创建并核对它，再加载镜像。

连接器来自Apache Maven发布坐标：
- org.apache.seatunnel:connector-cdc-mysql:3.1.0
- org.apache.seatunnel:connector-jdbc:3.1.0
驱动坐标：com.mysql:mysql-connector-j:8.4.0。

```bash
mkdir -p build
curl -fL https://repo.maven.apache.org/maven2/org/apache/seatunnel/connector-cdc-mysql/3.1.0/connector-cdc-mysql-3.1.0.jar -o build/connector-cdc-mysql-3.1.0.jar
curl -fL https://repo.maven.apache.org/maven2/org/apache/seatunnel/connector-jdbc/3.1.0/connector-jdbc-3.1.0.jar -o build/connector-jdbc-3.1.0.jar
curl -fL https://repo.maven.apache.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar -o build/mysql-connector-j-8.4.0.jar
docker build -t st-a02-k8s:3.1.0 build/
```

使用已有kind测试集群时：

```bash
docker pull mysql:8.0.46
kind load docker-image mysql:8.0.46 st-a02-k8s:3.1.0 --name st-a02-310
```

以上命令针对本文的独立测试集群。私有/托管K8s则将镜像推到节点可访问的注册表，并相应修改seatunnel.yaml的image字段；必要时配置imagePullSecrets。

不要认为宿主docker images中存在镜像就代表Kubernetes节点已有镜像。kind与Docker Desktop内置K8s的镜像缓存/镜像存储可能不同。

本文验证节点为containerd配置v4，kind0.30的load子命令不支持该配置版本。实际验证采用节点自带的镜像导入命令，未调整网络、镜像代理或访问权限：

```bash
docker save mysql:8.0.46 st-a02-k8s:3.1.0 | \
  docker exec -i st-a02-310-control-plane \
  ctr -n k8s.io images import --platform linux/arm64 --snapshotter overlayfs -
```

节点名、CPU架构和snapshotter必须与实际集群一致。本例为arm64/overlayfs；不要原样套到不同架构。正式部署优先使用集群可以访问的镜像仓库，或兼容节点版本的kind工具。
