# Flink Kafka Pipeline Connector

## 项目说明
> 扩展了flink cdc pipeline 新增了同步mongodb和sink kafka的功能，提供kafka flink table schema同步功能
> - flink-cdc-client 以命令行方式启动cdc pipeline 任务，兼容原有的pipeline配置
> - flink-cdc-pipeline-connector-kafka 新增kafka pipeline connector
> - flink-cdc-pipeline-connector-mongodb 新增mongodb pipeline connector 未完成
> - flink-cdc-pipeline-connector-kafka-schema-sync 在kafka connector 使用remote metadata applier模式时使用

## 配置说明

### flink-cdc-client
  > 1. application.properties 文件配置的参数作为默认参数
  > 2. 命令行参数以 --key value 形式传入 例如 --sink.catalog-enable true
### flink-cdc-pipeline-connector-kafka-schema-sync
  > 需要配置几个参数
  > 1. catalog.name: catalog 名称
  > 2. catalog.store.path: catalog store 存储的文件路径
  > 3. catalog.hadoop.username: hadoop用户名

  
