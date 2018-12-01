# Data Simulator

```bash

# Add config to myConfigs.yml
cp myConfigsExample.yml myConfigs.yml
python Generator.py

```

### Kafka Testing

```bash
# this will install java 1.8, zookeeper, and kafka
brew install kafka

# this will run ZK and kafka as services
zkserver start
brew services start kafka

/usr/local/Cellar/kafka/2.0.0/bin/kafka-server-start /usr/local/etc/kafka/server.properties

/usr/local/Cellar/kafka/2.0.0/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-topic

/usr/local/Cellar/kafka/2.0.0/bin/kafka-topics --list --zookeeper localhost:2181

/usr/local/Cellar/kafka/2.0.0/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic test-topic --from-beginning

brew services stop kafka
brew services stop zookeeper

```
