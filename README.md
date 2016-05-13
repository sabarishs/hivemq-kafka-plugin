# hivemq-kafka-plugin - HiveMQ plugin for Kafka

## What this does

This plugin will directly push MQTT messages received by HiveMQ onto a Kafka cluster. Most of the relevant properties are configurable (broker url, ack mode, time outs, synchronous or asynchronous puts, linger ms etc).

## What's great about this

#### No out of order headaches
Out of order events can be a pitb. By using a combination of QoS 1/2 (MQTT ordered topic guarantees) and this plugin, you can eliminate out of order issues while ensuring good enough performance. HiveMQ supports a shared subscriber mode for performance. This means HiveMQ can treat multiple individual consumers as belonging to a single group and will make sure each message is given only once to the group but load balanced among the consumers. Though this improves performance, now there is no ordered topic guarantee anymore. This plugin is a different approach to see if we can have more performance without relaxing the ordering guarantees that MQTT provides.

I tested this on my macbook pro (Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz (Quad Core), 16 GB RAM) with HiveMQ, MQTT client, Kafka running on the same instance. Without pushing to Kafka, HiveMQ is processing 10k messages per second. With Kafka push, HiveMQ processes 10k messages in 1.250 seconds. So that is a 250 ms overhead.

#### Good defaults
By default, does synchronous puts to Kafka with full acks. This means message delivery to Kafka is ensured as soon as message is received in the HiveMQ broker. This is ofcourse, the slowest mode, but gives the best guarantee.

#### Mapping from MQTT topics to Kafka message keys
Use the properties to control which substring of the topic to use as the message keys

## Before you use

- Ensure you are using QoS 1 or 2. This plugin registers for publish receieved events to detect incoming messages
- Right now there is no special handling for dups
- The key for every record put to Kafka is derived from the MQTT topic name. This is typically the case with MQTT, that we will have different subscription topics per device/device-group that is sending the events. This way the messages are never parsed for determining the key
- By default, every put to kafka is sychronous and acked all. If loss of messages is not a deal breaker you can turn async.puts to true. You might also want to change acks mode to 0 or 1 depending on how it works for you

## How to deploy?

- modify the kafka-plugin.properties as needed
- maven clean package
- copy the jar from target to each HiveMQ broker's hivemq-x.x.x/plugins
- copy your kafka-plugin.properties to hivemq-x.x.x/plugins (the one in the jar is not used)
- modify the properties

## What more needs done

- Right now, there is no special handling for dups. This needs to be corrected.
- Failure scenarios around Kafka put

## What are the other options

- HiveMQ has the concept of a shared subscriber that can give good read performance. But ordering guarantees are absent.
