/*
 * Copyright 2016 Sabarish Sasidharan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sabscape.mqtt.plugin;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hivemq.spi.PluginEntryPoint;
import com.hivemq.spi.callback.events.OnPublishReceivedCallback;
import com.hivemq.spi.callback.exception.OnPublishReceivedException;
import com.hivemq.spi.callback.registry.CallbackRegistry;
import com.hivemq.spi.config.SystemInformation;
import com.hivemq.spi.message.PUBLISH;
import com.hivemq.spi.security.ClientData;

/**
 * This is the main class of the plugin, which is instantiated during the HiveMQ start up process.
 *
 * @author Sabarish Sasidharan
 */
public class KafkaPluginMain extends PluginEntryPoint {

    private static final Logger log = LoggerFactory.getLogger(KafkaPluginMain.class);

    private final Properties pluginProperties = new Properties();


    @Inject
    public KafkaPluginMain(SystemInformation systemInfo) throws IOException{
    	File propsFile = new File(systemInfo.getPluginFolder(), "kafka-plugin.properties");
        InputStream is = new BufferedInputStream(new FileInputStream(propsFile));
        pluginProperties.load(is);
        is.close();
    }

    /**
     * This method is executed after the instanciation of the whole class. It is used to initialize
     * the implemented callbacks and make them known to the HiveMQ core.
     */
    @PostConstruct
    public void postConstruct() {

        CallbackRegistry callbackRegistry = getCallbackRegistry();
        callbackRegistry.addCallback(new OnPublishReceivedCallback() {
			private Properties props = new Properties();
			private KafkaProducer<String, byte[]> producer = null;
			private AtomicInteger puts = new AtomicInteger(0);
			private boolean asyncPuts = Boolean.valueOf(pluginProperties.getProperty("async.puts", "false"));
			private boolean noPuts = Boolean.valueOf(pluginProperties.getProperty("no.puts", "false"));
			private String sinkTopic = pluginProperties.getProperty("sink.topic");
			private String clientId = pluginProperties.getProperty("client.id");
			private String kafkaBroker = pluginProperties.getProperty("broker.url");
			private String ackMode = pluginProperties.getProperty("acks");
			private int lingerMs = Integer.valueOf(pluginProperties.getProperty("linger.ms", "0"));
			private int metadataFetchTimeoutMs = Integer.valueOf(pluginProperties.getProperty("metadata.fetch.timeout.ms", "500"));
			private int numPutRetries = Integer.valueOf(pluginProperties.getProperty("num.put.retries", "3"));
			private String keySerializerClassName = pluginProperties.getProperty("key.serializer", StringSerializer.class.getName());
			private String valueSerializerClassName = pluginProperties.getProperty("value.serializer", ByteArraySerializer.class.getName());
			private String kafkaPartitionerClassName = pluginProperties.getProperty("partitioner", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
			private boolean replaceClassloader = Boolean.valueOf(pluginProperties.getProperty("replace.classloader", "true"));
			private int logFrequency = Integer.valueOf(pluginProperties.getProperty("log.frequency", "10000"));
			private int[] kafkaRecordKeyRange = new int[] {
					Integer.valueOf(pluginProperties.getProperty("kafka.record.key.start.pos", "4")),
					Integer.valueOf(pluginProperties.getProperty("kafka.record.key.end.pos", "13"))
			};
			private long start = System.currentTimeMillis();
			{
				log.info("Instance created");
				props.put("client.id", clientId);
				props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
				props.put("acks", ackMode);
				props.put("linger.ms", lingerMs);
				props.put("metadata.fetch.timeout.ms", metadataFetchTimeoutMs);
				props.put("retries", numPutRetries);
				props.put("key.serializer", keySerializerClassName);
				props.put("value.serializer", valueSerializerClassName);
				try {
					Class.forName(kafkaPartitionerClassName);
				}
				catch (Exception e) {
					log.error("Failed when loading partitioner", e);
					throw new RuntimeException(e);
				}
				ClassLoader ccl = Thread.currentThread().getContextClassLoader();
				if (replaceClassloader) {
					Thread.currentThread().setContextClassLoader(null);
				}
				producer = new KafkaProducer<String, byte[]>(props);
				if (replaceClassloader) {
					Thread.currentThread().setContextClassLoader(ccl);
				}
			}
			
        	@Override
			public int priority() {
				return 0;
			}
			
			@Override
			public void onPublishReceived(PUBLISH message, ClientData cd) throws OnPublishReceivedException {
				if (!noPuts) {
					Future<RecordMetadata> f = producer.send(new ProducerRecord<String, byte[]>(sinkTopic, message.getTopic().substring(kafkaRecordKeyRange[0], kafkaRecordKeyRange[1]), message.getPayload()));
					if (!asyncPuts) {
						try {
							RecordMetadata rm = f.get();
						}
						catch (ExecutionException | InterruptedException ee) {
							//check offset before putting again?
							log.error("Failed when putting to kafka", ee);
						}
					}
				}
				if (logFrequency != -1) {
					int np = puts.incrementAndGet();
					if (np % logFrequency == 0) {
						log.info("Put {} in {} ms", puts, System.currentTimeMillis() - start);
						start = System.currentTimeMillis();
						puts.set(0);
					}
				}
			}
		});
    }

    /**
     * Programmatically add a new Retained Message.
     */
    public void addRetainedMessage(String topic, String message) {
    }
}
