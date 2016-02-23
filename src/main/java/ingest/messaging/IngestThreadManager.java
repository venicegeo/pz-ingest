/**
 * Copyright 2016, RadiantBlue Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package ingest.messaging;

import ingest.inspect.Inspector;
import ingest.persist.PersistMetadata;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import messaging.job.KafkaClientFactory;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import util.PiazzaLogger;
import util.UUIDFactory;

/**
 * Main listener class for Ingest Jobs. Handles an incoming Ingest Job request
 * by indexing metadata, storing files, and updating appropriate database
 * tables. This class manages the Thread Pool of running Ingest Jobs.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class IngestThreadManager {
	private static final String INGEST_TOPIC_NAME = "ingest";
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private UUIDFactory uuidFactory;
	@Autowired
	private PersistMetadata metadataPersist;
	@Autowired
	private Inspector inspector;
	@Value("${kafka.host}")
	private String KAFKA_HOST;
	@Value("${kafka.port}")
	private String KAFKA_PORT;
	@Value("${kafka.group}")
	private String KAFKA_GROUP;
	private Producer<String, String> producer;
	private Consumer<String, String> consumer;
	private ThreadPoolExecutor executor;
	private final AtomicBoolean closed = new AtomicBoolean(false);

	/**
	 * Worker class that listens for and processes Ingestion messages.
	 */
	public IngestThreadManager() {
	}

	/**
	 * Initializes the Thread Pool and begins Polling for Jobs.
	 */
	@PostConstruct
	public void initialize() {
		// Initialize the Kafka consumer/producer
		producer = KafkaClientFactory.getProducer(KAFKA_HOST, KAFKA_PORT);
		consumer = KafkaClientFactory.getConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_GROUP);

		// Initialize the Thread Pool
		executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

		// Start polling for Kafka Jobs. This occurs on a separate Thread so as
		// not to block Spring.
		Thread pollThread = new Thread() {
			public void run() {
				listen();
			}
		};
		pollThread.start();
	}

	@PreDestroy
	public void cleanup() {
		consumer.close();
		producer.close();
		executor.shutdown();
	}

	/**
	 * Begins listening for events.
	 */
	@Async
	public void listen() {
		try {
			consumer.subscribe(Arrays.asList(INGEST_TOPIC_NAME));
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
				// Handle new Messages on this topic.
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					// Create a new worker to process this message and add it to
					// the thread pool.
					IngestWorker ingestWorker = new IngestWorker(consumerRecord, inspector, producer, uuidFactory,
							logger);
					executor.execute(ingestWorker);
				}
			}
		} catch (WakeupException exception) {
			logger.log(String.format("Ingest Listener Thread forcefully shut: %s", exception.getMessage()),
					PiazzaLogger.FATAL);
			// Ignore exception if closing
			if (!closed.get()) {
				throw exception;
			}
		} finally {
			cleanup();
		}
	}

}
