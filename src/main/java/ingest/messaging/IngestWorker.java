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

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
import model.data.DataResource;
import model.job.Job;
import model.job.JobProgress;
import model.job.type.IngestJob;
import model.status.StatusUpdate;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoException;

/**
 * Main listener class for Ingest Jobs. Handles an incoming Ingest Job request
 * by indexing metadata, storing files, and updating appropriate database
 * tables.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class IngestWorker {
	private static final String INGEST_TOPIC_NAME = "ingest";
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
	private final AtomicBoolean closed = new AtomicBoolean(false);

	/**
	 * Worker class that listens for and processes Ingestion messages.
	 */
	public IngestWorker() {
	}

	/**
	 * 
	 */
	@PostConstruct
	public void initialize() {
		// Initialize the Kafka consumer/producer
		producer = KafkaClientFactory.getProducer(KAFKA_HOST, KAFKA_PORT);
		consumer = KafkaClientFactory.getConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_GROUP);
		// Listen for events TODO: Talk to Sonny about moving to @Async method
		Thread pollThread = new Thread() {
			public void run() {
				listen();
			}
		};
		pollThread.start();
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
					System.out.println("Processing Ingest Message " + consumerRecord.topic() + " with key "
							+ consumerRecord.key());
					try {
						// Parse the Job from the Kafka Message
						ObjectMapper mapper = new ObjectMapper();
						Job job = mapper.readValue(consumerRecord.value(), Job.class);
						IngestJob ingestJob = (IngestJob) job.getJobType();
						// Get the description of the Data to be ingested
						DataResource dataResource = ingestJob.getData();

						// Assign a Resource ID to the incoming DataResource.
						if (dataResource.getDataId() == null) {
							String dataId = UUID.randomUUID().toString();
							dataResource.setDataId(dataId);
						}

						// Update Status on Handling
						JobProgress jobProgress = new JobProgress(0);
						StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_RUNNING, jobProgress);
						producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate));

						// Inspect processes the Data item. Adds appropriate
						// metadata, and stores data if requested.
						inspector.inspect(dataResource, ingestJob.getHost());

						// Update Status when Complete
						jobProgress.percentComplete = 100;
						statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS, jobProgress);
						// The result of this Job was creating a resource at the
						// specified ID.
						statusUpdate.setResult(dataResource.getDataId());
						producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate));
					} catch (IOException jsonException) {
						handleException(consumerRecord.key(), jsonException);
						System.out.println("Error Parsing Ingest Job Message.");
					} catch (MongoException mongoException) {
						handleException(consumerRecord.key(), mongoException);
						System.out.println("Error committing Metadata object to Mongo Collections: "
								+ mongoException.getMessage());
					} catch (Exception exception) {
						handleException(consumerRecord.key(), exception);
						System.out.println("An unexpected error occurred while processing the Job Message: "
								+ exception.getMessage());
					}
				}
			}
		} catch (WakeupException exception) {
			// Ignore exception if closing
			if (!closed.get()) {
				throw exception;
			}
		} finally {
			consumer.close();
		}
	}

	/**
	 * Handles the common exception actions that should be taken upon errors
	 * encountered during the inspection/parsing/loading process. Sends the
	 * error message to Kafka that this Job has errored out.
	 * 
	 * @param jobId
	 * @param exception
	 */
	private void handleException(String jobId, Exception exception) {
		exception.printStackTrace();
		try {
			producer.send(JobMessageFactory.getUpdateStatusMessage(jobId, new StatusUpdate(StatusUpdate.STATUS_ERROR)));
		} catch (JsonProcessingException jsonException) {
			System.out.println("Could update Job Manager with failure event in Ingest Worker. Error creating message: "
					+ jsonException.getMessage());
			jsonException.printStackTrace();
		}
	}
}
