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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
import messaging.job.WorkerCallback;
import model.job.type.IngestJob;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import util.PiazzaLogger;

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
	private static final String INGEST_TOPIC_NAME = IngestJob.class.getSimpleName();
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private IngestWorker ingestWorker;

	@Value("${vcap.services.pz-kafka.credentials.host}")
	private String KAFKA_ADDRESS;
	private String KAFKA_HOST;
	private String KAFKA_PORT;
	@Value("#{'${kafka.group}' + '-' + '${SPACE}'}")
	private String KAFKA_GROUP;
	@Value("${SPACE}")
	private String SPACE;

	private Producer<String, String> producer;
	private Map<String, Future<?>> runningJobs;
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
		// Initialize the Kafka Producer
		KAFKA_HOST = KAFKA_ADDRESS.split(":")[0];
		KAFKA_PORT = KAFKA_ADDRESS.split(":")[1];
		producer = KafkaClientFactory.getProducer(KAFKA_HOST, KAFKA_PORT);

		// Log the initialization.
		logger.log(String.format("Ingest listening to Kafka at %s in space %s.", KAFKA_ADDRESS, SPACE),
				PiazzaLogger.INFO);

		// Initialize the Thread Pool and Map of running Threads
		runningJobs = new HashMap<String, Future<?>>();

		// Start polling for Kafka Jobs on the Group Consumer.. This occurs on a
		// separate Thread so as not to block Spring.
		Thread ingestJobsThread = new Thread() {
			public void run() {
				pollIngestJobs();
			}
		};
		ingestJobsThread.start();

		// Start polling for Kafka Abort Jobs on the unique Consumer.
		Thread pollAbortThread = new Thread() {
			public void run() {
				pollAbortJobs();
			}
		};
		pollAbortThread.start();
	}

	/**
	 * Begins listening for Ingest Jobs.
	 */
	public void pollIngestJobs() {
		try {
			// Callback that will be invoked when a Worker completes. This will
			// remove the Job Id from the running Jobs list.
			WorkerCallback callback = new WorkerCallback() {
				@Override
				public void onComplete(String jobId) {
					runningJobs.remove(jobId);
				}
			};

			// Create the General Group Consumer
			Consumer<String, String> generalConsumer = KafkaClientFactory.getConsumer(KAFKA_HOST, KAFKA_PORT,
					KAFKA_GROUP);
			generalConsumer.subscribe(Arrays.asList(String.format("%s-%s", INGEST_TOPIC_NAME, SPACE)));

			// Poll
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = generalConsumer.poll(1000);
				// Handle new Messages on this topic.
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					// Create a new worker to process this message and add it to
					// the thread pool.
					Future<?> workerFuture = ingestWorker.run(consumerRecord, producer, callback);

					// Keep track of all Running Jobs
					runningJobs.put(consumerRecord.key(), workerFuture);
				}
			}
		} catch (WakeupException exception) {
			logger.log(String.format("Polling Thread forcefully closed: %s", exception.getMessage()),
					PiazzaLogger.FATAL);
		}
	}

	/**
	 * Stops all polling.
	 */
	public void stopPolling() {
		closed.set(true);
	}

	/**
	 * Begins listening for Abort Jobs. If a Job is owned by this component,
	 * then it will be terminated.
	 */
	public void pollAbortJobs() {
		try {
			// Create the Unique Consumer
			Consumer<String, String> uniqueConsumer = KafkaClientFactory.getConsumer(KAFKA_HOST, KAFKA_PORT,
					String.format("%s-%s", KAFKA_GROUP, UUID.randomUUID().toString()));
			uniqueConsumer.subscribe(Arrays.asList(String
					.format("%s-%s", JobMessageFactory.ABORT_JOB_TOPIC_NAME, SPACE)));

			// Poll
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = uniqueConsumer.poll(1000);
				// Handle new Messages on this topic.
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					// Determine if this Job Id is being processed by this
					// component.
					String jobId = consumerRecord.key();
					if (runningJobs.containsKey(jobId)) {
						// Cancel the Running Job
						runningJobs.get(jobId).cancel(true);
						// Remove it from the list of Running Jobs
						runningJobs.remove(jobId);
					}
				}
			}
		} catch (WakeupException exception) {
			logger.log(String.format("Polling Thread forcefully closed: %s", exception.getMessage()),
					PiazzaLogger.FATAL);
		}
	}

	/**
	 * Returns a list of the Job Ids that are currently being processed by this
	 * instance
	 * 
	 * @return The list of Job Ids
	 */
	public List<String> getRunningJobIds() {
		return new ArrayList<String>(runningJobs.keySet());
	}

}
