package ingest.messaging;

import ingest.database.PersistMetadata;
import ingest.inspect.Inspector;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
import model.job.Job;
import model.job.JobProgress;
import model.job.metadata.ResourceMetadata;
import model.job.type.IngestJob;
import model.status.StatusUpdate;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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
	@Autowired
	private PersistMetadata metadataPersist;
	@Value("${kafka.host}")
	private String KAFKA_HOST;
	@Value("${kafka.port}")
	private String KAFKA_PORT;
	@Value("${kafka.group}")
	private String KAFKA_GROUP;
	private Producer<String, String> producer;
	private Consumer<String, String> consumer;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private Inspector inspector = new Inspector();

	/**
	 * 
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
		// Listen for events
		listen();
	}

	/**
	 * Begins listening for events.
	 */
	public void listen() {
		try {
			consumer.subscribe(Arrays.asList(JobMessageFactory.CREATE_JOB_TOPIC_NAME));
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
				// Handle new Messages on this topic.
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					System.out.println("Processing Ingest Message " + consumerRecord.topic() + " with key "
							+ consumerRecord.key());
					// Wrap the JobRequest in the Job object
					try {
						ObjectMapper mapper = new ObjectMapper();
						Job job = mapper.readValue(consumerRecord.value(), Job.class);
						// Process Ingest Jobs
						if (job.jobType instanceof IngestJob) {
							// Update Status on Handling
							JobProgress jobProgress = new JobProgress(0);
							StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_RUNNING, jobProgress);
							producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate));

							// Process the Job based on its information and
							// retrieve any available metadata
							ResourceMetadata metadata = inspector.inspect((IngestJob) job.jobType);

							// Store the Metadata in the MongoDB metadata
							// collection
							metadataPersist.insertMetadata(metadata);

							// If applicable, store the spatial information in
							// the Piazza databases.
							// TODO: Database stuff

							// Update Status when Complete
							jobProgress.percentComplete = 100;
							statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS, jobProgress);
							producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate));
						}
					} catch (IOException jsonException) {
						System.out.println("Error Parsing Ingest Job Message.");
						jsonException.printStackTrace();
					} catch (MongoException mongoException) {
						System.out.println("Error committing Metadata object to Mongo Collections: "
								+ mongoException.getMessage());
						mongoException.printStackTrace();
					} catch (Exception exception) {
						System.out.println("An unexpected error occurred while processing the Job Message: "
								+ exception.getMessage());
						exception.printStackTrace();
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
}
