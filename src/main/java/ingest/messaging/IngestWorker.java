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

import ingest.event.IngestEvent;
import ingest.inspect.Inspector;

import java.io.IOException;

import messaging.job.JobMessageFactory;
import messaging.job.WorkerCallback;
import model.data.DataResource;
import model.job.Job;
import model.job.JobProgress;
import model.job.result.type.DataResult;
import model.job.result.type.ErrorResult;
import model.job.type.IngestJob;
import model.status.StatusUpdate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import util.PiazzaLogger;
import util.UUIDFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoException;

/**
 * Worker class that handles a specific Ingest Job. These Runnables are managed
 * by the ExecutorService defined in the IngestManager class.
 * 
 * @author Patrick.Doody
 * 
 */
public class IngestWorker implements Runnable {
	private ConsumerRecord<String, String> consumerRecord;
	private Producer<String, String> producer;
	private PiazzaLogger logger;
	private UUIDFactory uuidFactory;
	private Inspector inspector;
	private WorkerCallback callback;

	private String EVENT_ID;
	private String WORKFLOW_URL;

	/**
	 * Creates a new Worker Thread for the specified Kafka Message containing an
	 * Ingest Job.
	 * 
	 * @param consumerRecord
	 *            The Kafka Message containing the Job.
	 * @param inspector
	 *            The Inspector
	 * @param producer
	 *            The Kafka producer, used to send update messages
	 * @param callback
	 *            The callback that will be invoked when this Job has finished
	 *            processing (error or success, regardless)
	 * @param uuidFactory
	 *            UUIDGen factory used to create UUIDs for Data Resource items
	 * @param logger
	 *            The Piazza Logger instance for logging
	 * @param eventId
	 *            The ID of the pz-workflow Ingest name
	 * @param workflowUrl
	 *            The URL of the pz-workflow Alerter endpoint that will be
	 *            POSTed to notify upon successful ingest
	 */
	public IngestWorker(ConsumerRecord<String, String> consumerRecord, Inspector inspector,
			Producer<String, String> producer, WorkerCallback callback, UUIDFactory uuidFactory, PiazzaLogger logger,
			String eventId, String workflowUrl) {
		this.consumerRecord = consumerRecord;
		this.inspector = inspector;
		this.producer = producer;
		this.callback = callback;
		this.uuidFactory = uuidFactory;
		this.logger = logger;
		this.EVENT_ID = eventId;
		this.WORKFLOW_URL = workflowUrl;
	}

	@Override
	public void run() {
		try {
			// Log
			logger.log(
					String.format("Processing Ingest for Topic %s with Key %s", consumerRecord.topic(),
							consumerRecord.key()), PiazzaLogger.INFO);

			// Parse the Job from the Kafka Message
			ObjectMapper mapper = new ObjectMapper();
			Job job = mapper.readValue(consumerRecord.value(), Job.class);
			IngestJob ingestJob = (IngestJob) job.getJobType();
			// Get the description of the Data to be ingested
			DataResource dataResource = ingestJob.getData();

			// Assign a Resource ID to the incoming DataResource.
			if (dataResource.getDataId() == null) {
				String dataId = uuidFactory.getUUID();
				dataResource.setDataId(dataId);
			}

			// Log what we're going to Ingest
			logger.log(String.format(
					"Inspected Ingest Job; begin Ingesting Data %s of Type %s. Hosted: %s with Ingest Job ID of %s",
					dataResource.getDataId(), dataResource.getDataType().getType(), ingestJob.getHost().toString(),
					job.getJobId()), PiazzaLogger.INFO);

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
			statusUpdate.setResult(new DataResult(dataResource.getDataId()));
			producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate));

			// Console Logging
			logger.log(
					String.format("Successful Ingest of Data %s for Job %s", dataResource.getDataId(), job.getJobId()),
					PiazzaLogger.INFO);

			// Fire the Event to Pz-Workflow that a successful Ingest has taken
			// place.
			try {
				dispatchWorkflowEvent(job, dataResource);
			} catch (Exception exception) {
				logger.log(String.format(
						"Event for Ingest of Data %s for Job %s could not be sent to the Workflow Service: %s",
						dataResource.getDataId(), job.getJobId(), exception.getMessage()), PiazzaLogger.ERROR);
			}
		} catch (IOException jsonException) {
			handleException(consumerRecord.key(), jsonException);
			System.out.println("Error Parsing Ingest Job Message.");
		} catch (MongoException mongoException) {
			handleException(consumerRecord.key(), mongoException);
			System.out.println("Error committing Metadata object to Mongo Collections: " + mongoException.getMessage());
		} catch (Exception exception) {
			handleException(consumerRecord.key(), exception);
			System.out.println("An unexpected error occurred while processing the Job Message: "
					+ exception.getMessage());
		} finally {
			callback.onComplete(consumerRecord.key());
		}
	}

	/**
	 * Dispatches the REST POST request to the pz-workflow service for the event
	 * that data has been successfully ingested.
	 * 
	 * @param job
	 *            The job
	 * @param dataResource
	 *            The DataResource that has been ingested
	 */
	private void dispatchWorkflowEvent(Job job, DataResource dataResource) throws Exception {
		RestTemplate restTemplate = new RestTemplate();
		HttpHeaders headers = new HttpHeaders();
		IngestEvent ingestEvent = new IngestEvent(EVENT_ID, job, dataResource);
		String ingestString = new ObjectMapper().writeValueAsString(ingestEvent);
		HttpEntity<String> entity = new HttpEntity<String>(ingestString, headers);
		headers.setContentType(MediaType.APPLICATION_JSON);
		ResponseEntity<Object> response = restTemplate.postForEntity(WORKFLOW_URL, entity, Object.class);
		if (response.getStatusCode() == HttpStatus.CREATED) {
			// The Event was successfully received by pz-workflow
			logger.log(
					String.format(
							"Event for Ingest of Data %s for Job %s was successfully sent to the Workflow Service with response %s",
							dataResource.getDataId(), job.getJobId(), response.getBody().toString()), PiazzaLogger.INFO);

		} else {
			// 201 not received. Throw an exception that something went wrong.
			throw new Exception(String.format("Status code %s received.", response.getStatusCode()));
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
		logger.log(
				String.format("An Error occurred during Data Ingestion for Job %s: %s", jobId, exception.getMessage()),
				PiazzaLogger.ERROR);
		try {
			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_ERROR);
			statusUpdate.setResult(new ErrorResult("Error while Ingesting the Data.", exception.getMessage()));
			producer.send(JobMessageFactory.getUpdateStatusMessage(jobId, statusUpdate));
		} catch (JsonProcessingException jsonException) {
			System.out.println("Could update Job Manager with failure event in Ingest Worker. Error creating message: "
					+ jsonException.getMessage());
			jsonException.printStackTrace();
		}
	}

}
