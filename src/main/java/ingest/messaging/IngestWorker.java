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
import ingest.utility.IngestUtilities;

import java.io.IOException;
import java.util.concurrent.Future;

import messaging.job.JobMessageFactory;
import messaging.job.WorkerCallback;
import model.data.DataResource;
import model.data.FileRepresentation;
import model.data.location.FileLocation;
import model.data.location.FolderShare;
import model.data.location.S3FileStore;
import model.job.Job;
import model.job.JobProgress;
import model.job.metadata.ResourceMetadata;
import model.job.result.type.DataResult;
import model.job.result.type.ErrorResult;
import model.job.type.IngestJob;
import model.job.type.SearchMetadataIngestJob;
import model.response.PiazzaResponse;
import model.status.StatusUpdate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import util.PiazzaLogger;
import util.UUIDFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoException;

/**
 * Worker class that handles a specific Ingest Job. The threads are managed by
 * the IngestThreadManager class.
 * 
 * @author Patrick.Doody & Sonny.Saniev & Russell.Orf
 * 
 */
@Component
public class IngestWorker {
	@Value("${SPACE}")
	private String SPACE;
	@Value("${workflow.event.id}")
	private String EVENT_ID;
	@Value("${workflow.url}")
	private String WORKFLOW_URL;
	@Value("${search.url}")
	private String SEARCH_URL;
	@Value("${workflow.endpoint}")
	private String WORKFLOW_ENDPOINT;
	@Value("${search.ingest.endpoint}")
	private String SEARCH_ENDPOINT;
	@Value("${vcap.services.pz-blobstore.credentials.bucket}")
	private String AMAZONS3_BUCKET_NAME;

	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private Inspector inspector;
	@Autowired
	private IngestUtilities ingestUtilities;
	@Autowired
	private UUIDFactory uuidFactory;

	/**
	 * Creates a new Worker Thread for the specified Kafka Message containing an
	 * Ingest Job.
	 * 
	 * @param consumerRecord
	 *            The Kafka Message containing the Job.
	 * @param producer
	 *            The Kafka producer, used to send update messages
	 * @param callback
	 *            The callback that will be invoked when this Job has finished
	 *            processing (error or success, regardless)
	 */
	@Async
	public Future<DataResource> run(ConsumerRecord<String, String> consumerRecord, Producer<String, String> producer,
			WorkerCallback callback) {
		DataResource dataResource = null;

		try {
			// Log
			logger.log(String.format("Processing Data Load for Topic %s with Key %s", consumerRecord.topic(),
					consumerRecord.key()), PiazzaLogger.INFO);

			// Parse the Job from the Kafka Message
			ObjectMapper mapper = new ObjectMapper();
			Job job = mapper.readValue(consumerRecord.value(), Job.class);
			IngestJob ingestJob = (IngestJob) job.getJobType();
			// Get the description of the Data to be ingested
			dataResource = ingestJob.getData();

			// Assign a Resource ID to the incoming DataResource.
			if (dataResource.getDataId() == null) {
				String dataId = uuidFactory.getUUID();
				dataResource.setDataId(dataId);
			}

			// Log what we're going to Ingest
			logger.log(String.format(
					"Inspected Load Job; begin Loading Data %s of Type %s. Hosted: %s with Job ID of %s",
					dataResource.getDataId(), dataResource.getDataType().getType(), ingestJob.getHost().toString(),
					job.getJobId()), PiazzaLogger.INFO);

			// Update Status on Handling
			JobProgress jobProgress = new JobProgress(0);
			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_RUNNING, jobProgress);
			producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate, SPACE));

			if (ingestJob.getData().getDataType() instanceof FileRepresentation) {
				FileRepresentation fileRep = (FileRepresentation) ingestJob.getData().getDataType();
				FileLocation fileLoc = fileRep.getLocation();
				fileLoc.setFileSize(ingestUtilities.getFileSize(dataResource));

				if (ingestJob.getHost().booleanValue()) {
					// Copy to Piazza S3 bucket if hosted = true; If already in
					// S3, make
					// sure it's different than the Piazza S3; Depending on the
					// Type of file
					switch (fileLoc.getType()) {
					case S3FileStore.type:
						S3FileStore s3FS = (S3FileStore) fileLoc;
						if (!s3FS.getBucketName().equals(AMAZONS3_BUCKET_NAME)) {
							ingestUtilities.copyS3Source(dataResource);
							fileRep.setLocation(new S3FileStore(AMAZONS3_BUCKET_NAME, dataResource.getDataId() + "-"
									+ s3FS.getFileName(), s3FS.getFileSize(), s3FS.getDomainName()));
						}
						break;
					case FolderShare.type:
						ingestUtilities.copyS3Source(dataResource);
						break;
					}
				}
			}

			if (dataResource.metadata == null) {
				dataResource.metadata = new ResourceMetadata();
			}

			// Inspect processes the Data item, adds appropriate metadata and
			// stores if requested
			inspector.inspect(dataResource, ingestJob.getHost());

			// Update Status when Complete
			jobProgress.percentComplete = 100;
			statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS, jobProgress);

			// The result of this Job was creating a resource at the specified
			// ID.
			statusUpdate.setResult(new DataResult(dataResource.getDataId()));
			producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate, SPACE));

			// Console Logging
			logger.log(
					String.format("Successful Load of Data %s for Job %s", dataResource.getDataId(), job.getJobId()),
					PiazzaLogger.INFO);

			// Fire the Event to Pz-Search that new metadata has been ingested
			try {
				dispatchMetadataIngestMessage(dataResource, String.format("%s/%s/", SEARCH_URL, SEARCH_ENDPOINT));
			} catch (Exception exception) {
				logger.log(String.format("Metadata Load for %s for Job %s could not be sent to the Search Service: %s",
						dataResource.getDataId(), job.getJobId(), exception.getMessage()), PiazzaLogger.ERROR);
			}

			// Fire the Event to Pz-Workflow that a successful Ingest has taken
			// place.
			try {
				dispatchWorkflowEvent(job, dataResource, EVENT_ID,
						String.format("%s/%s", WORKFLOW_URL, WORKFLOW_ENDPOINT));
			} catch (Exception exception) {
				logger.log(String.format(
						"Event for Loading of Data %s for Job %s could not be sent to the Workflow Service: %s",
						dataResource.getDataId(), job.getJobId(), exception.getMessage()), PiazzaLogger.ERROR);
			}
		} catch (IOException jsonException) {
			handleException(producer, consumerRecord.key(), jsonException);
			System.out.println("Error Parsing Data Load Job Message.");
		} catch (MongoException mongoException) {
			handleException(producer, consumerRecord.key(), mongoException);
			System.out.println("Error committing Metadata object to Mongo Collections: " + mongoException.getMessage());
		} catch (Exception exception) {
			handleException(producer, consumerRecord.key(), exception);
			System.out.println("An unexpected error occurred while processing the Job Message: "
					+ exception.getMessage());
		} finally {
			callback.onComplete(consumerRecord.key());
		}

		return new AsyncResult<DataResource>(dataResource);
	}

	/**
	 * Dispatches the REST POST request to the pz-search service for the
	 * ingestion of metadata for the newly ingested data resource.
	 * 
	 * @param dataResource
	 *            The Data Resource to ingest metadata for
	 */
	private void dispatchMetadataIngestMessage(DataResource dataResource, String searchUrl) {
		// Create the Ingest Job that the Search Service Expects
		SearchMetadataIngestJob job = new SearchMetadataIngestJob();
		job.data = dataResource;

		// Create Request Objects
		RestTemplate restTemplate = new RestTemplate();
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<SearchMetadataIngestJob> entity = new HttpEntity<SearchMetadataIngestJob>(job, headers);

		// Send the Request
		try {
			restTemplate.postForObject(searchUrl, entity, PiazzaResponse.class);
		} catch (Exception exception) {
			// Log failure of Ingest
			logger.log(String.format("Search Metadata Upload for Data %s failed with error: %s",
					dataResource.getDataId(), exception.getMessage()), PiazzaLogger.ERROR);
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
	private void dispatchWorkflowEvent(Job job, DataResource dataResource, String eventId, String workflowUrl)
			throws Exception {
		RestTemplate restTemplate = new RestTemplate();
		HttpHeaders headers = new HttpHeaders();
		IngestEvent ingestEvent = new IngestEvent(eventId, job, dataResource);
		String ingestString = new ObjectMapper().writeValueAsString(ingestEvent);
		HttpEntity<String> entity = new HttpEntity<String>(ingestString, headers);
		headers.setContentType(MediaType.APPLICATION_JSON);
		ResponseEntity<Object> response = restTemplate.postForEntity(workflowUrl, entity, Object.class);
		if (response.getStatusCode() == HttpStatus.CREATED) {
			// The Event was successfully received by pz-workflow
			logger.log(
					String.format(
							"Event for Loading of Data %s for Job %s was successfully sent to the Workflow Service with response %s",
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
	private void handleException(Producer<String, String> producer, String jobId, Exception exception) {
		exception.printStackTrace();
		logger.log(String.format("An Error occurred during Data Load for Job %s: %s", jobId, exception.getMessage()),
				PiazzaLogger.ERROR);
		try {
			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_ERROR);
			statusUpdate.setResult(new ErrorResult("Error while Loading the Data.", exception.getMessage()));
			producer.send(JobMessageFactory.getUpdateStatusMessage(jobId, statusUpdate, SPACE));
		} catch (JsonProcessingException jsonException) {
			System.out.println("Could update Job Manager with failure event in Loader Worker. Error creating message: "
					+ jsonException.getMessage());
			jsonException.printStackTrace();
		}
	}
}
