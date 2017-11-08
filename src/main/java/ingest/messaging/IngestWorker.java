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

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
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
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.amazonaws.AmazonClientException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import exception.DataInspectException;
import exception.InvalidInputException;
import ingest.inspect.Inspector;
import ingest.utility.IngestUtilities;
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
import model.logger.AuditElement;
import model.logger.Severity;
import model.response.EventTypeListResponse;
import model.response.PiazzaResponse;
import model.status.StatusUpdate;
import model.workflow.Event;
import model.workflow.EventType;
import util.PiazzaLogger;
import util.UUIDFactory;

/**
 * Worker class that handles a specific Ingest Job. The threads are managed by the IngestThreadManager class.
 * 
 * @author Patrick.Doody & Sonny.Saniev & Russell.Orf
 * 
 */
@Component
public class IngestWorker {
	@Value("${SPACE}")
	private String SPACE;
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
	@Autowired
	private RestTemplate restTemplate;
	@Autowired
	private Queue updateJobsQueue;
	@Autowired
	private RabbitTemplate rabbitTemplate;
	@Autowired
	private ObjectMapper mapper;

	private static final Logger LOG = LoggerFactory.getLogger(IngestWorker.class);
	private static final String INGEST_EVENT_TYPE_NAME = "piazza:ingest";

	/**
	 * Creates a new Worker Thread for the specified Message containing an Ingest Job.
	 * 
	 * @param job
	 *            The Job Model containing all Job information
	 * @param callback
	 *            The callback that will be invoked when this Job has finished processing (error or success, regardless)
	 */
	@Async
	public Future<DataResource> run(Job job, WorkerCallback callback) {
		DataResource dataResource = null;
		try {
			// Log
			logger.log(String.format("Processing Data Load for IngestJob for Job Id %s", job.getJobId()), Severity.INFORMATIONAL);

			// Parse the Job from the Message
			IngestJob ingestJob = (IngestJob) job.getJobType();

			// Get the description of the Data to be ingested
			dataResource = ingestJob.getData();

			// Assign a Resource Id to the incoming DataResource.
			if ((dataResource.getDataId() == null) || (dataResource.getDataId().isEmpty())) {
				String dataId = uuidFactory.getUUID();
				dataResource.setDataId(dataId);
			}

			// Ensure we have a metadata wrapper.
			if (dataResource.metadata == null) {
				dataResource.metadata = new ResourceMetadata();
			}

			// Log what we're going to Ingest
			logger.log(
					String.format("Begin Processing Load Job; begin Loading Data %s of Type %s. Hosted: %s with Job Id of %s",
							dataResource.getDataId(), dataResource.getDataType().getClass().getSimpleName(), ingestJob.getHost().toString(),
							job.getJobId()),
					Severity.INFORMATIONAL, new AuditElement(job.getJobId(), "beginLoadData", dataResource.getDataId()));

			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			// Update Status on Handling
			JobProgress jobProgress = new JobProgress(0);
			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_RUNNING, jobProgress);
			statusUpdate.setJobId(job.getJobId());
			rabbitTemplate.convertAndSend(JobMessageFactory.PIAZZA_EXCHANGE_NAME, updateJobsQueue.getName(),
					mapper.writeValueAsString(statusUpdate));

			if (ingestJob.getData().getDataType() instanceof FileRepresentation) {
				processFileRepresentation(ingestJob, dataResource);
			}

			dataResource.metadata.setCreatedBy(job.getCreatedBy());
			dataResource.metadata.setCreatedOn(job.getCreatedOnString());
			dataResource.metadata.createdByJobId = job.getJobId();

			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			// Inspect processes the Data item, adds appropriate metadata and
			// stores if requested
			inspector.inspect(dataResource, ingestJob.getHost());

			// Update Status when Complete
			jobProgress.percentComplete = 100;
			statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS, jobProgress);
			statusUpdate.setJobId(job.getJobId());

			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			// The result of this Job was creating a resource at the specified
			// Id.
			statusUpdate.setResult(new DataResult(dataResource.getDataId()));
			rabbitTemplate.convertAndSend(JobMessageFactory.PIAZZA_EXCHANGE_NAME, updateJobsQueue.getName(),
					mapper.writeValueAsString(statusUpdate));

			// Console Logging
			logger.log(String.format("Successful Load of Data %s for Job %s", dataResource.getDataId(), job.getJobId()),
					Severity.INFORMATIONAL, new AuditElement(job.getJobId(), "loadedData", dataResource.getDataId()));

			// Fire Events
			fireEvents(job, dataResource);
		} catch (AmazonClientException amazonException) {
			String systemError = String.format("Error interacting with S3: %s", amazonException.getMessage());
			String userError = "There was an issue with S3 during Data Load. Please contact a Piazza administrator for details.";
			LOG.error(systemError, amazonException);
			logger.log(systemError, Severity.ERROR);
			handleException(job.getJobId(), new DataInspectException(userError));
		} catch (DataInspectException exception) {
			handleException(job.getJobId(), exception);
			LOG.error("An Inspection Error occurred while processing the Job Message: " + exception.getMessage(), exception);
		} catch (InterruptedException exception) { // NOSONAR
			String error = String.format("Thread interrupt received for Job %s", job.getJobId());
			LOG.error(error, exception);
			logger.log(error, Severity.INFORMATIONAL, new AuditElement(job.getJobId(), "cancelledIngestJob", ""));
			handleInterruptedException(job.getJobId());
		} catch (IOException jsonException) {
			handleException(job.getJobId(), jsonException);
			LOG.error("Error Parsing Data Load Job Message.", jsonException);
		} catch (Exception exception) {
			handleException(job.getJobId(), exception);
			LOG.error("An unexpected error occurred while processing the Job Message: " + exception.getMessage(), exception);
		} finally {
			if (callback != null) {
				callback.onComplete(job.getJobId());
			}
		}

		return new AsyncResult<DataResource>(dataResource);
	}

	private void processFileRepresentation(final IngestJob ingestJob, final DataResource dataResource)
			throws InvalidInputException, IOException {
		FileRepresentation fileRep = (FileRepresentation) ingestJob.getData().getDataType();
		FileLocation fileLoc = fileRep.getLocation();
		if (fileLoc != null) {
			fileLoc.setFileSize(ingestUtilities.getFileSize(dataResource));
		}

		if (ingestJob.getHost().booleanValue() && (fileLoc != null)) {
			// Copy to Piazza S3 bucket if hosted is true. If already in
			// S3, make sure it's different than the Piazza S3
			// Depending on the Type of file
			if (fileLoc instanceof S3FileStore) {
				S3FileStore s3FS = (S3FileStore) fileLoc;
				if (!s3FS.getBucketName().equals(AMAZONS3_BUCKET_NAME)) {
					ingestUtilities.copyS3Source(dataResource);
					fileRep.setLocation(new S3FileStore(AMAZONS3_BUCKET_NAME, dataResource.getDataId() + "-" + s3FS.getFileName(),
							s3FS.getFileSize(), s3FS.getDomainName()));
				}
			} else if (fileLoc instanceof FolderShare) {
				ingestUtilities.copyS3Source(dataResource);
			}
		}
	}

	private void fireEvents(final Job job, final DataResource dataResource) {
		// Fire the Event to Pz-Search that new metadata has been ingested
		try {
			dispatchMetadataIngestMessage(dataResource, String.format("%s/%s/", SEARCH_URL, SEARCH_ENDPOINT));
		} catch (HttpClientErrorException | HttpServerErrorException exception) {
			String error = String.format("Metadata Load for %s for Job %s could not be sent to the Search Service: %s",
					dataResource.getDataId(), job.getJobId(), exception.getResponseBodyAsString());
			LOG.error(error, exception);
			logger.log(error, Severity.ERROR);
		} catch (Exception genException) {
			String error = String.format("Metadata Load for %s for Job %s could not be sent to the Search Service: %s",
					dataResource.getDataId(), job.getJobId(), genException.getMessage());
			LOG.error(error, genException);
			logger.log(error, Severity.ERROR);
		}

		// Fire the Event to Pz-Workflow that a successful Ingest has taken
		// place.
		try {
			dispatchWorkflowEvent(job, dataResource, String.format("%s/%s", WORKFLOW_URL, WORKFLOW_ENDPOINT));
		} catch (JsonParseException | JsonMappingException exception) {
			String error = String.format("Could not create JSON to send to Workflow Service Event: %s", exception.getMessage());
			LOG.error(error, exception);
			logger.log(error, Severity.ERROR);
		} catch (HttpClientErrorException | HttpServerErrorException exception) {
			String error = String.format("Event for Loading of Data %s for Job %s could not be sent to the Workflow Service: %s",
					dataResource.getDataId(), job.getJobId(), exception.getResponseBodyAsString());
			LOG.error(error, exception);
			logger.log(error, Severity.ERROR);
		} catch (Exception exception) {
			LOG.error(exception.getMessage(), exception);
			logger.log(exception.getMessage(), Severity.WARNING);
		}
	}

	/**
	 * Dispatches the REST POST request to the pz-search service for the ingestion of metadata for the newly ingested
	 * data resource.
	 * 
	 * @param dataResource
	 *            The Data Resource to ingest metadata for
	 */
	private void dispatchMetadataIngestMessage(DataResource dataResource, String searchUrl) {
		// Create the Ingest Job that the Search Service Expects
		SearchMetadataIngestJob job = new SearchMetadataIngestJob();
		job.data = dataResource;

		// Create Request Objects
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<SearchMetadataIngestJob> entity = new HttpEntity<SearchMetadataIngestJob>(job, headers);

		// Send the Request
		restTemplate.postForObject(searchUrl, entity, PiazzaResponse.class);
	}

	/**
	 * Dispatches the REST POST request to the pz-workflow service for the event that data has been successfully
	 * ingested.
	 * 
	 * @param job
	 *            The job
	 * @param dataResource
	 *            The DataResource that has been ingested
	 */
	private void dispatchWorkflowEvent(Job job, DataResource dataResource, String workflowUrl)
			throws JsonParseException, JsonMappingException, RestClientException, IOException {
		ObjectMapper objectMapper = new ObjectMapper();

		// Make an initial request to Workflow in order to get the UUID of the System Event.
		String url = String.format("%s/%s?name=%s", WORKFLOW_URL, "eventType", INGEST_EVENT_TYPE_NAME);
		EventType eventType = objectMapper.readValue(restTemplate.getForObject(url, String.class), EventTypeListResponse.class).data.get(0);

		// Create the Event to Post
		Event event = new Event();
		event.createdBy = job.getCreatedBy();
		event.eventTypeId = eventType.eventTypeId;

		// Populate the Event Data
		event.data = new HashMap<String, Object>();
		event.data.put("dataId", dataResource.getDataId());
		event.data.put("dataType", dataResource.getDataType().getClass().getSimpleName());
		if (dataResource.getSpatialMetadata() != null) {
			event.data.put("epsg", dataResource.getSpatialMetadata().getEpsgCode());
			event.data.put("minX", dataResource.getSpatialMetadata().getMinX());
			event.data.put("minY", dataResource.getSpatialMetadata().getMinY());
			event.data.put("maxX", dataResource.getSpatialMetadata().getMaxX());
			event.data.put("maxY", dataResource.getSpatialMetadata().getMaxY());
		} else {
			logger.log(String.format("Could not populate Spatial Metadata block for %s, because no Spatial Metadata was found.",
					dataResource.getDataId()), Severity.WARNING);
		}
		event.data.put("hosted", ((IngestJob) job.getJobType()).getHost());

		// Send the Event
		HttpHeaders headers = new HttpHeaders();
		String eventString = objectMapper.writeValueAsString(event);
		HttpEntity<String> entity = new HttpEntity<String>(eventString, headers);
		headers.setContentType(MediaType.APPLICATION_JSON);
		ResponseEntity<Object> response = restTemplate.postForEntity(workflowUrl, entity, Object.class);
		if (response.getStatusCode() == HttpStatus.CREATED) {
			// The Event was successfully received by pz-workflow
			logger.log(
					String.format("Event for Loading of Data %s for Job %s was successfully sent to the Workflow Service with response %s",
							dataResource.getDataId(), job.getJobId(), response.getBody().toString()),
					Severity.INFORMATIONAL);
		} else {
			// 201 not received. Throw an exception that something went wrong.
			throw new HttpServerErrorException(response.getStatusCode(),
					String.format(
							"Non-201 CREATED Status code received from Workflow upon Event creation: %s. This creation may not have succeeded.",
							response.getStatusCode()));
		}
	}

	/**
	 * Handles the common exception actions that should be taken upon errors encountered during the
	 * inspection/parsing/loading process. Sends the error message that this Job has errored out.
	 * 
	 * @param jobId
	 * @param exception
	 */
	private void handleException(String jobId, Exception exception) {
		String error = String.format("An Error occurred during Data Load for Job %s: %s", jobId, exception.getMessage());
		LOG.error(error);
		logger.log(error, Severity.ERROR, new AuditElement(jobId, "failedToLoadData", ""));
		try {
			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_ERROR);
			statusUpdate.setResult(new ErrorResult("Error while Loading the Data.", exception.getMessage()));
			statusUpdate.setJobId(jobId);
			rabbitTemplate.convertAndSend(JobMessageFactory.PIAZZA_EXCHANGE_NAME, updateJobsQueue.getName(),
					mapper.writeValueAsString(statusUpdate));
		} catch (JsonProcessingException jsonException) {
			LOG.info("Could update Job Manager with failure event in Loader Worker. Error creating message: " + jsonException.getMessage(),
					jsonException);
		}
	}

	private void handleInterruptedException(final String jobId) {
		StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_CANCELLED);
		statusUpdate.setJobId(jobId);
		try {
			rabbitTemplate.convertAndSend(JobMessageFactory.PIAZZA_EXCHANGE_NAME, updateJobsQueue.getName(),
					mapper.writeValueAsString(statusUpdate));
		} catch (JsonProcessingException jsonException) {
			String error = String.format(
					"Error sending Cancelled Status from Job %s: %s. The Job was cancelled, but its status will not be updated in the Job Manager.",
					jobId, jsonException.getMessage());
			LOG.error(error, jsonException);
			logger.log(error, Severity.ERROR, new AuditElement(jobId, "failedToSendCancelledStatus", ""));
		}
	}
}
