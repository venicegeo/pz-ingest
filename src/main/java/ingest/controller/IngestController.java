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
package ingest.controller;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import exception.InvalidInputException;
import ingest.messaging.IngestThreadManager;
import ingest.persist.DatabaseAccessor;
import ingest.utility.IngestUtilities;
import model.data.DataResource;
import model.job.metadata.ResourceMetadata;
import model.logger.AuditElement;
import model.logger.Severity;
import model.response.ErrorResponse;
import model.response.PiazzaResponse;
import model.response.SuccessResponse;
import util.PiazzaLogger;

/**
 * REST Controller for ingest. Ingest has no functional REST endpoints, as all communication is done through Kafka.
 * However, this controller exposes useful debug/status endpoints which can be used administratively.
 * 
 * @author Patrick.Doody
 * 
 */
@RestController
public class IngestController {
	@Autowired
	private IngestThreadManager threadManager;
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private DatabaseAccessor accessor;
	@Autowired
	private IngestUtilities ingestUtil;
	@Autowired
	private ThreadPoolTaskExecutor threadPoolTaskExecutor;
	@Autowired
	private RestTemplate restTemplate;

	@Value("${access.url}")
	private String ACCESS_URL;
	@Value("${search.url}")
	private String SEARCH_URL;
	@Value("${search.delete}")
	private String SEARCH_DELETE_SUFFIX;

	private static final Logger LOG = LoggerFactory.getLogger(IngestController.class);
	private static final String LOADER = "Loader";
	private static final String INGEST = "ingest";
	
	/**
	 * Deletes the Data resource object from the Resources collection.
	 * 
	 * @param dataId
	 *            Id of the Resource
	 * @return The resource matching the specified Id
	 */
	@RequestMapping(value = "/data/{dataId}", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<PiazzaResponse> deleteData(@PathVariable(value = "dataId") String dataId) {
		try {
			if (dataId.isEmpty()) {
				throw new InvalidInputException("No Data Id specified.");
			}
			// Query for the Data Id
			DataResource data = accessor.getData(dataId);
			if (data == null) {
				logger.log(String.format("Data not found for requested Id %s", dataId), Severity.WARNING);
				return new ResponseEntity<PiazzaResponse>(new ErrorResponse(String.format("Data not found: %s", dataId), LOADER),
						HttpStatus.NOT_FOUND);
			}

			// Delete the Data if hosted
			ingestUtil.deleteDataResourceFiles(data);

			// Remove the Data from the database
			accessor.deleteDataEntry(dataId);

			// Request that Access delete any Deployments for this Data ID
			deleteDeploymentsByDataId(dataId);

			// Delete from Elasticsearch
			String searchUrl = String.format("%s/%s?dataId=%s", SEARCH_URL, SEARCH_DELETE_SUFFIX, dataId);
			ingestUtil.deleteElasticsearchByDataResource(data, searchUrl);

			// Log the deletion
			logger.log(String.format("Successfully Deleted Data Id %s", dataId), Severity.INFORMATIONAL,
					new AuditElement(INGEST, "deletedData", dataId));
			// Return
			return new ResponseEntity<PiazzaResponse>(new SuccessResponse("Data " + dataId + " was deleted successfully", LOADER),
					HttpStatus.OK);
		} catch (Exception exception) {
			String error = String.format("Error deleting Data %s: %s", dataId, exception.getMessage());
			LOG.error(error, exception);
			logger.log(error, Severity.ERROR, new AuditElement(INGEST, "errorDeletingData", dataId));
			return new ResponseEntity<PiazzaResponse>(new ErrorResponse("Error deleting Data: " + exception.getMessage(), LOADER),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	private void deleteDeploymentsByDataId(final String dataId) {
		try {
			String url = String.format("%s/deployment?dataId=%s", ACCESS_URL, dataId);
			restTemplate.delete(url);
		} catch (HttpClientErrorException | HttpServerErrorException httpException) {
			// Log the error; but do not fail the request entirely if this deletion fails.
			String error = String.format("Error requesting deletion of Deployments while deleting Data ID %s: %s", dataId,
					httpException.getResponseBodyAsString());
			logger.log(error, Severity.WARNING);
			LOG.warn(error, httpException);
		}
	}
	
	/**
	 * Update the metadata of a Data Resource
	 * 
	 * @param dataId
	 *            The Id of the resource
	 * @param user
	 *            the user submitting the request
	 * @return OK if successful; error if not.
	 */
	@RequestMapping(value = "/data/{dataId}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<PiazzaResponse> updateMetadata(@PathVariable(value = "dataId") String dataId,
			@RequestBody ResourceMetadata metadata) {
		try {
			logger.log(String.format("Updating Data Metadata for Data %s", dataId), Severity.INFORMATIONAL);
			// Query for the Data Id
			DataResource data = accessor.getData(dataId);
			if (data == null) {
				logger.log(String.format("Data not found for requested Id %s", dataId), Severity.WARNING,
						new AuditElement(INGEST, "noDataFoundForId", dataId));
				return new ResponseEntity<PiazzaResponse>(new ErrorResponse(String.format("Data not found: %s", dataId), LOADER),
						HttpStatus.NOT_FOUND);
			}

			// Update the Metadata
			accessor.updateMetadata(dataId, metadata);
			// Return OK
			return new ResponseEntity<PiazzaResponse>(new SuccessResponse("Metadata " + dataId + " was successfully updated.", LOADER),
					HttpStatus.OK);
		} catch (Exception exception) {
			logger.log(exception.getMessage(), Severity.ERROR, new AuditElement(INGEST, "updateMetadataFailure", dataId));
			LOG.error(exception.getMessage(), exception);
			return new ResponseEntity<PiazzaResponse>(new ErrorResponse(exception.getMessage(), LOADER), HttpStatus.BAD_REQUEST);
		}
	}

	/**
	 * Returns administrative statistics for this component.
	 * 
	 * @return Component information
	 */
	@RequestMapping(value = "/admin/stats", method = RequestMethod.GET)
	public ResponseEntity<Map<String, Object>> getAdminStats() {
		Map<String, Object> stats = new HashMap<String, Object>();
		// Return information on the jobs currently being processed
		stats.put("jobs", threadManager.getRunningJobIds());
		stats.put("activeThreads", threadPoolTaskExecutor.getActiveCount());
		if (threadPoolTaskExecutor.getThreadPoolExecutor() != null) {
			stats.put("threadQueue", threadPoolTaskExecutor.getThreadPoolExecutor().getQueue().size());
		}
		return new ResponseEntity<Map<String, Object>>(stats, HttpStatus.OK);
	}

	/**
	 * Healthcheck required for all Piazza Core Services
	 * 
	 * @return String
	 */
	@RequestMapping(value = "/", method = RequestMethod.GET)
	public String getHealthCheck() {
		return "Hello, Health Check here for Loader.";
	}
}
