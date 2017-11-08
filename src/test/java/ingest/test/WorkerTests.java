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
package ingest.test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.concurrent.Future;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import ingest.inspect.Inspector;
import ingest.messaging.IngestWorker;
import messaging.job.JobMessageFactory;
import model.data.DataResource;
import model.data.type.GeoJsonDataType;
import model.job.Job;
import model.job.metadata.SpatialMetadata;
import model.job.type.IngestJob;
import util.PiazzaLogger;
import util.UUIDFactory;

/**
 * Tests the Ingest worker, that handles triggering Inspectors
 * 
 * @author Patrick.Doody
 *
 */
public class WorkerTests {
	@Mock
	private PiazzaLogger logger;
	@Mock
	private Inspector inspector;
	@Mock
	private UUIDFactory uuidFactory;
	@Mock
	private RestTemplate restTemplate;
	@Mock
	private RabbitTemplate rabbitTemplate;
	@Mock
	private Queue updateJobsQueue;
	@Mock
	private ObjectMapper mapper;
	@InjectMocks
	private IngestWorker worker;

	private Job mockJob = new Job();
	private IngestJob mockIngest = new IngestJob();
	private DataResource mockData = new DataResource();

	/**
	 * Setup mocks
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		// Mock the Job and Data
		mockIngest.host = true;
		GeoJsonDataType mockDataType = new GeoJsonDataType();
		mockDataType.geoJsonContent = "{\"type\": \"FeatureCollection\",\"features\": [{\"type\": \"Feature\",\"geometry\": {\"type\": \"Point\",\"coordinates\": [102.0,0.5]},\"properties\": {\"prop0\": \"value0\"}},{\"type\": \"Feature\",\"geometry\": {\"type\": \"Point\",\"coordinates\": [106.0,4]},\"properties\": {\"prop0\": \"value0\"}}]}";
		mockData.dataType = mockDataType;
		mockData.spatialMetadata = new SpatialMetadata();
		mockIngest.data = mockData;
		mockJob.setJobId("123456");
		mockJob.setJobType(mockIngest);
		mockJob.setCreatedOnString(new DateTime().toString());
		mockJob.setCreatedBy("Test User");
	}

	/**
	 * Tests the Ingest Worker for processing a mock message
	 */
	@Test
	public void testWorker() throws Exception {
		// Ensure we get a GUID for the Data Resource
		when(uuidFactory.getUUID()).thenReturn("654321");
		when(updateJobsQueue.getName()).thenReturn("Update-Job-Unit-Test");
		Mockito.doNothing().when(rabbitTemplate).convertAndSend(eq(JobMessageFactory.PIAZZA_EXCHANGE_NAME), eq("654321"),
				Mockito.anyString());

		// Mock the REST response from Workflow and Metadata Ingest
		when(restTemplate.postForObject(anyString(), any(), eq(String.class))).thenReturn("OK");
		when(restTemplate.postForEntity(anyString(), any(), eq(Object.class))).thenReturn(new ResponseEntity<Object>(HttpStatus.OK));

		// Format a correct message and re-test
		Future<DataResource> workerFuture = worker.run(mockJob, null);

		// Verify
		assertTrue(workerFuture.get() != null);
		DataResource inspectedData = workerFuture.get();
		assertTrue(inspectedData.getMetadata() != null);
		assertTrue(inspectedData.getMetadata().createdBy.equals("Test User"));
		assertTrue(inspectedData.getSpatialMetadata() != null);
	}
}
