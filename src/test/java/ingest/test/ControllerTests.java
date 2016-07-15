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
import static org.mockito.Matchers.eq;
import ingest.controller.IngestController;
import ingest.messaging.IngestThreadManager;
import ingest.persist.PersistMetadata;
import ingest.utility.IngestUtilities;

import java.util.Map;

import model.data.DataResource;
import model.job.metadata.ResourceMetadata;
import model.response.ErrorResponse;
import model.response.PiazzaResponse;
import model.response.SuccessResponse;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import util.PiazzaLogger;

/**
 * Tests the Ingest Rest Controller
 * 
 * @author Patrick.Doody
 *
 */
public class ControllerTests {
	@Mock
	private IngestThreadManager threadManager;
	@Mock
	private PiazzaLogger logger;
	@Mock
	private PersistMetadata persistence;
	@Mock
	private IngestUtilities ingestUtil;

	@InjectMocks
	private IngestController ingestController;

	/**
	 * Initialized
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
	}

	/**
	 * Test DELETE /data/{dataId}
	 */
	@Test
	public void testDelete() {
		// Test empty Data
		ResponseEntity<PiazzaResponse> response = ingestController.deleteData("");
		assertTrue(response.getBody() instanceof ErrorResponse);
		assertTrue(((ErrorResponse) response.getBody()).message.contains("No Data ID"));

		// Test no Data Resource for the ID
		Mockito.when(persistence.getData(eq("123456"))).thenReturn(null);
		response = ingestController.deleteData("123456");
		assertTrue(response.getBody() instanceof ErrorResponse);
		assertTrue(((ErrorResponse) response.getBody()).message.contains("Data not found"));

		// Successful deletion
		Mockito.when(persistence.getData(eq("123456"))).thenReturn(new DataResource());
		// Mockito.doNothing().when(ingestUtil.deleteDataResourceFiles(any(DataResource.class)));
		// Mockito.doNothing().when(persistence.deleteDataEntry(eq("123456")));
		response = ingestController.deleteData("123456");
		assertTrue(response.getStatusCode().compareTo(HttpStatus.OK) == 0);
	}

	/**
	 * Test POST /data/{dataId}
	 */
	@Test
	public void testUpdate() throws Exception {
		// Test
		Mockito.doNothing().when(persistence).updateMetadata(eq("123456"), any(ResourceMetadata.class));
		Mockito.when(persistence.getData(eq("123456"))).thenReturn(new DataResource());
		ResponseEntity<PiazzaResponse> response = ingestController.updateMetadata("123456", new ResourceMetadata());
		assertTrue(response.getBody() instanceof SuccessResponse);

		// Test Exception
		Mockito.doThrow(new Exception("Error")).when(persistence).updateMetadata(eq("123456"), any(ResourceMetadata.class));
		response = ingestController.updateMetadata("123456", new ResourceMetadata());
		assertTrue(response.getBody() instanceof ErrorResponse);
	}

	/**
	 * Test GET /admin/stats
	 */
	@Test
	public void testAdminStats() {
		// Test
		ResponseEntity<Map<String, Object>> entity = ingestController.getAdminStats();
		Map<String, Object> map = entity.getBody();

		// Verify
		assertTrue(entity.getStatusCode().equals(HttpStatus.OK));
		assertTrue(map.keySet().contains("jobs"));
	}

	/**
	 * Test GET /
	 */
	@Test
	public void testHealthCheck() {
		// Test
		String response = ingestController.getHealthCheck();
		// Verify
		assertTrue(response.contains("Health"));
	}
}
