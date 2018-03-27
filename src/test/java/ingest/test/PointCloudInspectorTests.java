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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import ingest.inspect.PointCloudInspector;
import ingest.model.PointCloudResponse;
import ingest.utility.IngestUtilities;
import model.data.DataResource;
import model.data.location.FolderShare;
import model.data.type.PointCloudDataType;
import model.job.metadata.ResourceMetadata;
import util.PiazzaLogger;

/**
 * Tests the Point Cloud inspector
 * 
 * @author Patrick.Doody
 *
 */
public class PointCloudInspectorTests {
	@Mock
	private PiazzaLogger logger;
	@Mock
	private RestTemplate restTemplate;
	@Mock
	private IngestUtilities ingestUtilities;
	@InjectMocks
	private PointCloudInspector inspector;

	private DataResource mockData;

	/**
	 * Setup mock data
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		Mockito.doCallRealMethod().when(ingestUtilities).getFileFactoryForDataResource(Mockito.any());

		// Mock PC Data
		mockData = new DataResource();
		mockData.setDataId("123456");
		PointCloudDataType dataType = new PointCloudDataType();
		FolderShare location = new FolderShare();
		location.filePath = "src" + File.separator + "test" + File.separator + "resources" + File.separator + "samp71-utm.laz";
		dataType.location = location;
		mockData.dataType = dataType;
		mockData.metadata = new ResourceMetadata();
		mockData.metadata.setName("PC");
	}

	/**
	 * Test ingestion of PC data
	 */
	@Test
	public void testInspection() throws Exception {
		// Mock the Point Cloud metadata call
		String mockResponse = "{\"response\": {\"metadata\": {\"maxx\": 1,\"maxy\": 2,\"maxz\": 3,\"minx\": 1,\"miny\": 2,\"minz\": 3,\"spatialreference\": \"4326\"}}}";
		when(restTemplate.postForObject(anyString(), any(), eq(String.class))).thenReturn(mockResponse);

		// Test
		DataResource data = inspector.inspect(mockData, true);

		// Verify
		assertTrue(data != null);
		assertTrue(data.getSpatialMetadata() != null);
		assertTrue(data.getMetadata().getName().equals("PC"));
		assertTrue(data.getSpatialMetadata().getMinX().equals(1.0));
		assertTrue(data.getSpatialMetadata().getMinY().equals(2.0));
		assertTrue(data.getSpatialMetadata().getMinZ().equals(3.0));
		assertTrue(data.getSpatialMetadata().getMaxX().equals(1.0));
		assertTrue(data.getSpatialMetadata().getMaxY().equals(2.0));
		assertTrue(data.getSpatialMetadata().getMaxZ().equals(3.0));
	}

	@Test
	public void testResponseSerialization() throws Exception {
		// Mock
		PointCloudResponse response = new PointCloudResponse();
		response.setSpatialreference("EPSG:4326");
		response.setMaxx(1.0);
		response.setMaxy(2.0);
		response.setMaxz(3.0);
		response.setMinx(4.0);
		response.setMiny(5.0);
		response.setMinz(6.0);
		// Test
		ObjectMapper mapper = new ObjectMapper();
		String responseString = mapper.writeValueAsString(response);
		PointCloudResponse deserializedResponse = mapper.readValue(responseString, PointCloudResponse.class);
		// Verify
		assertEquals(response.getMaxx(), deserializedResponse.getMaxx());
		assertEquals(response.getMaxy(), deserializedResponse.getMaxy());
		assertEquals(response.getMaxz(), deserializedResponse.getMaxz());
		assertEquals(response.getMinx(), deserializedResponse.getMinx());
		assertEquals(response.getMiny(), deserializedResponse.getMiny());
		assertEquals(response.getMinz(), deserializedResponse.getMinz());
		assertEquals(response.toString(), deserializedResponse.toString());
	}
}
