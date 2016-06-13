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
import static org.mockito.Mockito.when;
import ingest.inspect.GeoJsonInspector;
import ingest.utility.IngestUtilities;

import java.io.File;

import model.data.DataResource;
import model.data.type.GeoJsonDataType;
import model.job.metadata.ResourceMetadata;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import util.PiazzaLogger;

/**
 * Tests the GeoJSON Inspector
 * 
 * @author Patrick.Doody
 *
 */
public class GeoJSONInspectorTests {
	@Mock
	private IngestUtilities ingestUtilities;
	@Mock
	private PiazzaLogger logger;

	@InjectMocks
	private GeoJsonInspector inspector;

	private DataResource mockData;

	/**
	 * Test initialization
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		// Sample some GeoJSON Data
		mockData = new DataResource();
		mockData.setDataId("123456");
		GeoJsonDataType mockDataType = new GeoJsonDataType();
		mockDataType.geoJsonContent = "{\"type\": \"FeatureCollection\",\"features\": [{\"type\": \"Feature\",\"geometry\": {\"type\": \"Point\",\"coordinates\": [102.0,0.5]},\"properties\": {\"prop0\": \"value0\"}},{\"type\": \"Feature\",\"geometry\": {\"type\": \"Point\",\"coordinates\": [106.0,4]},\"properties\": {\"prop0\": \"value0\"}}]}";
		mockData.dataType = mockDataType;
		mockData.metadata = new ResourceMetadata();
		mockData.metadata.setName("Test GeoJSON");
	}

	/**
	 * Test inspecting of a GeoJSON Data Resource
	 */
	@Test
	public void testInspect() throws Exception {
		// Mock
		when(ingestUtilities.getShapefileDataStore(anyString())).thenCallRealMethod();
		when(ingestUtilities.deleteDirectoryRecursive(any(File.class))).thenCallRealMethod();

		// Test
		DataResource data = inspector.inspect(mockData, true);

		// Verify
		assertTrue(data != null);
		assertTrue(data.getSpatialMetadata() != null);
		assertTrue(data.getSpatialMetadata().getMaxX().equals(106.00));
		assertTrue(data.getSpatialMetadata().getMaxY().equals(4.0));
		assertTrue(data.getSpatialMetadata().getMinX().equals(102.0));
		assertTrue(data.getSpatialMetadata().getMinY().equals(0.5));
		assertTrue(data.getSpatialMetadata().getEpsgCode().equals(4326));
	}
}
