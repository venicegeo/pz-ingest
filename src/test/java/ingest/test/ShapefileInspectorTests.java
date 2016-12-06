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
import ingest.inspect.ShapefileInspector;
import ingest.utility.IngestUtilities;

import java.io.File;

import model.data.DataResource;
import model.data.location.FolderShare;
import model.data.type.ShapefileDataType;
import model.job.metadata.ResourceMetadata;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import util.PiazzaLogger;

/**
 * Tests Shapefile inspector
 * 
 * @author Patrick.Doody
 *
 */
public class ShapefileInspectorTests {
	@Mock
	private PiazzaLogger logger;
	@Mock
	private IngestUtilities ingestUtilities;
	@InjectMocks
	private ShapefileInspector inspector;

	private DataResource mockData = new DataResource();

	/**
	 * Initialize mock data
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		// Temporary file location
		ReflectionTestUtils.setField(inspector, "DATA_TEMP_PATH", "tmp");

		mockData.setDataId("123456");
		mockData.metadata = new ResourceMetadata();
		mockData.metadata.setName("Shape");
		ShapefileDataType dataType = new ShapefileDataType();
		FolderShare location = new FolderShare();
		location.filePath = "src" + File.separator + "test" + File.separator + "resources" + File.separator + "TestShape.zip";
		dataType.location = location;
		mockData.dataType = dataType;
	}

	/**
	 * Test shapefile inspection
	 * <p>
	 * TODO: This test is currently ignored because the SL61 Jenkins build machine is failing when creating the
	 * Geotools-EPSG-HSQL database during Unit Test time. The GeoTools library cannot create the proper database, thus
	 * the library fails to find lookup codes, thus this unit test fails because the data can't be parsed properly. When
	 * the SL61 issue is resolved, this Unit test MUST be re-included into the suite.
	 * </p>
	 */
	@Test
	@Ignore
	public void testInspector() throws Exception {
		// Mock - run certain real methods. The rest will be mocked.
		Mockito.doCallRealMethod().when(ingestUtilities).extractZip(anyString(), anyString());
		Mockito.doCallRealMethod().when(ingestUtilities).findShapeFileName(anyString());
		Mockito.doCallRealMethod().when(ingestUtilities).getShapefileDataStore(anyString());
		Mockito.doCallRealMethod().when(ingestUtilities).deleteDirectoryRecursive(any(File.class));

		// Test
		DataResource data = inspector.inspect(mockData, true);

		// Verify
		assertTrue(data != null);
		assertTrue(data.getSpatialMetadata() != null);
		assertTrue(data.getMetadata().getName().equals("Shape"));
		assertTrue(data.getSpatialMetadata().getMaxX().equals(106.00));
		assertTrue(data.getSpatialMetadata().getMaxY().equals(4.0));
		assertTrue(data.getSpatialMetadata().getMinX().equals(102.0));
		assertTrue(data.getSpatialMetadata().getMinY().equals(0.5));
		assertTrue(data.getSpatialMetadata().getEpsgCode().equals(4326));

		// Verify resources have cleaned up
		File directory = new File("tmp" + File.separator + "123456");
		assertTrue(directory.exists() == false);
		File tempZip = new File("tmp" + File.separator + "123456.zip");
		assertTrue(tempZip.exists() == false);
	}
}
