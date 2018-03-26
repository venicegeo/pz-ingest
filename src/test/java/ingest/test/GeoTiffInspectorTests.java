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

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.test.util.ReflectionTestUtils;

import ingest.inspect.GeoTiffInspector;
import ingest.utility.IngestUtilities;
import model.data.DataResource;
import model.data.location.FolderShare;
import model.data.type.RasterDataType;
import model.job.metadata.ResourceMetadata;
import util.PiazzaLogger;

/**
 * Tests the GeoTIFF inspector
 * 
 * @author Patrick.Doody
 *
 */
public class GeoTiffInspectorTests {
	@Mock
	private PiazzaLogger logger;
	@Spy
	private IngestUtilities ingestUtilities;
	@InjectMocks
	private GeoTiffInspector inspector;

	private DataResource mockData;

	/**
	 * Setup the tests
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		// Sample Data Resource to inspect
		mockData = new DataResource();
		mockData.dataId = "123456";
		RasterDataType rasterType = new RasterDataType();
		FolderShare location = new FolderShare();
		location.filePath = "src" + File.separator + "test" + File.separator + "resources" + File.separator + "elevation.tif";
		rasterType.location = location;
		mockData.dataType = rasterType;
		mockData.metadata = new ResourceMetadata();
		mockData.metadata.name = "Raster";
	}

	/**
	 * Tests the GeoTIFF Inspector.
	 * <p>
	 * TODO: This test is currently ignored because the SL61 Jenkins build machine is failing when creating the
	 * Geotools-EPSG-HSQL database during Unit Test time. The GeoTools library cannot create the proper database, thus
	 * the library fails to find lookup codes, thus this unit test fails because the data can't be parsed properly. When
	 * the SL61 issue is resolved, this Unit test MUST be re-included into the suite.
	 * </p>
	 */
	@Test
	// @Ignore
	public void testInspector() throws Exception {
		// Mock
		ReflectionTestUtils.setField(inspector, "DATA_TEMP_PATH", "tmp");

		// Test
		DataResource data = inspector.inspect(mockData, true);

		// Verify
		assertTrue(data != null);
		assertTrue(data.getSpatialMetadata() != null);
		assertTrue(data.getMetadata().name.equals("Raster"));
		assertTrue(data.getSpatialMetadata().getMinX().equals(496147.97));
		assertTrue(data.getSpatialMetadata().getMinY().equals(5422119.88));
		assertTrue(data.getSpatialMetadata().getMaxX().equals(496545.97));
		assertTrue(data.getSpatialMetadata().getMaxY().equals(5422343.88));
		assertTrue(data.getSpatialMetadata().getEpsgCode().equals(32632));

		// Verify resources have cleaned up
		File file = new File("tmp" + File.separator + "123456.tif");
		assertTrue(file.exists() == false);
	}
}
