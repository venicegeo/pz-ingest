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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.geotools.data.FeatureSource;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.operation.TransformException;
import org.springframework.test.util.ReflectionTestUtils;

import com.amazonaws.services.s3.AmazonS3;

import ingest.utility.IngestUtilities;
import model.data.DataResource;
import model.data.location.FolderShare;
import model.data.type.RasterDataType;
import model.job.metadata.SpatialMetadata;
import util.PiazzaLogger;

public class IngestUtilitiesTests {
	@Mock
	private PiazzaLogger logger;
	@InjectMocks
	private IngestUtilities utilities;

	/**
	 * Setup the tests
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		new File("mockZipDirectory").delete();
		new File("mockDirectory").delete();
	}

	/**
	 * Test directory deletion (cleaning up ingested temp files)
	 */
	@Test
	public void testDeleteRecursive() throws Exception {
		// Mock a directory containing a directory and some files
		File mockDir = new File("mockDirectory");
		assertTrue(mockDir.mkdir());
		File mockInnerDir = new File(mockDir.getAbsolutePath() + File.separator + "mockInnerDirectory");
		assertTrue(mockInnerDir.mkdir());
		File mockFile = new File(mockInnerDir.getAbsolutePath() + File.separator + "mockFile");
		assertTrue(mockFile.createNewFile());
		assertTrue(mockFile.exists());
		// Test
		boolean success = utilities.deleteDirectoryRecursive(mockDir);
		// Verify
		assertTrue(success);
		assertFalse(mockDir.exists());
		assertFalse(mockInnerDir.exists());
		assertFalse(mockFile.exists());
	}

	/**
	 * Tests ZIP extraction
	 */
	@Test
	public void testExtractZip() throws Exception {
		// Test
		String zipFilePath = "src" + File.separator + "test" + File.separator + "resources" + File.separator + "TestShape.zip";
		String extractPath = "mockZipDirectory";
		utilities.extractZip(zipFilePath, extractPath);
		// No Errors Thrown
		boolean success = utilities.deleteDirectoryRecursive(new File(extractPath));
		assertTrue(success);
	}

	/**
	 * Test GeoTools Shapefile Data Store parsing. Test is failing due to a GeoTools interaction with local file names.
	 * TODO: Investigate.
	 */
	@Test
	@Ignore
	public void testShapefileDataStore() throws Exception {
		// Mock
		String zipFilePath = "src" + File.separator + "test" + File.separator + "resources" + File.separator + "TestShape.zip";
		// Test
		FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = utilities.getShapefileDataStore(zipFilePath);
		// Verify
		assertNotNull(featureSource);
	}

	/**
	 * Test Shapefile name extraction
	 */
	@Test
	public void testShapefileName() throws Exception {
		// Mock
		String zipFilePath = "src" + File.separator + "test" + File.separator + "resources" + File.separator + "TestShape.zip";
		String extractPath = "mockZipDirectory";
		utilities.extractZip(zipFilePath, extractPath);
		// Test
		String shapeFileName = utilities.findShapeFileName(extractPath);
		// Verify
		assertEquals("ShapefileData.shp", shapeFileName);
		// Cleanup
		boolean success = utilities.deleteDirectoryRecursive(new File(extractPath));
		assertTrue(success);
	}

	/**
	 * Test file size
	 */
	@Test
	public void testFileSize() throws Exception {
		// Mock
		DataResource mockData = new DataResource();
		RasterDataType rasterType = new RasterDataType();
		FolderShare location = new FolderShare();
		location.filePath = "src" + File.separator + "test" + File.separator + "resources" + File.separator + "elevation.tif";
		rasterType.location = location;
		mockData.dataType = rasterType;

		// Test
		long fileSize = utilities.getFileSize(mockData);

		// Verify
		assertTrue(fileSize == 90074);
	}

	/**
	 * Test factory method to generate AWS Client
	 */
	@Test
	public void testAwsClient() {
		// Test default client
		ReflectionTestUtils.setField(utilities, "AMAZONS3_ACCESS_KEY", "");
		ReflectionTestUtils.setField(utilities, "AMAZONS3_PRIVATE_KEY", "");
		AmazonS3 client = utilities.getAwsClient(false);
		assertTrue(client != null);

		// Test client with creds
		ReflectionTestUtils.setField(utilities, "AMAZONS3_ACCESS_KEY", "access");
		ReflectionTestUtils.setField(utilities, "AMAZONS3_PRIVATE_KEY", "private");
		ReflectionTestUtils.setField(utilities, "S3_KMS_CMK_ID", "shh");
		client = utilities.getAwsClient(true);
	}

	/**
	 * Test Logic for file cleanup
	 * <p>
	 * TODO: This test is currently ignored because the SL61 Jenkins build machine is failing when creating the
	 * Geotools-EPSG-HSQL database during Unit Test time. The GeoTools library cannot create the proper database, thus
	 * the library fails to find lookup codes, thus this unit test fails because the data can't be parsed properly. When
	 * the SL61 issue is resolved, this Unit test MUST be re-included into the suite.
	 * </p>
	 */
	@Test
	public void testProjectionMetadata() throws NoSuchAuthorityCodeException, FactoryException, TransformException {
		// Mock
		SpatialMetadata mockMetadata = new SpatialMetadata();
		mockMetadata.setEpsgCode(3857);
		mockMetadata.setMaxX(111000.0);
		mockMetadata.setMinX(0.0);
		mockMetadata.setMinY(0.0);
		mockMetadata.setMaxY(111000.0);

		// Test
		SpatialMetadata projected = utilities.getProjectedSpatialMetadata(mockMetadata);

		// Verify
		assertTrue(projected.getEpsgCode().equals(4326));
		assertEquals(projected.getMinX().doubleValue(), 0.0, 0.001);
		assertEquals(projected.getMaxX().doubleValue(), 0.99707963, 0.001);
		assertEquals(projected.getMinY().doubleValue(), 0.0, 0.001);
		assertEquals(projected.getMaxY().doubleValue(), 0.99712996, 0.001);
	}
}
