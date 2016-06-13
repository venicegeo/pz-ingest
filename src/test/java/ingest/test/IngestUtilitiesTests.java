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
import ingest.utility.IngestUtilities;

import java.io.File;

import model.data.DataResource;
import model.data.location.FolderShare;
import model.data.type.RasterDataType;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import util.PiazzaLogger;

import com.amazonaws.services.s3.AmazonS3;

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
		location.filePath = "src" + File.separator + "test" + File.separator + "resources" + File.separator
				+ "elevation.tif";
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
		AmazonS3 client = utilities.getAwsClient();
		assertTrue(client != null);

		// Test client with creds
		ReflectionTestUtils.setField(utilities, "AMAZONS3_ACCESS_KEY", "access");
		ReflectionTestUtils.setField(utilities, "AMAZONS3_PRIVATE_KEY", "private");
		client = utilities.getAwsClient();
	}
}
