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
import ingest.event.IngestEvent;
import model.data.DataResource;
import model.data.type.ShapefileDataType;
import model.job.Job;
import model.job.metadata.SpatialMetadata;
import model.job.type.IngestJob;

import org.joda.time.DateTime;
import org.junit.Test;

/**
 * Tests the event package
 * 
 * @author Patrick.Doody
 *
 */
public class EventTests {
	/**
	 * Test creation of an event
	 */
	@Test
	public void testEventCreation() {
		// Mock
		Job mockJob = new Job();
		mockJob.jobType = new IngestJob();
		((IngestJob) mockJob.jobType).host = true;
		mockJob.submitted = new DateTime();
		DataResource mockData = new DataResource();
		mockData.dataId = "123456";
		mockData.dataType = new ShapefileDataType();
		mockData.spatialMetadata = new SpatialMetadata();
		mockData.spatialMetadata.setEpsgCode(4326);
		mockData.spatialMetadata.setMinX(0.0);
		mockData.spatialMetadata.setMinY(1.0);
		mockData.spatialMetadata.setMaxX(2.0);
		mockData.spatialMetadata.setMaxY(3.0);

		// Test
		IngestEvent testEvent = new IngestEvent("W2", mockJob, mockData);

		// Verify
		assertTrue(testEvent.data.get("epsg").equals(4326));
		assertTrue(testEvent.data.get("dataType").equals(mockData.getDataType().getClass().getSimpleName()));
		assertTrue(testEvent.data.get("minX").equals(0.0));
		assertTrue(testEvent.data.get("hosted").equals(true));
	}
}
