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

import ingest.inspect.GeoJsonInspector;
import ingest.inspect.GeoTiffInspector;
import ingest.inspect.Inspector;
import ingest.inspect.PointCloudInspector;
import ingest.inspect.ShapefileInspector;
import ingest.inspect.TextInspector;
import ingest.inspect.WfsInspector;
import ingest.persist.DatabaseAccessor;
import model.data.DataResource;
import model.data.type.TextDataType;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Test the generic Inspector parent class
 * 
 * @author Patrick.Doody
 *
 */
public class InspectorTests {
	@Mock
	private ShapefileInspector shapefileInspector;
	@Mock
	private TextInspector textInspector;
	@Mock
	private WfsInspector wfsInspector;
	@Mock
	private GeoTiffInspector geotiffInspector;
	@Mock
	private PointCloudInspector pointCloudInspector;
	@Mock
	private GeoJsonInspector geoJsonInspector;
	@InjectMocks
	private Inspector inspector;

	private DataResource mockData;

	/**
	 * Test setup
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		// Mock some Data
		mockData = new DataResource();
		mockData.dataType = new TextDataType();
	}

	/**
	 * Test Inspector to generate the correct class
	 */
	@Test
	public void testInspector() throws Exception {
		// Test
		inspector.inspect(mockData, true);
	}
}
