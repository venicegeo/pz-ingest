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
import static org.mockito.Matchers.isA;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.geotools.data.DataUtilities;
import org.geotools.data.memory.MemoryDataStore;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.CRS;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import ingest.inspect.WfsInspector;
import ingest.utility.IngestUtilities;
import model.data.DataResource;
import model.data.type.WfsDataType;
import util.PiazzaLogger;

/**
 * Tests internal Ingest components such as data type Inspectors
 * 
 * @author Patrick.Doody
 * 
 */
public class WfsInspectTests {
	@Mock
	private PiazzaLogger logger;
	@Mock
	private IngestUtilities ingestUtilities;
	private static final String MOCK_FEATURE_NAME = "Test";
	private MemoryDataStore mockDataStore;
	@Spy
	private WfsInspector wfsInspector;

	/**
	 * <p>
	 * TODO: This test is currently ignored because the SL61 Jenkins build machine is failing when creating the
	 * Geotools-EPSG-HSQL database during Unit Test time. The GeoTools library cannot create the proper database, thus
	 * the library fails to find lookup codes, thus this unit test fails because the data can't be parsed properly. When
	 * the SL61 issue is resolved, this Unit test MUST be re-included into the suite.
	 * </p>
	 */
	@Before
	//@Ignore
	public void init() throws Exception {
		MockitoAnnotations.initMocks(this);

		// Creating a Mock in-memory Data Store
		mockDataStore = new MemoryDataStore();
		SimpleFeatureType featureType = DataUtilities.createType(MOCK_FEATURE_NAME, "the_geom:Point:srid=4326");
		SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		// Create some sample Test Points
		List<SimpleFeature> features = new ArrayList<SimpleFeature>();
		Point point = geometryFactory.createPoint(new Coordinate(5, 5));
		featureBuilder.add(point);
		SimpleFeature feature = featureBuilder.buildFeature(null);
		features.add(feature);
		Point otherPoint = geometryFactory.createPoint(new Coordinate(0, 0));
		featureBuilder.add(otherPoint);
		SimpleFeature otherFeature = featureBuilder.buildFeature(null);
		features.add(otherFeature);
		mockDataStore.addFeatures(features);
	}

	/**
	 * Tests the WFS Inspector
	 * <p>
	 * TODO: This test is currently ignored because the SL61 Jenkins build machine is failing when creating the
	 * Geotools-EPSG-HSQL database during Unit Test time. The GeoTools library cannot create the proper database, thus
	 * the library fails to find lookup codes, thus this unit test fails because the data can't be parsed properly. When
	 * the SL61 issue is resolved, this Unit test MUST be re-included into the suite.
	 * </p>
	 */
	@Test
	//@Ignore
	public void testWfsInspector() throws Exception {
		// Mock a WFS DataResource
		WfsDataType wfsResource = new WfsDataType();
		DataResource mockResource = new DataResource();
		mockResource.dataId = UUID.randomUUID().toString();
		mockResource.dataType = wfsResource;
		ContentFeatureSource featureSource = mockDataStore.getFeatureSource(MOCK_FEATURE_NAME);

		Mockito.doReturn(featureSource).when(wfsInspector).getWfsFeatureSource(isA(DataResource.class));

		// Inspect our sample resource
		DataResource inspectedResource = wfsInspector.inspect(mockResource, false);

		// Verify that the Spatial Metadata has been appropriately populated
		assertTrue(inspectedResource.getSpatialMetadata().getMinX().equals(featureSource.getBounds().getMinX()));
		assertTrue(inspectedResource.getSpatialMetadata().getMinY().equals(featureSource.getBounds().getMinY()));
		assertTrue(inspectedResource.getSpatialMetadata().getMaxX().equals(featureSource.getBounds().getMaxX()));
		assertTrue(inspectedResource.getSpatialMetadata().getMaxY().equals(featureSource.getBounds().getMaxY()));
		assertTrue(inspectedResource.getSpatialMetadata().getEpsgCode().equals(CRS.lookupEpsgCode(featureSource.getInfo().getCRS(), true)));
	}
}
