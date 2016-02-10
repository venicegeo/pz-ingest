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
package ingest.inspect;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import model.data.DataResource;
import model.data.type.ShapefileResource;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Inspects a Shapefile, populating any essential metadata from the file itself.
 * At the very least, the spatial metadata such as CRS/EPSG must be found in
 * order for proper Access to occur at a later time. Bounds are also pulled if
 * able.
 * 
 * @author Patrick.Doody
 * 
 */
public class ShapefileInspector implements InspectorType {

	@Override
	public DataResource inspect(DataResource dataResource) throws Exception {
		// Get the Store information from GeoTools for accessing the Shapefile
		FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = getShapefileDataStore(dataResource);

		// Get the Bounding Box, set the Metadata
		ReferencedEnvelope envelope = featureSource.getBounds();
		dataResource.getSpatialMetadata().setMinX(envelope.getMinX());
		dataResource.getSpatialMetadata().setMinY(envelope.getMinY());
		dataResource.getSpatialMetadata().setMaxX(envelope.getMaxX());
		dataResource.getSpatialMetadata().setMaxY(envelope.getMaxY());

		// Get the SRS and EPSG codes
		dataResource.getSpatialMetadata().setCoordinateReferenceSystem(featureSource.getInfo().getCRS().toString());
		dataResource.getSpatialMetadata().setEpsgCode(CRS.lookupEpsgCode(featureSource.getInfo().getCRS(), true));

		// Return the Metadata
		return dataResource;
	}

	/**
	 * Gets the GeoTools Feature Store for the Shapefile.
	 * 
	 * @param dataResource
	 *            The DataResource
	 * @return The GeoTools Shapefile Data Store Feature Source
	 */
	private FeatureSource<SimpleFeatureType, SimpleFeature> getShapefileDataStore(DataResource dataResource)
			throws IOException {
		ShapefileResource shapefileResource = (ShapefileResource) dataResource.getDataType();
		File shapeFile = shapefileResource.getLocation().getFile();
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("url", shapeFile.toURI().toURL());
		DataStore dataStore = DataStoreFinder.getDataStore(map);
		String typeName = dataStore.getTypeNames()[0];
		FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = dataStore.getFeatureSource(typeName);
		return featureSource;
	}
}
