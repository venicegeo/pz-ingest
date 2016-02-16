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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import model.data.DataResource;
import model.data.type.WfsResource;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Inspects a remote WFS URL and parses out the relevant information for it.
 * 
 * @author Patrick.Doody
 * 
 */
public class WfsInspector implements InspectorType {
	private static final String CAPABILITIES_TEMPLATE = "%s?SERVICE=wfs&REQUEST=GetCapabilities&version=%s";

	@Override
	public DataResource inspect(DataResource dataResource, boolean host) throws Exception {
		// Connect to the WFS and grab a reference to the Feature Source for the
		// specified Feature Type
		FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = getWfsFeatureSource(dataResource);

		// Get the Bounding Box, set the Metadata
		ReferencedEnvelope envelope = featureSource.getBounds();
		dataResource.getSpatialMetadata().setMinX(envelope.getMinX());
		dataResource.getSpatialMetadata().setMinY(envelope.getMinY());
		dataResource.getSpatialMetadata().setMaxX(envelope.getMaxX());
		dataResource.getSpatialMetadata().setMaxY(envelope.getMaxY());

		// Get the SRS and EPSG codes
		dataResource.getSpatialMetadata().setCoordinateReferenceSystem(featureSource.getInfo().getCRS().toString());
		dataResource.getSpatialMetadata().setEpsgCode(CRS.lookupEpsgCode(featureSource.getInfo().getCRS(), true));

		// Return the Populated Metadata
		return dataResource;
	}

	/**
	 * Gets the GeoTools Feature Source for the provided WFS Resource
	 * 
	 * @return GeoTools Feature Source that can be queried for spatial features
	 *         and metadata
	 */
	private FeatureSource<SimpleFeatureType, SimpleFeature> getWfsFeatureSource(DataResource dataResource)
			throws IOException {
		// Form the Get Capabilities URL
		WfsResource wfsResource = (WfsResource) dataResource.getDataType();
		String capabilitiesUrl = String.format(CAPABILITIES_TEMPLATE, wfsResource.getUrl(), wfsResource.getVersion());
		Map<String, String> connectionParameters = new HashMap<String, String>();
		connectionParameters.put("WFSDataStoreFactory:GET_CAPABILITIES_URL", capabilitiesUrl);

		// Connect to the Data and Feature Source
		DataStore dataStore = DataStoreFinder.getDataStore(connectionParameters);
		FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = dataStore.getFeatureSource(wfsResource
				.getFeatureType());
		return featureSource;
	}

}
