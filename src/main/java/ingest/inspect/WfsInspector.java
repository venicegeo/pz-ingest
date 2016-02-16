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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Inspects a remote WFS URL and parses out the relevant information for it.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class WfsInspector implements InspectorType {
	private static final String CAPABILITIES_TEMPLATE = "%s?SERVICE=wfs&REQUEST=GetCapabilities&VERSION=%s";
	private static final String POSTGRES_DATASTORE_TYPE = "postgres";
	@Value("${postgres.host}")
	private String POSTGRES_HOST;
	@Value("${postgres.port}")
	private String POSTGRES_PORT;
	@Value("${postgres.db.name}")
	private String POSTGRES_DB_NAME;
	@Value("${postgres.user}")
	private String POSTGRES_USER;
	@Value("${postgres.password}")
	private String POSTGRES_PASSWORD;
	@Value("${postgres.schema}")
	private String POSTGRES_SCHEMA;

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

		// If this Data Source is to be hosted within the Piazza PostGIS, then
		// copy that data as a new table in the database.
		if (host) {
			copyWfsToPostGis(dataResource);
		}

		// Return the Populated Metadata
		return dataResource;
	}

	/**
	 * Copies the WFS Resource into a new Piazza PostGIS table.
	 * 
	 * @param dataResource
	 *            The WFS Data Resource to copy.
	 */
	private void copyWfsToPostGis(DataResource dataResource) throws Exception {
		// Create a Connection to the Piazza PostGIS Database for writing.
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("dbtype", POSTGRES_DATASTORE_TYPE);
		params.put("host", POSTGRES_HOST);
		params.put("port", POSTGRES_PORT);
		params.put("schema", POSTGRES_SCHEMA);
		params.put("database", POSTGRES_DB_NAME);
		params.put("user", POSTGRES_USER);
		params.put("passwd", POSTGRES_PASSWORD);
		DataStore postGisStore = DataStoreFinder.getDataStore(params);

		// Create the Schema in the Data Store

		// Commit the Features to the Data Store

		// Update the Metadata of the DataResource to the new PostGIS table, and
		// treat as a PostGIS Resource type.
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
