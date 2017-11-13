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

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureSource;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.AmazonClientException;

import exception.DataInspectException;
import exception.InvalidInputException;
import ingest.utility.IngestUtilities;
import model.data.DataResource;
import model.data.type.PostGISDataType;
import model.data.type.WfsDataType;
import model.job.metadata.SpatialMetadata;
import model.logger.AuditElement;
import model.logger.Severity;
import util.GeoToolsUtil;
import util.PiazzaLogger;

/**
 * Inspects a remote WFS URL and parses out the relevant information for it.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class WfsInspector implements InspectorType {
	@Autowired
	private IngestUtilities ingestUtilities;
	@Autowired
	private PiazzaLogger logger;
	private static final String CAPABILITIES_TEMPLATE = "%s?SERVICE=wfs&REQUEST=GetCapabilities&VERSION=%s";
	@Value("${vcap.services.pz-postgres.credentials.db_host}")
	private String POSTGRES_HOST;
	@Value("${vcap.services.pz-postgres.credentials.db_port}")
	private String POSTGRES_PORT;
	@Value("${vcap.services.pz-postgres.credentials.db_name}")
	private String POSTGRES_DB_NAME;
	@Value("${vcap.services.pz-postgres.credentials.username}")
	private String POSTGRES_USER;
	@Value("${vcap.services.pz-postgres.credentials.password}")
	private String POSTGRES_PASSWORD;
	@Value("${postgres.schema}")
	private String POSTGRES_SCHEMA;

	private static final Logger LOG = LoggerFactory.getLogger(WfsInspector.class);

	@Override
	public DataResource inspect(DataResource dataResource, boolean host)
			throws DataInspectException, AmazonClientException, InvalidInputException, IOException, FactoryException {

		// Connect to the WFS and grab a reference to the Feature Source for the
		// specified Feature Type
		FeatureSource<SimpleFeatureType, SimpleFeature> wfsFeatureSource = getWfsFeatureSource(dataResource);

		// Get the Bounding Box, populate the Spatial Metadata
		SpatialMetadata spatialMetadata = new SpatialMetadata();
		ReferencedEnvelope envelope = wfsFeatureSource.getBounds();
		spatialMetadata.setMinX(envelope.getMinX());
		spatialMetadata.setMinY(envelope.getMinY());
		spatialMetadata.setMaxX(envelope.getMaxX());
		spatialMetadata.setMaxY(envelope.getMaxY());
		spatialMetadata.setNumFeatures(wfsFeatureSource.getFeatures().size());

		// Get the SRS and EPSG codes
		spatialMetadata.setCoordinateReferenceSystem(wfsFeatureSource.getInfo().getCRS().toString());
		spatialMetadata.setEpsgCode(CRS.lookupEpsgCode(wfsFeatureSource.getInfo().getCRS(), true));

		// Set the Spatial Metadata
		dataResource.spatialMetadata = spatialMetadata;

		// Populate the projected EPSG:4326 spatial metadata
		try {
			dataResource.spatialMetadata.setProjectedSpatialMetadata(ingestUtilities.getProjectedSpatialMetadata(spatialMetadata));
		} catch (Exception exception) {
			String error = String.format("Could not project the spatial metadata for Data %s because of exception: %s",
					dataResource.getDataId(), exception.getMessage());
			LOG.error(error, exception);
			if (logger != null) {
				logger.log(error, Severity.WARNING);
			}
		}

		// If this Data Source is to be hosted within the Piazza PostGIS, then
		// copy that data as a new table in the database.
		if (host) {
			copyWfsToPostGis(dataResource, wfsFeatureSource);
		}

		// Clean up Resources
		wfsFeatureSource.getDataStore().dispose();

		// Return the Populated Metadata
		return dataResource;
	}

	/**
	 * Copies the WFS Resource into a new Piazza PostGIS table.
	 * 
	 * @param dataResource
	 *            The WFS Data Resource to copy.
	 * @param featureSource
	 *            GeoTools Feature source for WFS.
	 * @throws IOException
	 */
	private void copyWfsToPostGis(DataResource dataResource, FeatureSource<SimpleFeatureType, SimpleFeature> wfsFeatureSource)
			throws IOException {
		// Create a Connection to the Piazza PostGIS Database for writing.
		DataStore postGisStore = GeoToolsUtil.getPostGisDataStore(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_SCHEMA, POSTGRES_DB_NAME,
				POSTGRES_USER, POSTGRES_PASSWORD);

		// Create the Schema in the Data Store
		String tableName = dataResource.getDataId();
		SimpleFeatureType wfsSchema = wfsFeatureSource.getSchema();
		SimpleFeatureType postGisSchema = GeoToolsUtil.cloneFeatureType(wfsSchema, tableName);
		postGisStore.createSchema(postGisSchema);
		SimpleFeatureStore postGisFeatureStore = (SimpleFeatureStore) postGisStore.getFeatureSource(tableName);

		logger.log(String.format("Copying Data %s to PostGIS Table %s", dataResource.getDataId(), tableName), Severity.INFORMATIONAL,
				new AuditElement("ingest", "copyWfsToPostGisTable", tableName));

		
		try (Transaction transaction = new DefaultTransaction()) {
					
			// Get the Features from the WFS and add them to the PostGIS store
			SimpleFeatureCollection wfsFeatures = (SimpleFeatureCollection) wfsFeatureSource.getFeatures();
			if (wfsFeatures.size() == 0) {
				transaction.close();
				throw new IOException("No features could be collected from the WFS. Nothing to store.");
			}
			postGisFeatureStore.addFeatures(wfsFeatures);
			
			// Commit the changes
			transaction.commit();
		} catch (Exception exception) {
			LOG.error("Error during WFS to PostGIS transaction, had to roll back changes.", exception);
			
			// Rethrow
			throw new IOException(exception.getMessage());
		} finally {
			
			// Clean up the PostGIS Store
			postGisStore.dispose();
		}

		// Update the Metadata of the DataResource to the new PostGIS table, and
		// treat as a PostGIS Resource type from now on.
		PostGISDataType postGisData = new PostGISDataType();
		postGisData.database = POSTGRES_DB_NAME;
		postGisData.table = tableName;
		dataResource.dataType = postGisData;
	}

	/**
	 * Gets the GeoTools Feature Source for the provided WFS Resource
	 * 
	 * @return GeoTools Feature Source that can be queried for spatial features and metadata
	 */
	public FeatureSource<SimpleFeatureType, SimpleFeature> getWfsFeatureSource(DataResource dataResource) throws IOException {
		// Form the Get Capabilities URL
		WfsDataType wfsResource = (WfsDataType) dataResource.getDataType();
		String capabilitiesUrl = String.format(CAPABILITIES_TEMPLATE, wfsResource.getUrl(), wfsResource.getVersion());
		Map<String, String> connectionParameters = new HashMap<String, String>();
		connectionParameters.put("WFSDataStoreFactory:GET_CAPABILITIES_URL", capabilitiesUrl);

		// Connect to the Data and Feature Source
		DataStore dataStore = DataStoreFinder.getDataStore(connectionParameters);
		FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = dataStore.getFeatureSource(wfsResource.getFeatureType());
		return featureSource;
	}
}
