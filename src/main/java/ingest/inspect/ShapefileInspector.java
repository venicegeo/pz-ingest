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

import ingest.utility.IngestUtilities;

import java.io.File;
import java.io.InputStream;

import model.data.DataResource;
import model.data.location.FileAccessFactory;
import model.data.type.ShapefileDataType;
import model.job.metadata.SpatialMetadata;

import org.apache.commons.io.FileUtils;
import org.geotools.data.FeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import util.PiazzaLogger;

/**
 * Inspects a Shapefile, populating any essential metadata from the file itself.
 * At the very least, the spatial metadata such as CRS/EPSG must be found in
 * order for proper Access to occur at a later time. Bounds are also pulled if
 * able.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class ShapefileInspector implements InspectorType {
	@Value("${vcap.services.pz-geoserver-efs.credentials.postgres.hostname}")
	private String POSTGRES_HOST;
	@Value("${vcap.services.pz-geoserver-efs.credentials.postgres.port}")
	private String POSTGRES_PORT;
	@Value("${vcap.services.pz-geoserver-efs.credentials.postgres.database}")
	private String POSTGRES_DB_NAME;
	@Value("${vcap.services.pz-geoserver-efs.credentials.postgres.username}")
	private String POSTGRES_USER;
	@Value("${vcap.services.pz-geoserver-efs.credentials.postgres.password}")
	private String POSTGRES_PASSWORD;
	@Value("${postgres.schema}")
	private String POSTGRES_SCHEMA;
	@Value("${data.temp.path}")
	private String DATA_TEMP_PATH;
	@Value("${vcap.services.pz-blobstore.credentials.access_key_id}")
	private String AMAZONS3_ACCESS_KEY;
	@Value("${vcap.services.pz-blobstore.credentials.secret_access_key}")
	private String AMAZONS3_PRIVATE_KEY;
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private IngestUtilities ingestUtilities;

	@Override
	public DataResource inspect(DataResource dataResource, boolean host) throws Exception {
		// Get the Shapefile and write it to disk for temporary use.
		FileAccessFactory fileFactory = new FileAccessFactory(AMAZONS3_ACCESS_KEY, AMAZONS3_PRIVATE_KEY);
		InputStream shapefileStream = fileFactory.getFile(((ShapefileDataType) dataResource.getDataType())
				.getLocation());
		File shapefileZip = new File(String.format("%s%s%s.%s", DATA_TEMP_PATH, File.separator,
				dataResource.getDataId(), "zip"));
		FileUtils.copyInputStreamToFile(shapefileStream, shapefileZip);

		// Unzip the Shapefile into a temporary directory, which will allow us
		// to parse the Shapefile's sidecar files.
		String extractPath = DATA_TEMP_PATH + File.separator + dataResource.getDataId();

		// Log the file locations.
		logger.log(String.format("Inspecting shapefile. Copied Zip to temporary path %s. Inflating contents into %s.",
				shapefileZip.getAbsolutePath(), extractPath), PiazzaLogger.INFO);

		ingestUtilities.extractZip(shapefileZip.getAbsolutePath(), extractPath);
		// Get the path to the actual *.shp file
		String shapefilePath = String.format("%s%s%s", extractPath, File.separator,
				ingestUtilities.findShapeFileName(extractPath));

		// Get the Store information from GeoTools for accessing the Shapefile
		FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = ingestUtilities
				.getShapefileDataStore(shapefilePath);

		// Get the Bounding Box, set the Spatial Metadata
		SpatialMetadata spatialMetadata = new SpatialMetadata();
		ReferencedEnvelope envelope = featureSource.getBounds();
		spatialMetadata.setMinX(envelope.getMinX());
		spatialMetadata.setMinY(envelope.getMinY());
		spatialMetadata.setMaxX(envelope.getMaxX());
		spatialMetadata.setMaxY(envelope.getMaxY());
		spatialMetadata.setNumFeatures(featureSource.getFeatures().size());

		// Get the SRS and EPSG codes
		spatialMetadata.setCoordinateReferenceSystem(featureSource.getInfo().getCRS().toString());
		spatialMetadata.setEpsgCode(CRS.lookupEpsgCode(featureSource.getInfo().getCRS(), true));

		// Set the spatial metadata
		dataResource.spatialMetadata = spatialMetadata;
		
		// Populate the projected EPSG:4326 spatial metadata
		try {
			dataResource.spatialMetadata.setProjectedSpatialMetadata(ingestUtilities.getProjectedSpatialMetadata(spatialMetadata));
		} catch (Exception exception) {
			exception.printStackTrace();
			logger.log(String.format("Could not project the spatial metadata for Data %s because of exception: %s",
					dataResource.getDataId(), exception.getMessage()), PiazzaLogger.WARNING);
		}

		// Process and persist shapefile file into the Piazza PostGIS database.
		if (host) {
			((ShapefileDataType) dataResource.getDataType()).setDatabaseTableName(dataResource.getDataId());
			ingestUtilities.persistFeatures(featureSource, dataResource);
		}

		// Clean up the temporary Shapefile, and the directory that contained
		// the expanded contents.
		shapefileZip.delete();
		ingestUtilities.deleteDirectoryRecursive(new File(extractPath));
		featureSource.getDataStore().dispose();

		// Return the populated metadata
		return dataResource;
	}
}
