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
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import model.data.DataResource;
import model.data.location.FileAccessFactory;
import model.data.type.GeoJsonDataType;
import org.apache.commons.io.FileUtils;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import ingest.utility.IngestUtilities;
import util.PiazzaLogger;

/**
 * Inspects geojson string and loads it into postGIS.
 * 
 * @author Sonny.Saniev
 * 
 */
@Component
public class GeoJsonInspector implements InspectorType {
	private static final String CAPABILITIES_TEMPLATE = "%s?SERVICE=wfs&REQUEST=GetCapabilities&VERSION=%s";
	@Value("${vcap.services.pz-postgres.credentials.host}")
	private String POSTGRES_HOST;
	@Value("${vcap.services.pz-postgres.credentials.port}")
	private String POSTGRES_PORT;
	@Value("${vcap.services.pz-postgres.credentials.database}")
	private String POSTGRES_DB_NAME;
	@Value("${vcap.services.pz-postgres.credentials.username}")
	private String POSTGRES_USER;
	@Value("${vcap.services.pz-postgres.credentials.password}")
	private String POSTGRES_PASSWORD;
	@Value("${vcap.services.pz-blobstore.credentials.access:}")
	private String AMAZONS3_ACCESS_KEY;
	@Value("${vcap.services.pz-blobstore.credentials.private:}")
	private String AMAZONS3_PRIVATE_KEY;
	@Value("${postgres.schema}")
	private String POSTGRES_SCHEMA;
	@Value("${data.temp.geojson.path}")
	private String DATA_TEMP_PATH;
	@Autowired
	IngestUtilities ingestUtilities;
	@Autowired
	private PiazzaLogger logger;

	@Override
	public DataResource inspect(DataResource dataResource, boolean host) throws Exception {
		
		// Create local directory to hold the shapefile contents
		File localWriteDir = new File(String.format("%s%s", "tmp_output_", dataResource.getDataId()));
		localWriteDir.mkdir();
		
		// Create new placeholder for new shapefile
		File shapeFilePlaceHolder = new File(String.format("%s%s%s%s", localWriteDir.getAbsolutePath(), File.separator, dataResource.getDataId(), ".shp"));
		shapeFilePlaceHolder.createNewFile();
		
		// Map geojson to shapefile
		File newShapeFile = convertGeoJsonToShapeFile(shapeFilePlaceHolder, dataResource);
		
		// Persisting mapped shapefile into the Piazza PostGIS database.
		if (true || host ) {
			FeatureSource<SimpleFeatureType, SimpleFeature> shpFeatureSource = ingestUtilities.getShapefileDataStore(newShapeFile.getAbsolutePath());
			ingestUtilities.persistShapeFile(shpFeatureSource, dataResource);
		}

		// Delete temporary shapefile contents local folder 
		ingestUtilities.deleteDirectoryRecursive(localWriteDir);

		// Return DataResource
		return dataResource;
	}

	/**
	 * @param geojson
	 * @param dataResource
	 * 			dadataresource to be used for unique temp folder output generation
	 * 
	 * @return File object location of the newly created shapefile
	 * 		
	 * @throws Exception
	 */
	public File convertGeoJsonToShapeFile(File shapefileOutput, DataResource dataResource) throws Exception {

		File geoJsonOriginalFile = getFile(dataResource);
		//File geoJsonOriginalFile = new File("C:\\geoFiles\\geojson\\gz_2010_us_outline_500k.json");
		//File geoJsonOriginalFile = new File("C:\\geoFiles\\geojson\\Hanson_Area_GS_NGA.geojson");
		//File geoJsonOriginalFile = new File("C:\\geoFiles\\geojson\\Hanson_Output3_espg4326.geojson");

		// Mapping geojson to shapefile
		ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
		Map<String, Serializable> params = new HashMap<String, Serializable>();
		params.put("url", shapefileOutput.toURI().toURL());
		params.put("create spatial index", Boolean.TRUE);
		ShapefileDataStore shpDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
		InputStream in = new FileInputStream(geoJsonOriginalFile);
		int decimals = 15;
		GeometryJSON gjson = new GeometryJSON(decimals);
		FeatureJSON fjson = new FeatureJSON(gjson);
		FeatureCollection fc = fjson.readFeatureCollection(in);
		SimpleFeatureType type = (SimpleFeatureType) fc.getSchema();
		shpDataStore.createSchema(type);
		Transaction transaction = new DefaultTransaction("create");
		String typeName = shpDataStore.getTypeNames()[0];
		SimpleFeatureSource featureSource = shpDataStore.getFeatureSource(typeName);
		if (featureSource instanceof FeatureStore) {
			SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
			featureStore.setTransaction(transaction);
			try {
				featureStore.addFeatures(fc);
				transaction.commit();
			} catch (Exception ex) {
				ex.printStackTrace();
				transaction.rollback();
			} finally {
				transaction.close();
			}
		} else {
			logger.log(String.format("%s%s", typeName, " does not support read/write access"), PiazzaLogger.INFO);
		}
		
		return shapefileOutput;
	}
	
	/**
	 * 
	 * @param dataResource data resource to pull file from 
	 * @return File object
	 * @throws Exception
	 */
	private File getFile(DataResource dataResource) throws Exception {
		// Convert this to generic method, need to cast to generic type
		File file = new File(String.format("%s%s%s.%s", DATA_TEMP_PATH, File.separator, dataResource.getDataId(), "tif"));

		FileAccessFactory fileFactory = new FileAccessFactory(AMAZONS3_ACCESS_KEY, AMAZONS3_PRIVATE_KEY);
		InputStream fileStream = fileFactory.getFile(((GeoJsonDataType) dataResource.getDataType()).getLocation());
		FileUtils.copyInputStreamToFile(fileStream, file);
		
		return file;
	}
}
