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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import model.data.DataResource;
import model.data.location.FileAccessFactory;
import model.data.type.PostGISDataType;
import model.data.type.WfsDataType;
import model.job.metadata.SpatialMetadata;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import ingest.utility.IngestUtilities;
import util.GeoToolsUtil;

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
	@Value("${postgres.schema}")
	private String POSTGRES_SCHEMA;
	
	@Value("${data.temp.geojson.path}")
	private String DATA_TEMP_PATH;
	
	@Autowired
	IngestUtilities ingestUtilities;

	@Override
	public DataResource inspect(DataResource dataResource, boolean host) throws Exception {
		
//		ClassLoader classLoader = getClass().getClassLoader();
//		String payload="";
//		try {
//			payload = IOUtils.toString(classLoader.getResourceAsStream("templates/geojson.json"));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		

System.out.println("===================================\n calling toShp();" );


// Create new local unique folder to hold the shapefile specific files
File localWriteDir = new File(String.format("%s%s", "tmp_output_", dataResource.getDataId()));
localWriteDir.mkdir();

// Create new placeholder for a new shapefile
File shapeFilePlaceHolder = new File(String.format("%s%s%s%s", localWriteDir.getAbsolutePath(), File.separator, dataResource.getDataId(), ".shp"));
shapeFilePlaceHolder.createNewFile();

File newShapeFile = convertGeoJsonToShapeFile(shapeFilePlaceHolder, dataResource);

// Persisting newly mapped shapefile into the Piazza PostGIS database.
if (host) {
	//ingestUtilities.persistShapeFile(FeatureSource<SimpleFeatureType, SimpleFeature> shpFeatureSource, DataResource dataResource);
}

// Delete temporary shapefile contents local folder 
ingestUtilities.deleteDirectoryRecursive(localWriteDir);


//File shapefileZip = new File(String.format("%s%s%s.%s", DATA_TEMP_PATH, File.separator, dataResource.getDataId(), "zip"));
//FileUtils.copyInputStreamToFile(shapefileStream, shapefileZip);

		// Return the Populated Metadata
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

//		ClassLoader classLoader = getClass().getClassLoader();
//		String payload = "";
//		try {
//			payload = IOUtils.toString(classLoader.getResourceAsStream("templates/geojson.json"));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		InputStream stream = new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8));

		// FileAccessFactory fileFactory = new
		// FileAccessFactory(AMAZONS3_ACCESS_KEY, AMAZONS3_PRIVATE_KEY);
		// File file = new File(GEOSERVER_DATA_DIRECTORY,
		// fileLocation.getFileName());
		// FileUtils.copyInputStreamToFile(fileFactory.getFile(fileLocation),
		// file);

		// File shpFile = new File("test.shp");
		//File newShapeFile = new File("C:\\geoFiles\\geojson\\tmp\\geojsonToShapeFileOutput.shp");
		
		
		File geoJsonOriginalFile = new File("C:\\geoFiles\\geojson\\gz_2010_us_outline_500k.json");
		//File geoJsonOriginalFile = new File("C:\\geoFiles\\geojson\\Hanson_Area_GS_NGA.geojson");
		//File geoJsonOriginalFile = new File("C:\\geoFiles\\geojson\\Hanson_Output3_espg4326.geojson");
		
System.out.println("===================================\n geoJsonFile: " + geoJsonOriginalFile.getAbsolutePath() );
		// FileUtils.copyInputStreamToFile(stream, newShapeFile);

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
			System.out.println(typeName + " does not support read/write access");
		}
		
		return shapefileOutput;
	}
	
	
	public static SimpleFeatureSource geoJSON2Shp(InputStream input,
	        SimpleFeatureType schema, ShapefileDataStore shpDataStore)
	        throws IOException {

	    FeatureJSON fjson = new FeatureJSON(new GeometryJSON(15));

	    SimpleFeatureType featureType = schema;

	    if (featureType != null) {
	        fjson.setFeatureType(featureType);
	    }

	    FeatureIterator<SimpleFeature> jsonIt = fjson
	            .streamFeatureCollection(input);

	    if (!jsonIt.hasNext()) {
	        throw new IllegalArgumentException(
	                "Cannot create shapefile. GeoJSON stream is empty");
	    }

	    FeatureWriter<SimpleFeatureType, SimpleFeature> writer = null;

	    try {

	        // use feature type of first feature, if not supplied
	        SimpleFeature firstFeature = jsonIt.next();
	        if (featureType == null) {
	            featureType = firstFeature.getFeatureType();
	        }

	        shpDataStore.createSchema(featureType);

	        writer = shpDataStore.getFeatureWriterAppend(
	                shpDataStore.getTypeNames()[0], Transaction.AUTO_COMMIT);

	        addFeature(firstFeature, writer);

	        while (jsonIt.hasNext()) {
	            SimpleFeature feature = jsonIt.next();
	            addFeature(feature, writer);                
	        }
	    } finally {
	        if (writer != null) {
	            writer.close();
	        }
	    }

	    return shpDataStore.getFeatureSource(shpDataStore.getTypeNames()[0]);
	}


	private static void addFeature(SimpleFeature feature, FeatureWriter<SimpleFeatureType, SimpleFeature> writer) throws IOException {

	    SimpleFeature toWrite = writer.next();
	    for (int i = 0; i < toWrite.getType().getAttributeCount(); i++) {
	        String name = toWrite.getType().getDescriptor(i).getLocalName();
	        toWrite.setAttribute(name, feature.getAttribute(name));
	    }

	    // copy over the user data
	    if (feature.getUserData().size() > 0) {
	        toWrite.getUserData().putAll(feature.getUserData());
	    }

	    // perform the write
	    writer.write();
	}  
	
	/**
	 * Copies the WFS Resource into a new Piazza PostGIS table.
	 * 
	 * @param dataResource
	 *            The WFS Data Resource to copy.
	 * @param featureSource
	 *            GeoTools Feature source for WFS.
	 */
	private void copyWfsToPostGis(DataResource dataResource,
			FeatureSource<SimpleFeatureType, SimpleFeature> wfsFeatureSource) throws Exception {
		// Create a Connection to the Piazza PostGIS Database for writing.
		DataStore postGisStore = GeoToolsUtil.getPostGisDataStore(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_SCHEMA,
				POSTGRES_DB_NAME, POSTGRES_USER, POSTGRES_PASSWORD);

		// Create the Schema in the Data Store
		String tableName = dataResource.getDataId();
		SimpleFeatureType wfsSchema = wfsFeatureSource.getSchema();
		SimpleFeatureType postGisSchema = GeoToolsUtil.cloneFeatureType(wfsSchema, tableName);
		postGisStore.createSchema(postGisSchema);
		SimpleFeatureStore postGisFeatureStore = (SimpleFeatureStore) postGisStore.getFeatureSource(tableName);

		// Commit the Features to the Data Store
		Transaction transaction = new DefaultTransaction();
		try {
			// Get the Features from the WFS and add them to the PostGIS store
			SimpleFeatureCollection wfsFeatures = (SimpleFeatureCollection) wfsFeatureSource.getFeatures();
			postGisFeatureStore.addFeatures(wfsFeatures);
			// Commit the changes and clean up
			transaction.commit();
			transaction.close();
		} catch (IOException exception) {
			// Clean up resources
			transaction.rollback();
			transaction.close();
			System.out.println("Error copying WFS to PostGIS: " + exception.getMessage());
			// Rethrow
			throw exception;
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
	 * @return GeoTools Feature Source that can be queried for spatial features
	 *         and metadata
	 */
	public FeatureSource<SimpleFeatureType, SimpleFeature> getGeoJsonFeatureSource(DataResource dataResource) throws IOException {
		// Form the Get Capabilities URL
		WfsDataType wfsResource = (WfsDataType) dataResource.getDataType();
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
