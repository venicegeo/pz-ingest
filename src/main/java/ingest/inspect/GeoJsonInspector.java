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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import model.data.DataResource;
import model.data.location.FileAccessFactory;
import model.data.type.GeoJsonDataType;
import model.job.metadata.SpatialMetadata;

import org.apache.commons.io.FileUtils;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.data.Transaction;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureTypeImpl;
import org.geotools.feature.type.GeometryDescriptorImpl;
import org.geotools.feature.type.GeometryTypeImpl;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.AttributeType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.GeometryType;
import org.opengis.filter.identity.FeatureId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import util.PiazzaLogger;

/**
 * Inspects GeoJSON string and loads it into postGIS only if host is true.
 * 
 * @author Sonny.Saniev
 * 
 */
@Component
public class GeoJsonInspector implements InspectorType {
	@Value("${vcap.services.pz-geoserver.credentials.postgres.hostname}")
	private String POSTGRES_HOST;
	@Value("${vcap.services.pz-geoserver.credentials.postgres.port}")
	private String POSTGRES_PORT;
	@Value("${vcap.services.pz-geoserver.credentials.postgres.database}")
	private String POSTGRES_DB_NAME;
	@Value("${vcap.services.pz-geoserver.credentials.postgres.username}")
	private String POSTGRES_USER;
	@Value("${vcap.services.pz-geoserver.credentials.postgres.password}")
	private String POSTGRES_PASSWORD;
	@Value("${vcap.services.pz-blobstore.credentials.access_key_id:}")
	private String AMAZONS3_ACCESS_KEY;
	@Value("${vcap.services.pz-blobstore.credentials.secret_access_key:}")
	private String AMAZONS3_PRIVATE_KEY;
	@Value("${postgres.schema}")
	private String POSTGRES_SCHEMA;

	private static final Integer DEFAULT_GEOJSON_EPSG_CODE = 4326;

	@Autowired
	IngestUtilities ingestUtilities;
	@Autowired
	private PiazzaLogger logger;

	private ShapefileDataStore shpDataStore;
	
	@Override
	public DataResource inspect(DataResource dataResource, boolean host) throws Exception {

		// Create local placeholder file for Shapefile contents
		File localWriteDir = new File(String.format("%s%s", "tmp_output_", dataResource.getDataId()));
		localWriteDir.mkdir();
		String file = String.format("%s%s%s%s", localWriteDir.getAbsolutePath(), File.separator, dataResource.getDataId(), ".shp");
		File shapeFilePlaceHolder = new File(file);
		shapeFilePlaceHolder.createNewFile();

		// Persist mapped Shapefile into the Piazza PostGIS Database.
		if (host && dataResource.getDataType() instanceof GeoJsonDataType) {

			// Map GeoJSON to Shapefile
			File newShapeFile = convertGeoJsonToShapeFile4(shapeFilePlaceHolder, dataResource);

			// Persist to PostGIS
			FeatureSource<SimpleFeatureType, SimpleFeature> shpFeatureSource = ingestUtilities
					.getShapefileDataStore(newShapeFile.getAbsolutePath());
			ingestUtilities.persistShapeFile(shpFeatureSource, dataResource);

			// Get the Bounding Box, set the Spatial Metadata
			SpatialMetadata spatialMetadata = new SpatialMetadata();
			ReferencedEnvelope envelope = shpFeatureSource.getBounds();
			spatialMetadata.setMinX(envelope.getMinX());
			spatialMetadata.setMinY(envelope.getMinY());
			spatialMetadata.setMaxX(envelope.getMaxX());
			spatialMetadata.setMaxY(envelope.getMaxY());

			// Get the SRS and EPSG codes
			if (shpFeatureSource.getInfo().getCRS() != null) {
				spatialMetadata.setCoordinateReferenceSystem(shpFeatureSource.getInfo().getCRS().toString());
				spatialMetadata.setEpsgCode(CRS.lookupEpsgCode(shpFeatureSource.getInfo().getCRS(), true));
			} else {
				// Default to EPSG 4326. Most GeoJSON is this code, and is sort
				// of an unofficial standard for GeoJSON.
				spatialMetadata.setEpsgCode(DEFAULT_GEOJSON_EPSG_CODE);
			}

			dataResource.spatialMetadata = spatialMetadata;

			// Convert DataType to postgis from geojson
			((GeoJsonDataType) dataResource.getDataType()).setDatabaseTableName(dataResource.getDataId());
		}

		// Delete temporary Shapefile contents local temp folder
		ingestUtilities.deleteDirectoryRecursive(shapeFilePlaceHolder.getParentFile());

		// Return DataResource
		return dataResource;
	}

	/**
	 * 
	 * @param shapefileOutput
	 *            Shapefile file to map geojson into.
	 * @param dataResource
	 *            DataResource for unique names from id.
	 * 
	 * @return File object location of the newly created shapefile
	 * @throws Exception
	 */
	public File convertGeoJsonToShapeFile4(File shapefileOutput, DataResource dataResource) throws Exception {

		File geoJsonOriginalFile = getFile(dataResource);

		// Mapping geojson to shapefile
		ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
		Map<String, Serializable> params = new HashMap<String, Serializable>();
		params.put("url", shapefileOutput.toURI().toURL());
		params.put("create spatial index", Boolean.TRUE);
		shpDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
		
		InputStream in = new FileInputStream(geoJsonOriginalFile);
		int decimals = 15;
		GeometryJSON gjson = new GeometryJSON(decimals);
		FeatureJSON fjson = new FeatureJSON(gjson);
		//FeatureCollection fc = fjson.readFeatureCollection(in);
        FeatureCollection<SimpleFeatureType, SimpleFeature> fc = fjson.readFeatureCollection(in);
        fc.getSchema();
        
        // write features to shape file
        writeFeatures(fc);
        
		// clean up efforts
		in.close();
		ingestUtilities.deleteDirectoryRecursive(geoJsonOriginalFile.getParentFile());

		return shapefileOutput;
	}
	
	/**
	 * Code snippet from https://gitlab.com/snippets/9275 to write features into
	 * the shapefile
	 * 
	 * @param features
	 *            FeatureCollection parameter
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("resource")
	private boolean writeFeatures(FeatureCollection<SimpleFeatureType, SimpleFeature> features) throws Exception {

		if (shpDataStore == null) {
			throw new IllegalStateException("Datastore can not be null when writing");
		}
		SimpleFeatureType schema = features.getSchema();
		GeometryDescriptor geom = schema.getGeometryDescriptor();
		String oldGeomAttrib = "";
		try {

			// Write the features to the shapefile
			Transaction transaction = new DefaultTransaction("create");

			/*
			 * The Shapefile format has a couple limitations: - "the_geom" is
			 * always first, and used for the geometry attribute name -
			 * "the_geom" must be of type Point, MultiPoint, MuiltiLineString,
			 * MultiPolygon - Attribute names are limited in length - Not all
			 * data types are supported (example Timestamp represented as Date)
			 *
			 * Because of this we have to rename the geometry element and then
			 * rebuild the features to make sure that it is the first attribute.
			 */
			List<AttributeDescriptor> attributes = schema.getAttributeDescriptors();
			GeometryType geomType = null;
			List<AttributeDescriptor> attribs = new ArrayList<AttributeDescriptor>();
			for (AttributeDescriptor attrib : attributes) {
				AttributeType type = attrib.getType();
				if (type instanceof GeometryType) {
					geomType = (GeometryType) type;
					oldGeomAttrib = attrib.getLocalName();
				} else {
					attribs.add(attrib);
				}
			}

			GeometryTypeImpl gt = new GeometryTypeImpl(new NameImpl("the_geom"), geomType.getBinding(),
					geomType.getCoordinateReferenceSystem(), geomType.isIdentified(), geomType.isAbstract(), geomType.getRestrictions(),
					geomType.getSuper(), geomType.getDescription());

			GeometryDescriptor geomDesc = new GeometryDescriptorImpl(gt, new NameImpl("the_geom"), geom.getMinOccurs(), geom.getMaxOccurs(),
					geom.isNillable(), geom.getDefaultValue());

			attribs.add(0, geomDesc);

			SimpleFeatureType shpType = new SimpleFeatureTypeImpl(schema.getName(), attribs, geomDesc, schema.isAbstract(),
					schema.getRestrictions(), schema.getSuper(), schema.getDescription());

			shpDataStore.createSchema(shpType);

			String typeName = shpDataStore.getTypeNames()[0];
			SimpleFeatureSource featureSource = shpDataStore.getFeatureSource(typeName);

			// if (featureSource instanceof SimpleFeatureStore)
			if (featureSource instanceof FeatureStore) {

				SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
				List<SimpleFeature> feats = new ArrayList<SimpleFeature>();

				FeatureIterator<SimpleFeature> features2 = features.features();
				while (features2.hasNext()) {
					SimpleFeature f = features2.next();
					SimpleFeature reType = DataUtilities.reType(shpType, f, true);
					// set the default Geom (the_geom) from the original Geom
					reType.setAttribute("the_geom", f.getAttribute(oldGeomAttrib));

					feats.add(reType);
				}
				features2.close();
				SimpleFeatureCollection collection = new ListFeatureCollection(shpType, feats);

				featureStore.setTransaction(transaction);
				try {
					List<FeatureId> ids = featureStore.addFeatures(collection);
					transaction.commit();
				} catch (Exception problem) {
					problem.printStackTrace();
					transaction.rollback();
				} finally {
					transaction.close();
				}
				shpDataStore.dispose();
				return true;
			} else {
				shpDataStore.dispose();
				System.err.println("ShapefileStore not writable");
				logger.log("GeoJson to mapping exception occurred, temporary ShapefileStore not writable", PiazzaLogger.ERROR);

				throw new Exception("GeoJson to mapping exception occurred, temporary ShapefileStore not writable");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 
	 * @param dataResource
	 *            data resource to pull file from
	 * @return File object
	 * @throws Exception
	 */
	private File getFile(DataResource dataResource) throws Exception {
		File file = new File(String.format("%s%s%s%s.%s", "tmp_geojson_", dataResource.getDataId(), File.separator,
				dataResource.getDataId(), "json"));
		FileAccessFactory fileFactory = new FileAccessFactory(AMAZONS3_ACCESS_KEY, AMAZONS3_PRIVATE_KEY);
		InputStream fileStream = fileFactory.getFile(((GeoJsonDataType) dataResource.getDataType()).getLocation());
		FileUtils.copyInputStreamToFile(fileStream, file);

		return file;
	}
}
