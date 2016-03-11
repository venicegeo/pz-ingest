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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import model.data.DataResource;
import model.data.location.FileAccessFactory;
import model.data.type.ShapefileDataType;
import model.job.metadata.SpatialMetadata;

import org.apache.commons.io.FileUtils;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import util.GeoToolsUtil;

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
	@Value("${data.temp.path}")
	private String DATA_TEMP_PATH;
	@Value("${s3.key.access:}")
	private String AMAZONS3_ACCESS_KEY;
	@Value("${s3.key.private:}")
	private String AMAZONS3_PRIVATE_KEY;

	@Override
	public DataResource inspect(DataResource dataResource, boolean host) throws Exception {
		// Get the Shapefile and write it to disk for temporary use.
		FileAccessFactory fileFactory = new FileAccessFactory(AMAZONS3_ACCESS_KEY, AMAZONS3_PRIVATE_KEY);
		InputStream shapefileStream = fileFactory.getFile(((ShapefileDataType) dataResource.getDataType())
				.getLocation());
		File shapefileZip = new File(String.format("%s.%s.%s", DATA_TEMP_PATH, dataResource.getDataId(), "zip"));
		FileUtils.copyInputStreamToFile(shapefileStream, shapefileZip);

		// Unzip the Shapefile into a temporary directory, which will allow us
		// to parse the Shapefile's sidecar files.
		String extractPath = DATA_TEMP_PATH + dataResource.getDataId();
		extractZip(shapefileZip.getAbsolutePath(), extractPath);
		// Get the path to the actual *.shp file
		String shapefilePath = String.format("%s\\%s", extractPath, findShapeFileName(extractPath));

		// Get the Store information from GeoTools for accessing the Shapefile
		FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = getShapefileDataStore(shapefilePath);

		// Get the Bounding Box, set the Spatial Metadata
		SpatialMetadata spatialMetadata = new SpatialMetadata();
		ReferencedEnvelope envelope = featureSource.getBounds();
		spatialMetadata.setMinX(envelope.getMinX());
		spatialMetadata.setMinY(envelope.getMinY());
		spatialMetadata.setMaxX(envelope.getMaxX());
		spatialMetadata.setMaxY(envelope.getMaxY());

		// Get the SRS and EPSG codes
		spatialMetadata.setCoordinateReferenceSystem(featureSource.getInfo().getCRS().toString());
		spatialMetadata.setEpsgCode(CRS.lookupEpsgCode(featureSource.getInfo().getCRS(), true));

		// Set the spatial metadata
		dataResource.spatialMetadata = spatialMetadata;

		// Process and persist shapefile file into the Piazza PostGIS database.
		if (host) {
			persistShapeFile(featureSource, dataResource);
		}

		// Clean up the temporary Shapefile, and the directory that contained
		// the expanded contents.
		shapefileZip.delete();
		deleteDirectoryRecursive(new File(extractPath));
		featureSource.getDataStore().dispose();

		// Return the populated metadata
		return dataResource;
	}

	/**
	 * Gets the GeoTools Feature Store for the Shapefile.
	 * 
	 * @param shapefilePath
	 *            The full string path to the expanded *.shp shape file.
	 * @return The GeoTools Shapefile Data Store Feature Source
	 */
	private FeatureSource<SimpleFeatureType, SimpleFeature> getShapefileDataStore(String shapefilePath)
			throws IOException {
		File shapefile = new File(shapefilePath);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("url", shapefile.toURI().toURL());
		DataStore dataStore = DataStoreFinder.getDataStore(map);
		String typeName = dataStore.getTypeNames()[0];
		FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = dataStore.getFeatureSource(typeName);
		return featureSource;
	}

	/**
	 * Loads the contents of the Shapefile into the PostGIS Database.
	 * 
	 * @param shpFeatureSource
	 *            The GeoTools FeatureSource for the shapefile information.
	 * @param dataResource
	 *            The DataResource object with Shapefile metadata
	 */
	private void persistShapeFile(FeatureSource<SimpleFeatureType, SimpleFeature> shpFeatureSource,
			DataResource dataResource) throws Exception {
		// Get the DataStore to the PostGIS database.
		DataStore postGisStore = GeoToolsUtil.getPostGisDataStore(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_SCHEMA,
				POSTGRES_DB_NAME, POSTGRES_USER, POSTGRES_PASSWORD);

		// Create the Schema in the Data Store
		String tableName = dataResource.getDataId();
		// Associate the Table name with the Shapefile Resource
		((ShapefileDataType) dataResource.getDataType()).setDatabaseTableName(tableName);
		SimpleFeatureType shpSchema = shpFeatureSource.getSchema();
		SimpleFeatureType postGisSchema = GeoToolsUtil.cloneFeatureType(shpSchema, tableName);
		postGisStore.createSchema(postGisSchema);
		SimpleFeatureStore postGisFeatureStore = (SimpleFeatureStore) postGisStore.getFeatureSource(tableName);

		// Commit the Features to the Data Store
		Transaction transaction = new DefaultTransaction();
		try {
			// Get the Features from the Shapefile and add to the PostGIS store
			SimpleFeatureCollection wfsFeatures = (SimpleFeatureCollection) shpFeatureSource.getFeatures();
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
			// Cleanup Data Store
			postGisStore.dispose();
		}
	}

	/**
	 * Searches directory file list for the first matching file extension and
	 * returns the name (non-recursive)
	 * 
	 * @param directoryPath
	 *            Folder path to search
	 * @param fileExtension
	 *            File extension to match name
	 * 
	 * @return File name found in the directory
	 */
	private String findShapeFileName(String directoryPath) throws Exception {
		File[] files = new File(directoryPath).listFiles();
		for (int index = 0; index < files.length; index++) {
			String fileName = files[index].getName();
			if (fileName.toLowerCase().endsWith("." + "shp"))
				return fileName;
		}

		throw new Exception("No shape file was found inside unzipped directory: " + directoryPath);
	}

	/**
	 * Unzip the given zip into output directory
	 * 
	 * @param zipPath
	 *            Zip file full path
	 * @param extractPath
	 *            Extracted zip output directory
	 * 
	 * @return boolean if successful
	 */
	private void extractZip(String zipPath, String extractPath) throws Exception {
		byte[] buffer = new byte[1024];
		try {
			// Create output directory
			File directory = new File(extractPath);
			if (!directory.exists()) {
				directory.mkdir();
			}

			// Stream from zip content
			ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipPath));

			// Get initial file list entry
			ZipEntry zipEntry = zipInputStream.getNextEntry();
			while (zipEntry != null) {
				String fileName = zipEntry.getName();
				File newFile = new File(extractPath + File.separator + fileName);

				// Create all non existing folders
				new File(newFile.getParent()).mkdirs();
				FileOutputStream outputStream = new FileOutputStream(newFile);

				int length;
				while ((length = zipInputStream.read(buffer)) > 0) {
					outputStream.write(buffer, 0, length);
				}

				outputStream.close();
				zipEntry = zipInputStream.getNextEntry();
			}

			zipInputStream.closeEntry();
			zipInputStream.close();
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new Exception("Unable to extract zip: " + zipPath + " to path " + extractPath);
		}
	}

	/**
	 * Recursive deletion of directory
	 * 
	 * @param File
	 *            Directory to be deleted
	 * 
	 * @return boolean if successful
	 * @throws Exception
	 */
	private boolean deleteDirectoryRecursive(File directory) throws Exception {
		boolean result = false;

		if (directory.isDirectory()) {
			File[] files = directory.listFiles();

			for (int i = 0; i < files.length; i++) {
				if (files[i].isDirectory()) {
					deleteDirectoryRecursive(files[i]);
				}

				if (!files[i].delete())
					throw new Exception("Unable to delete file " + files[i].getName() + " from "
							+ directory.getAbsolutePath());
			}

			result = directory.delete();
		}

		return result;
	}

}
