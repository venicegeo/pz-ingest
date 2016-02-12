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
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import model.data.DataResource;
import model.data.type.ShapefileResource;

import org.gdal.ogr.DataSource;
import org.gdal.ogr.Driver;
import org.gdal.ogr.Layer;
import org.gdal.ogr.ogr;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.springframework.beans.factory.annotation.Value;

import com.mongodb.MongoClient;

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
	@Value("${shapefile.explode.path}")
	private String SHAPEFILE_EXPLODED_PATH;	

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
		
		//process and persist shape file
		persistShapeFile(dataResource);
		
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
	
	/**
	 * Loads the layer(s) from provided shape file ZIP to POSTGIS DB
	 * 	by extracting the zip, finding the shape file, and loading as a new layer
	 * 
	 * @param dataResource
	 *            The DataResource
	 * @throws Exception 
	 */
	private void persistShapeFile(DataResource dataResource) throws Exception {
		final String POSTGIS_LOGIN = new StringBuilder().append("PG: ").append("host='" + POSTGRES_HOST + "' ")
				.append("port='" + POSTGRES_PORT + "' ").append("user='" + POSTGRES_USER + "' ")
				.append("dbname='" + POSTGRES_DB_NAME + "' ").append("password='" + POSTGRES_PASSWORD + "'").toString();

		String shapeFileExplodedFolder = SHAPEFILE_EXPLODED_PATH + dataResource.getDataId();
		ShapefileResource shapefileResource = (ShapefileResource) dataResource.getDataType();
		File shapeFile = shapefileResource.getLocation().getFile();

		// extracting zip to temp folder
		extractZip(shapeFile.getAbsolutePath(), shapeFileExplodedFolder);

		// load shapefile layers to postgis
		loadShapeFileToPostGIS(POSTGIS_LOGIN, shapeFileExplodedFolder, dataResource.getDataId());

		// erase extracted directory
		deleteDirectoryRecursive(new File(shapeFileExplodedFolder));
	}
	
	/**
	 * Loads the layer from shape file to POSTGIS
	 * 
	 * @param login
	 *            Postgis login info
	 * @param shapeFilePath
	 *            ShapeFilePath to directory containing the shape file
	 * @param fileName
	 *            Full name of shape file. eg: shapefile.shp
	 * @param dataResourceId
	 *            Id from datasource to be used in layer name for uniqueness 
	 * @throws Exception 
	 */
	private void loadShapeFileToPostGIS(String login, String shapeFilePath, String dataResourceId) throws Exception {
		// Register all known configured OGR drivers
		ogr.RegisterAll();

		// Open data source to PostGIS with write access, 1 = write
		DataSource postgisSource = ogr.Open(login, 1);

		// Open data source to shape file with read access, 0 = read
		Driver shapeFileDriver = ogr.GetDriverByName("ESRI Shapefile");
		DataSource fileSource = shapeFileDriver.Open(shapeFilePath + File.separator + findShapeFileName(shapeFilePath), 0);

		// Load shape file layer to postgis, should contain only single layer
		Layer shapeFileLayer = fileSource.GetLayer(0);
		String newLayerName = shapeFileLayer.GetName() + "_" + dataResourceId;
		postgisSource.CopyLayer(shapeFileLayer, newLayerName);
		
		// close ogr sources
		fileSource.delete();
		postgisSource.delete();
	}
	
	
	/**
	 * 
	 * Searches directory file list for the first matching file extension and returns the name (non-recursive)
	 * 
	 * @param directoryPath
	 *            Folder path to search
	 * @param fileExtension
	 *            File extension to match name
	 * 
	 * @return File name found in the directory
	 * @throws Exception 
	 * 
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
	 * 
	 * Unzip the given zip into output directory
	 * 
	 * @param zipPath
	 *            Zip file full path
	 * @param extractPath
	 *            Extracted zip output directory
	 * 
	 * @return boolean if successful
	 * @throws Exception 
	 * 
	 */
	private void extractZip(String zipPath, String extractPath) throws Exception {

		byte[] buffer = new byte[1024];
		try {
			// create output directory
			File directory = new File(extractPath);
			if (!directory.exists()) {
				directory.mkdir();
			}

			// stream from zip content
			ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipPath));

			// get initial file list entry
			ZipEntry zipEntry = zipInputStream.getNextEntry();
			while (zipEntry != null) {
				String fileName = zipEntry.getName();
				File newFile = new File(extractPath + File.separator + fileName);

				// create all non existing folders
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
			throw new Exception ("Unable to extract zip: " + zipPath + " to path " + extractPath);
		}
	}
	
	/**
	 * 
	 * Recursive deletion of directory
	 * 
	 * @param File
	 *            Directory to be deleted
	 * 
	 * @return boolean if successful
	 * @throws Exception 
	 * 
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
					throw new Exception(
							"Unable to delete file " + files[i].getName() + " from " + directory.getAbsolutePath());
			}

			result = directory.delete();
		}

		return result;
	}

}
