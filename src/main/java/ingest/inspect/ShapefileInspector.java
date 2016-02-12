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
		
		//load the shape file to postgis
		loadLayerstoPostGIS(dataResource);
		
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
	 */
	private void loadLayerstoPostGIS(DataResource dataResource) {
		/*
		//connect the shape file location to pipes below for later use
		ShapefileResource shapefileResource = (ShapefileResource) dataResource.getDataType();
		File shapeFile = shapefileResource.getLocation().getFile();
		*/
		
		final String fileExtension = "shp";
		final String ZIP_PATH = "C:\\temp\\SheltersShp.zip";
		final String UNZIP_PATH = "C:\\temp\\unzipDirectory";
		final String POSTGIS_LOGIN = "PG: host='postgis.dev' port='5432' user='piazza' dbname='piazza' password='piazza'";

		// extracting zip to temporary folder
		boolean extracted = extractZip(ZIP_PATH, UNZIP_PATH);

		// obtain shape file name from unzipped folder
		String shapeFileName = findOneFileNameWithMatchingExtension_FromDirectory(UNZIP_PATH, fileExtension);

		if (extracted && shapeFileName != null) {
			try {
				loadShapeFileToPostGIS(POSTGIS_LOGIN, UNZIP_PATH, shapeFileName, dataResource.getDataId());
			} catch (Exception e) {
				System.err.println("Something went wrong while loading shape file(s) to postgis. " + e.getMessage());
			}

			// erase unzipped directory
			System.out.println("\nCleanup of extracted shapefile zip...");
			deleteDirectoryRecursive(new File(UNZIP_PATH));

		} else {
			System.err.println("Unable to find shp file from extracted folder: " + UNZIP_PATH);
		}
	}
	
		
	/**
	 * Loads the layer(s) from provided shape file to POSTGIS DB
	 * 
	 * @param login
	 *            Postgis login info
	 * @param shapeFilePath
	 *            ShapeFilePath to directory containing the shape file
	 * @param fileName
	 *            Full name of shape file. eg: shapefile.shp
	 * @param dataResourceId
	 *            Id from datasource to be used in layer name for uniqueness 
	 */
	private void loadShapeFileToPostGIS(String login, String shapeFilePath, String fileName, String dataResourceId) {

		// Register all known configured OGR drivers
		ogr.RegisterAll();

		// Open data source to PostGIS with write access, 1 = write
		DataSource postgisSource = ogr.Open(login, 1);

		// Open data source to shape file with read access, 0 = read only
		Driver shapeFileDriver = ogr.GetDriverByName("ESRI Shapefile");
		DataSource fileSource = shapeFileDriver.Open(shapeFilePath + File.separator + fileName, 0);

		// options for copying layers
		Vector<String> options = new Vector<String>();
		options.add("OVERWRITE=YES");

		// Load layer(s) from shape file to postgis, shape file should contain
		// only 1 layer
		for (int layerIndex = 0; layerIndex < fileSource.GetLayerCount(); layerIndex++) {

			Layer shapeFileLayer = fileSource.GetLayer(layerIndex);
			String newLayerName = shapeFileLayer.GetName() + "_" + dataResourceId;

			postgisSource.CopyLayer(shapeFileLayer, newLayerName, options);
			System.out.println("loaded layer \"" + shapeFileLayer.GetName() + "\" to \""
					+ postgisSource.GetDriver().getName() + "\"");
		}

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
	 * 
	 */
	private String findOneFileNameWithMatchingExtension_FromDirectory(String directoryPath, String fileExtension) {
		File[] files = new File(directoryPath).listFiles();
		for (int index = 0; index < files.length; index++) {
			String fileName = files[index].getName();

			if (fileName.toLowerCase().endsWith("." + fileExtension))
				return fileName;
		}

		return null;
	}

	/**
	 * 
	 * Unzip the given zip file into output directory
	 * 
	 * @param zipSource
	 *            Input zip file path
	 * @param extractDirectory
	 *            Extracted zip output directory
	 * 
	 * @return boolean if successful
	 * 
	 */
	private boolean extractZip(String zipSource, String extractDirectory) {

		System.out.println("\nExtracting zip: " + zipSource);
		byte[] buffer = new byte[1024];
		try {

			// create output directory
			File directory = new File(extractDirectory);
			if (!directory.exists()) {
				directory.mkdir();
			}

			// stream from zip content
			ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipSource));

			// get initial file list entry
			ZipEntry zipEntry = zipInputStream.getNextEntry();
			while (zipEntry != null) {
				String fileName = zipEntry.getName();
				File newFile = new File(extractDirectory + File.separator + fileName);
				System.out.println("file extracted: " + newFile.getAbsoluteFile());

				// create all non existing folders
				// or will trigger FileNotFoundException for compressed folder
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

			System.out.println("Done\n");
		} catch (IOException ex) {
			ex.printStackTrace();
			System.err.println("Unable to extract zip: " + zipSource);

			return false;
		}

		return true;
	}
	
	/**
	 * 
	 * Recursive deletion of directory
	 * 
	 * @param File
	 *            Directory to be deleted
	 * 
	 * @return boolean if successful
	 * 
	 */
	private boolean deleteDirectoryRecursive(File directory) {
		boolean result = false;

		if (directory.isDirectory()) {
			File[] files = directory.listFiles();

			for (int i = 0; i < files.length; i++) {
				if (files[i].isDirectory()) {
					deleteDirectoryRecursive(files[i]);
				}

				System.out.println("deleting file: " + files[i].getName() + " --deleted: " + files[i].delete());
			}

			result = directory.delete();
		}

		System.out.println("done...");
		return result;
	}

}
