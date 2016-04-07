package ingest.utility;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureSource;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import model.data.DataResource;
import model.data.type.ShapefileDataType;
import util.GeoToolsUtil;

/**
 * Utility component for some generic processing efforts.
 * 
 * @author Sonny.Saniev
 * 
 */
@Component
public class IngestUtilities {
	
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
	
	/**
	 * Recursive deletion of directory
	 * 
	 * @param File
	 *            Directory to be deleted
	 * 
	 * @return boolean if successful
	 * @throws Exception
	 */
	public boolean deleteDirectoryRecursive(File directory) throws Exception {
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
	public void extractZip(String zipPath, String extractPath) throws Exception {
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
	 * Gets the GeoTools Feature Store for the Shapefile.
	 * 
	 * @param shapefilePath
	 *            The full string path to the expanded *.shp shape file.
	 * @return The GeoTools Shapefile Data Store Feature Source
	 */
	public FeatureSource<SimpleFeatureType, SimpleFeature> getShapefileDataStore(String shapefilePath) throws IOException {
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
	public void persistShapeFile(FeatureSource<SimpleFeatureType, SimpleFeature> shpFeatureSource, DataResource dataResource) throws Exception {
		// Get the dataStore to the postGIS database.
		DataStore postGisStore = GeoToolsUtil.getPostGisDataStore(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_SCHEMA, POSTGRES_DB_NAME, POSTGRES_USER, POSTGRES_PASSWORD);

		// Create the schema in the data store
		String tableName = dataResource.getDataId();
		
		// Associate the table name with the shapefile resource
		SimpleFeatureType shpSchema = shpFeatureSource.getSchema();
		SimpleFeatureType postGisSchema = GeoToolsUtil.cloneFeatureType(shpSchema, tableName);
		postGisStore.createSchema(postGisSchema);
		SimpleFeatureStore postGisFeatureStore = (SimpleFeatureStore) postGisStore.getFeatureSource(tableName);

		// Commit the features to the data store
		Transaction transaction = new DefaultTransaction();
		try {
			// Get the features from the shapefile and add to the PostGIS store
			SimpleFeatureCollection features = (SimpleFeatureCollection) shpFeatureSource.getFeatures();
			postGisFeatureStore.addFeatures(features);

			// Commit the changes and clean up
			transaction.commit();
			transaction.close();
		} catch (IOException exception) {
			// Clean up resources
			transaction.rollback();
			transaction.close();
			System.out.println("Error copying shapefile to PostGIS: " + exception.getMessage());

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
	public String findShapeFileName(String directoryPath) throws Exception {
		File[] files = new File(directoryPath).listFiles();
		for (int index = 0; index < files.length; index++) {
			String fileName = files[index].getName();
			if (fileName.toLowerCase().endsWith("." + "shp"))
				return fileName;
		}

		throw new Exception("No shape file was found inside unzipped directory: " + directoryPath);
	}
}
