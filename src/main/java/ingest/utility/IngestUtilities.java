package ingest.utility;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
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

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;

import model.data.DataResource;
import model.data.FileRepresentation;
import model.data.location.FileAccessFactory;
import model.data.location.FileLocation;
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
	
	@Value("${vcap.services.pz-blobstore.credentials.access:}")
	private String AMAZONS3_ACCESS_KEY;
	@Value("${vcap.services.pz-blobstore.credentials.private:}")
	private String AMAZONS3_PRIVATE_KEY;
	@Value("${vcap.services.pz-blobstore.credentials.bucket}")
	private String AMAZONS3_BUCKET_NAME;
	
	private AmazonS3 s3Client;
	
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
	 * Will copy external AWS S3 file to piazza S3 Bucket 
	 *  
	 * @param dataResource
	 * @param host if piazza should host the data
	 * @throws Exception 
	 */
	public void copyS3Source(DataResource dataResource) throws Exception{

			// Connect to AWS S3 Bucket. Apply security only if credentials are present
			if ((AMAZONS3_ACCESS_KEY.isEmpty()) && (AMAZONS3_PRIVATE_KEY.isEmpty())) {
				s3Client = new AmazonS3Client();
			} else {
				BasicAWSCredentials credentials = new BasicAWSCredentials(AMAZONS3_ACCESS_KEY, AMAZONS3_PRIVATE_KEY);
				s3Client = new AmazonS3Client(credentials);
			}

			// Obtain file input stream
			FileLocation fileLocation = ((FileRepresentation)dataResource.getDataType()).getLocation();
			FileAccessFactory fileFactory = new FileAccessFactory(AMAZONS3_ACCESS_KEY,AMAZONS3_PRIVATE_KEY);
			InputStream inputStream = fileFactory.getFile(fileLocation);

			// Write stream directly into an s3 bucket
			ObjectMetadata metadata = new ObjectMetadata();
			String fileKey = String.format("%s-%s", dataResource.getDataId(), fileLocation.getFileName());
			s3Client.putObject(AMAZONS3_BUCKET_NAME, fileKey, inputStream, metadata);
			
			// Clean up
			inputStream.close();
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
