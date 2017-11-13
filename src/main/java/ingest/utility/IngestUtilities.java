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
package ingest.utility;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FilenameUtils;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureSource;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.vividsolutions.jts.geom.Envelope;

import exception.InvalidInputException;
import model.data.DataResource;
import model.data.DataType;
import model.data.FileRepresentation;
import model.data.location.FileAccessFactory;
import model.data.location.FileLocation;
import model.data.location.S3FileStore;
import model.data.type.GeoJsonDataType;
import model.data.type.ShapefileDataType;
import model.job.metadata.SpatialMetadata;
import model.logger.AuditElement;
import model.logger.Severity;
import model.response.ErrorResponse;
import model.response.PiazzaResponse;
import util.GeoToolsUtil;
import util.PiazzaLogger;

/**
 * Utility component for some generic processing efforts.
 * 
 * @author Sonny.Saniev
 * 
 */
@Component
public class IngestUtilities {
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private RestTemplate restTemplate;

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

	@Value("${vcap.services.pz-blobstore.credentials.access_key_id}")
	private String AMAZONS3_ACCESS_KEY;
	@Value("${vcap.services.pz-blobstore.credentials.secret_access_key}")
	private String AMAZONS3_PRIVATE_KEY;
	@Value("${vcap.services.pz-blobstore.credentials.bucket}")
	private String AMAZONS3_BUCKET_NAME;
	@Value("${vcap.services.pz-blobstore.credentials.encryption_key}")
	private String S3_KMS_CMK_ID;
	@Value("${s3.use.kms}")
	private Boolean USE_KMS;

	private static final Logger LOG = LoggerFactory.getLogger(IngestUtilities.class);
	private static final String INGEST = "ingest";

	/**
	 * Recursive deletion of directory
	 * 
	 * @param File
	 *            Directory to be deleted
	 * 
	 * @return boolean if successful
	 * @throws IOException
	 */
	public boolean deleteDirectoryRecursive(File directory) throws IOException {
		boolean result = false;

		if (directory.isDirectory()) {
			File[] files = directory.listFiles();

			for (int i = 0; i < files.length; i++) {
				if (files[i].isDirectory()) {
					deleteDirectoryRecursive(files[i]);
				}

				if (!files[i].delete())
					throw new IOException("Unable to delete file " + files[i].getName() + " from " + directory.getAbsolutePath());
			}

			result = directory.delete();
		}

		return result;
	}

	/**
	 * Unzip the given zip into output directory. This is only applicable for SHAPEFILES as it includes black/whitelist
	 * for preventing malicious inputs.
	 * 
	 * @param zipPath
	 *            Zip file full path
	 * @param extractPath
	 *            Extracted zip output directory
	 * 
	 * @return boolean if successful
	 * @throws IOException
	 */
	public void extractZip(String zipPath, String extractPath) throws IOException {
		try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipPath))) {
			// Create output directory
			File directory = new File(extractPath);
			if (!directory.exists()) {
				directory.mkdir();
			}

			// Get initial file list entry
			ZipEntry zipEntry = zipInputStream.getNextEntry();
			while (zipEntry != null) {
				extractZipEntry(zipEntry.getName(), zipInputStream, zipPath, extractPath);
				
				zipEntry = zipInputStream.getNextEntry();
			}

			zipInputStream.closeEntry();
		} 
		catch (IOException ex) {
			String error = "Unable to extract zip: " + zipPath + " to path " + extractPath;
			LOG.error(error, ex, new AuditElement(INGEST, "failedExtractShapefileZip", extractPath));
			throw new IOException(error);
		}
	}

	private void extractZipEntry(final String fileName, final ZipInputStream zipInputStream, final String zipPath, final String extractPath) throws IOException {
		byte[] buffer = new byte[1024];

		String extension = FilenameUtils.getExtension(fileName);
		String filePath = String.format("%s%s%s.%s", extractPath, File.separator, "ShapefileData", extension);

		// Sanitize - blacklist
		if (filePath.contains("..") || (fileName.contains("/")) || (fileName.contains("\\"))) {
			logger.log(
					String.format(
							"Cannot extract Zip entry %s because it contains a restricted path reference. Characters such as '..' or slashes are disallowed. The initial zip path was %s. This was blocked to prevent a vulnerability.",
							fileName, zipPath),
					Severity.WARNING, new AuditElement(INGEST, "restrictedPathDetected", zipPath));
			zipInputStream.closeEntry();
			return;
		}
		// Sanitize - whitelist
		boolean filePathContainsExtension = false;
		for( final String ext : Arrays.asList(".shp", ".prj", ".shx", ".dbf", ".sbn")) { 
			if( filePath.contains(ext) ) {
				filePathContainsExtension = true;
				break;
			}
		}
		
		if (filePathContainsExtension) {
			File newFile = new File(filePath).getCanonicalFile();

			// Create all non existing folders
			new File(newFile.getParent()).mkdirs();

			try (FileOutputStream outputStream = new FileOutputStream(newFile)) {
				int length;
				while ((length = zipInputStream.read(buffer)) > 0) {
					outputStream.write(buffer, 0, length);
				}
			}

			zipInputStream.closeEntry();
		} 
		else {
			zipInputStream.closeEntry();
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
	 * Loads the contents of a DataResource into the PostGIS Database.
	 * 
	 * @param featureSource
	 *            The GeoTools FeatureSource for the ingest information.
	 * @param dataResource
	 *            The DataResource object with FeatureSource metadata
	 * @throws IOException
	 */
	public void persistFeatures(FeatureSource<SimpleFeatureType, SimpleFeature> featureSource, DataResource dataResource,
			SimpleFeatureType featureSchema) throws IOException {
		// Get the dataStore to the postGIS database.
		DataStore postGisStore = GeoToolsUtil.getPostGisDataStore(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_SCHEMA, POSTGRES_DB_NAME,
				POSTGRES_USER, POSTGRES_PASSWORD);

		// Create the schema in the data store
		String tableName = dataResource.getDataId();

		// Associate the table name with the DataResource
		SimpleFeatureType postGisSchema = GeoToolsUtil.cloneFeatureType(featureSchema, tableName);
		postGisStore.createSchema(postGisSchema);
		SimpleFeatureStore postGisFeatureStore = (SimpleFeatureStore) postGisStore.getFeatureSource(tableName);

		// Commit the features to the data store
		try (Transaction transaction = new DefaultTransaction()) {

			// Get the features from the FeatureCollection and add to the PostGIS store
			SimpleFeatureCollection features = (SimpleFeatureCollection) featureSource.getFeatures();
			postGisFeatureStore.addFeatures(features);
			
			transaction.commit();

		} catch (IOException exception) {
			// Clean up resources

			String error = "Error copying DataResource to PostGIS: " + exception.getMessage();
			LOG.error(error, exception);
			logger.log(error, Severity.ERROR, new AuditElement(INGEST, "failedToCopyPostGisData", dataResource.getDataId()));

			// Rethrow
			throw exception;
		} finally {

			// Cleanup Data Store
			postGisStore.dispose();
		}

		logger.log("Committed Data to PostGIS.", Severity.INFORMATIONAL, new AuditElement(INGEST, "loadDataToPostGis", tableName));
	}

	/**
	 * Will copy external AWS S3 file to piazza S3 Bucket
	 * 
	 * @param dataResource
	 * @param host
	 *            if piazza should host the data
	 */
	public void copyS3Source(DataResource dataResource) throws InvalidInputException, IOException {
		logger.log(String.format("Copying Data %s to Piazza S3 Location.", dataResource.getDataId()), Severity.INFORMATIONAL,
				new AuditElement(INGEST, "copyS3DataToPiazza", dataResource.getDataId()));
		// Obtain file input stream
		FileLocation fileLocation = ((FileRepresentation) dataResource.getDataType()).getLocation();
		FileAccessFactory fileFactory = getFileFactoryForDataResource(dataResource);
		InputStream inputStream = fileFactory.getFile(fileLocation);

		// Write stream directly into the Piazza S3 bucket
		AmazonS3 s3Client = getAwsClient(USE_KMS.booleanValue());
		ObjectMetadata metadata = new ObjectMetadata();
		String fileKey = String.format("%s-%s", dataResource.getDataId(), fileLocation.getFileName());
		s3Client.putObject(AMAZONS3_BUCKET_NAME, fileKey, inputStream, metadata);

		// Clean up
		inputStream.close();
	}

	/**
	 * Returns an instance of the File Factory, instantiated with the correct credentials for the use of obtaining file
	 * bytes for the specified Data Resource. Such as if the Resource is a file, or an S3 Bucket, or an Encrypted S3
	 * bucket.
	 * 
	 * @param dataResource
	 *            The Data Resource
	 * @return FileAccessFactory
	 */
	public FileAccessFactory getFileFactoryForDataResource(DataResource dataResource) {
		// If S3 store, determine if this is the Piazza bucket (use encryption) or not (dont use encryption)
		final FileAccessFactory fileFactory;
		FileLocation fileLocation = ((FileRepresentation) dataResource.getDataType()).getLocation();
		if (fileLocation instanceof S3FileStore) {
			if (AMAZONS3_BUCKET_NAME.equals(((S3FileStore) fileLocation).getBucketName())) {
				// Piazza bucket. If KMS Encryption is enabled, then use it.
				if (USE_KMS.booleanValue()) {
					fileFactory = new FileAccessFactory(AMAZONS3_ACCESS_KEY, AMAZONS3_PRIVATE_KEY, S3_KMS_CMK_ID);
				} else {
					fileFactory = new FileAccessFactory(AMAZONS3_ACCESS_KEY, AMAZONS3_PRIVATE_KEY);
				}
			} else {
				fileFactory = new FileAccessFactory(AMAZONS3_ACCESS_KEY, AMAZONS3_PRIVATE_KEY);
			}
		} else {
			// No AWS Creds needed
			fileFactory = new FileAccessFactory();
		}
		return fileFactory;
	}

	public long getFileSize(DataResource dataResource) throws AmazonClientException, InvalidInputException, IOException {
		// Obtain file input stream
		FileLocation fileLocation = ((FileRepresentation) dataResource.getDataType()).getLocation();
		FileAccessFactory fileFactory = getFileFactoryForDataResource(dataResource);
		InputStream inputStream = fileFactory.getFile(fileLocation);

		long counter = inputStream.read();
		long numBytes = 0;
		while (counter >= 0) {
			counter = inputStream.read();
			numBytes++;
		}

		return numBytes;
	}

	/**
	 * Gets an instance of an S3 client to use.
	 * 
	 * @param useEncryption
	 *            True if encryption should be used (only for Piazza Bucket). For all external Buckets, encryption is
	 *            not used.
	 * 
	 * @return The S3 client
	 */
	public AmazonS3 getAwsClient(boolean useEncryption) {
		AmazonS3 s3Client;
		if ((AMAZONS3_ACCESS_KEY.isEmpty()) && (AMAZONS3_PRIVATE_KEY.isEmpty())) {
			s3Client = new AmazonS3Client();
		} else {
			BasicAWSCredentials credentials = new BasicAWSCredentials(AMAZONS3_ACCESS_KEY, AMAZONS3_PRIVATE_KEY);
			// Set up encryption using the KMS CMK Key
			if (useEncryption) {
				KMSEncryptionMaterialsProvider materialProvider = new KMSEncryptionMaterialsProvider(S3_KMS_CMK_ID);
				s3Client = new AmazonS3EncryptionClient(credentials, materialProvider,
						new CryptoConfiguration().withKmsRegion(Regions.US_EAST_1)).withRegion(Region.getRegion(Regions.US_EAST_1));
			} else {
				s3Client = new AmazonS3Client(credentials);
			}
		}
		return s3Client;
	}

	/**
	 * Searches directory file list for the first shape file and returns the name (non-recursive)
	 * 
	 * @param directoryPath
	 *            Folder path to search
	 * 
	 * @return shape file name found in the directory
	 */
	public String findShapeFileName(String directoryPath) throws IOException {
		File[] files = new File(directoryPath).listFiles();
		for (int index = 0; index < files.length; index++) {
			String fileName = files[index].getName();
			if (fileName.toLowerCase(Locale.ROOT).endsWith(".shp"))
				return fileName;
		}

		throw new IOException("No shape file was found inside unzipped directory: " + directoryPath);
	}

	/**
	 * Deletes all persistent files for a Data Resource item. This will not remove the entry from DB. That is
	 * handled separately.
	 * 
	 * <p>
	 * This will delete files from S3 for rasters/vectors, and for vectors this will also delete the PostGIS table.
	 * </p>
	 * 
	 * @param dataResource
	 */
	public void deleteDataResourceFiles(DataResource dataResource) throws IOException {
		// If the DataResource has PostGIS tables to clean
		DataType dataType = dataResource.getDataType();
		if (dataType instanceof ShapefileDataType) {
			deleteDatabaseTable(((ShapefileDataType) dataType).getDatabaseTableName());
		} else if (dataType instanceof GeoJsonDataType) {
			deleteDatabaseTable(((GeoJsonDataType) dataType).databaseTableName);
		}
		// If the Data Resource has S3 files to clean
		if (dataType instanceof S3FileStore) {
			S3FileStore fileStore = (S3FileStore) dataType;
			if (fileStore.getBucketName().equals(AMAZONS3_BUCKET_NAME)) {
				// Held by Piazza S3. Delete the data.
				AmazonS3 client = getAwsClient(USE_KMS.booleanValue());
				client.deleteObject(fileStore.getBucketName(), fileStore.getFileName());
			}
		}
	}

	/**
	 * Based on the input Spatial metadata in native projection, this will reproject to EPSG:4326 and return a newly
	 * created SpatialMetadata object with WGS84/EPSG:4326 projection information for that native bounding box.
	 * 
	 * @param spatialMetadata
	 *            The native bounding box information
	 * @return Projected (WGS84) bounding box information
	 */
	public SpatialMetadata getProjectedSpatialMetadata(SpatialMetadata spatialMetadata)
			throws NoSuchAuthorityCodeException, FactoryException, TransformException {
		// Coordinate system inputs to transform
		CoordinateReferenceSystem sourceCrs = CRS.decode(String.format("EPSG:%s", spatialMetadata.getEpsgCode()));
		CoordinateReferenceSystem targetCrs = CRS.decode("EPSG:4326");
		MathTransform transform = CRS.findMathTransform(sourceCrs, targetCrs);

		// Build the bounding box geometry to reproject with the transform
		ReferencedEnvelope envelope = new ReferencedEnvelope(spatialMetadata.getMinX(), spatialMetadata.getMaxX(),
				spatialMetadata.getMinY(), spatialMetadata.getMaxY(), sourceCrs);

		// Project
		Envelope projectedEnvelope = JTS.transform(envelope, transform);

		// Create a new container and add the information
		SpatialMetadata projected = new SpatialMetadata();
		projected.setMinX(projectedEnvelope.getMinX());
		projected.setMinY(projectedEnvelope.getMinY());
		projected.setMaxX(projectedEnvelope.getMaxX());
		projected.setMaxY(projectedEnvelope.getMaxY());
		projected.setEpsgCode(4326);

		return projected;
	}

	/**
	 * Drops a table, by name, in the Piazza database.
	 * 
	 * @param tableName
	 *            The name of the table to drop.
	 */
	public void deleteDatabaseTable(String tableName) throws IOException {
		logger.log("Dropping Table from PostGIS", Severity.INFORMATIONAL, new AuditElement(INGEST, "deletePostGisTable", tableName));

		// Delete the table
		DataStore postGisStore = GeoToolsUtil.getPostGisDataStore(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_SCHEMA, POSTGRES_DB_NAME,
				POSTGRES_USER, POSTGRES_PASSWORD);
		try {
			postGisStore.removeSchema(tableName);
		} catch (IllegalArgumentException exception) {
			// If this exception is triggered, then its likely the database
			// table was already deleted. Log that.
			String error = String.format(
					"Attempted to delete Table %s from Database for deleting a Data Resource, but the table was not found.", tableName);
			LOG.error(error, exception);
			logger.log(error, Severity.WARNING);
		} finally {
			postGisStore.dispose();
		}
	}

	/**
	 * Private method to post requests to elastic search for deleting the service metadata.
	 * 
	 * @param DataResource
	 *            Data object
	 * @param url
	 *            Elasticsearch endpoint for deletion
	 * @return PiazzaResponse response
	 */
	public PiazzaResponse deleteElasticsearchByDataResource(DataResource dataResource, String url) {
		try {
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<DataResource> entity = new HttpEntity<DataResource>(dataResource, headers);
			return restTemplate.postForObject(url, entity, PiazzaResponse.class);
		} catch (HttpClientErrorException | HttpServerErrorException exception) {
			String error = String.format("Could not delete DataResource ID: %s from Elasticsearch. %s StatusCode: %s",
					dataResource.getDataId(), exception.getResponseBodyAsString(), exception.getStatusCode());
			LOG.error(error, exception);
			logger.log(error, Severity.ERROR);
			return new ErrorResponse(error, "Loader");
		} catch (Exception exception) {
			String error = String.format("Could not delete DataResource id: %s from Elasticsearch. %s", dataResource.getDataId(),
					exception.getMessage());
			LOG.error(error, exception);
			logger.log(error, Severity.ERROR);
			return new ErrorResponse(error, "Loader");
		}
	}
	
	/**
	 * Private method to post requests to elastic search for updating the DataResource
	 * 
	 * @param DataResource
	 *            Data object
	 * @param url
	 *            Elasticsearch endpoint for update
	 * @return PiazzaResponse response
	 */
	public PiazzaResponse updateDataResourceInElasticsearch(DataResource dataResource, String url) {
		try {
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<DataResource> entity = new HttpEntity<DataResource>(dataResource, headers);
			return restTemplate.postForObject(url, entity, PiazzaResponse.class);
		} catch (HttpClientErrorException | HttpServerErrorException exception) {
			String error = String.format("Could not upadte DataResource ID: %s in Elasticsearch. %s StatusCode: %s",
					dataResource.getDataId(), exception.getResponseBodyAsString(), exception.getStatusCode());
			LOG.error(error, exception);
			logger.log(error, Severity.ERROR);
			return new ErrorResponse(error, "Loader");
		} catch (Exception exception) {
			String error = String.format("Could not update DataResource id: %s in Elasticsearch. %s", dataResource.getDataId(),
					exception.getMessage());
			LOG.error(error, exception);
			logger.log(error, Severity.ERROR);
			return new ErrorResponse(error, "Loader");
		}
	}
}
