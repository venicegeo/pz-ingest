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
package ingest.persist;

import java.util.Arrays;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.mongojack.DBQuery;
import org.mongojack.DBUpdate;
import org.mongojack.JacksonDBCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;

import exception.InvalidInputException;
import model.data.DataResource;
import model.job.metadata.ResourceMetadata;
import util.PiazzaLogger;

/**
 * Helper class to interact with and access the Mongo instance, which handles storing the DataResource information. This
 * contains the metadata on the ingested data, the locations, URLs and paths, and other important metadata. This is not
 * the data itself. Storing of the spatial data is handled via PostGIS, S3, or other stores.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class PersistMetadata {
	@Autowired
	private PiazzaLogger logger;
	@Value("${vcap.services.pz-mongodb.credentials.database}")
	private String DATABASE_NAME;
	@Value("${vcap.services.pz-mongodb.credentials.host}")
	private String DATABASE_HOST;
	@Value("${vcap.services.pz-mongodb.credentials.port}")
	private int DATABASE_PORT;
	@Value("${vcap.services.pz-mongodb.credentials.username:}")
	private String DATABASE_USERNAME;
	@Value("${vcap.services.pz-mongodb.credentials.password:}")
	private String DATABASE_CREDENTIAL;
	@Value("${mongo.db.collection.name}")
	private String RESOURCE_COLLECTION_NAME;
	private MongoClient mongoClient;
	@Value("${mongo.thread.multiplier}")
	private int mongoThreadMultiplier;

	@Autowired
	private Environment environment;
	private static final Logger LOG = LoggerFactory.getLogger(PersistMetadata.class);
	private static final String DATAID = "dataId";
	
	public PersistMetadata() {
		// Expected for Component instantiation
	}

	@PostConstruct
	private void initialize() {
		try {
			MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
			// Enable SSL if the `mongossl` Profile is enabled
			if (Arrays.stream(environment.getActiveProfiles()).anyMatch(env -> "mongossl".equalsIgnoreCase(env))) {
				builder.sslEnabled(true);
				builder.sslInvalidHostNameAllowed(true);
			}
			// If a username and password are provided, then associate these credentials with the connection
			if ((!StringUtils.isEmpty(DATABASE_USERNAME)) && (!StringUtils.isEmpty(DATABASE_CREDENTIAL))) {
				mongoClient = new MongoClient(new ServerAddress(DATABASE_HOST, DATABASE_PORT),
						Arrays.asList(
								MongoCredential.createCredential(DATABASE_USERNAME, DATABASE_NAME, DATABASE_CREDENTIAL.toCharArray())),
						builder.threadsAllowedToBlockForConnectionMultiplier(mongoThreadMultiplier).build());
			} else {
				mongoClient = new MongoClient(new ServerAddress(DATABASE_HOST, DATABASE_PORT),
						builder.threadsAllowedToBlockForConnectionMultiplier(mongoThreadMultiplier).build());
			}

		} catch (Exception exception) {
			LOG.error(String.format("Error connecting to MongoDB Instance. %s", exception.getMessage()), exception);

		}
	}

	@PreDestroy
	private void close() {
		mongoClient.close();
	}

	/**
	 * Gets a reference to the MongoDB Client Object.
	 * 
	 * @return
	 */
	public MongoClient getClient() {
		return mongoClient;
	}

	/**
	 * Gets the Mongo Collection of all data currently referenced within Piazza.
	 * 
	 * @return Mongo collection for DataResources
	 */
	public JacksonDBCollection<DataResource, String> getResourceCollection() {
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(RESOURCE_COLLECTION_NAME);
		return JacksonDBCollection.wrap(collection, DataResource.class, String.class);
	}

	/**
	 * Deletes the item from the database matching the Data Id.
	 * 
	 * @param dataId
	 *            The Data Id to delete.
	 */
	public void deleteDataEntry(String dataId) {
		BasicDBObject query = new BasicDBObject(DATAID, dataId);
		getResourceCollection().remove(query);
	}

	/**
	 * Inserts the DataResource into the Mongo Resource collection.
	 * 
	 * @param dataResource
	 *            The data to insert.
	 * @throws MongoException
	 *             Error while inserting into database
	 */
	public void insertData(DataResource dataResource) throws MongoException {
		getResourceCollection().insert(dataResource);
	}

	/**
	 * Gets the DataResource from the Resources collection by Id. This Id is typically what will be returned to the user
	 * as the result of their Job.
	 * 
	 * @param dataId
	 *            The Id of the DataResource
	 * @return DataResource object
	 */
	public DataResource getData(String dataId) {
		BasicDBObject query = new BasicDBObject(DATAID, dataId);
		DataResource data;

		try {
			if ((data = getResourceCollection().findOne(query)) == null) {
				return null;
			}
		} catch (MongoTimeoutException mte) {
			String error = "MongoDB instance not available.";
			LOG.error(error, mte);
			throw new MongoException(error);
		}

		return data;
	}

	/**
	 * Updates the Metadata for the Data Resource object.
	 * 
	 * @param dataId
	 *            The Data Id of the resource to update
	 * @param metadata
	 *            The metadata to update with
	 */
	public void updateMetadata(String dataId, ResourceMetadata metadata) throws InvalidInputException {
		// Get the Data Resource
		DataResource dataResource = getData(dataId);
		if (dataResource == null) {
			throw new InvalidInputException(String.format("No Data Resource found matching Id %s", dataId));
		}
		// Merge the ResourceMetadata together
		dataResource.getMetadata().merge(metadata, false);
		// Update the DataResource in the database
		getResourceCollection().update(DBQuery.is(DATAID, dataId), DBUpdate.set("metadata", dataResource.getMetadata()));
	}
}