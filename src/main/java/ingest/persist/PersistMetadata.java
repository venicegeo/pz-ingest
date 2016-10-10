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

import java.net.UnknownHostException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import model.data.DataResource;
import model.job.metadata.ResourceMetadata;

import org.mongojack.DBQuery;
import org.mongojack.DBUpdate;
import org.mongojack.JacksonDBCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import util.PiazzaLogger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;

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
	@Value("${vcap.services.pz-mongodb.credentials.uri}")
	private String DATABASE_URI;
	@Value("${vcap.services.pz-mongodb.credentials.database}")
	private String DATABASE_NAME;
	@Value("${mongo.db.collection.name}")
	private String RESOURCE_COLLECTION_NAME;
	private MongoClient mongoClient;
	@Value("${mongo.thread.multiplier}")
	private int mongoThreadMultiplier;

	private final static Logger LOGGER = LoggerFactory.getLogger(PersistMetadata.class);
	
	/**
	 * Required for Component init
	 */
	public PersistMetadata() {
	}

	@PostConstruct
	private void initialize() {
		try {
			mongoClient = new MongoClient(new MongoClientURI(DATABASE_URI + "?waitQueueMultiple=" + mongoThreadMultiplier));
		} catch (Exception exception) {
			String error = String.format("Error Connecting to MongoDB Instance: %s", exception.getMessage());
			logger.log(error, PiazzaLogger.FATAL);
			LOGGER.error(error);
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
		BasicDBObject query = new BasicDBObject("dataId", dataId);
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
		BasicDBObject query = new BasicDBObject("dataId", dataId);
		DataResource data;

		try {
			if ((data = getResourceCollection().findOne(query)) == null) {
				return null;
			}
		} catch (MongoTimeoutException mte) {
			throw new MongoException("MongoDB instance not available.");
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
	public void updateMetadata(String dataId, ResourceMetadata metadata) throws Exception {
		// Get the Data Resource
		DataResource dataResource = getData(dataId);
		if (dataResource == null) {
			throw new Exception(String.format("No Data Resource found matching Id %s", dataId));
		}
		// Merge the ResourceMetadata together
		dataResource.getMetadata().merge(metadata, false);
		// Update the DataResource in the database
		getResourceCollection().update(DBQuery.is("dataId", dataId), DBUpdate.set("metadata", dataResource.getMetadata()));
	}
}