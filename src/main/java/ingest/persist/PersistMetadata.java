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

import org.mongojack.JacksonDBCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import util.PiazzaLogger;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 * Helper class to interact with and access the Mongo instance, which handles
 * storing the DataResource information. This contains the metadata on the
 * ingested data, the locations, URLs and paths, and other important metadata.
 * This is not the data itself. Storing of the spatial data is handled via
 * PostGIS, S3, or other stores.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class PersistMetadata {
	@Autowired
	private PiazzaLogger logger;
	@Value("${mongo.host}")
	private String DATABASE_HOST;
	@Value("${mongo.port}")
	private int DATABASE_PORT;
	@Value("${mongo.db.name}")
	private String DATABASE_NAME;
	@Value("${mongo.db.collection.name}")
	private String RESOURCE_COLLECTION_NAME;
	private MongoClient mongoClient;

	/**
	 * Required for Component init
	 */
	public PersistMetadata() {
	}

	@PostConstruct
	private void initialize() {
		try {
			mongoClient = new MongoClient(DATABASE_HOST, DATABASE_PORT);
		} catch (UnknownHostException exception) {
			logger.log(String.format("Error Connecting to MongoDB Instance: %s", exception.getMessage()),
					PiazzaLogger.FATAL);
			exception.printStackTrace();
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
}