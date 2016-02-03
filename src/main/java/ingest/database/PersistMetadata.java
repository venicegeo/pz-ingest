package ingest.database;

import java.net.UnknownHostException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import model.data.DataResource;

import org.mongojack.JacksonDBCollection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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
	@Value("${mongo.host}")
	private String DATABASE_HOST;
	@Value("${mongo.port}")
	private int DATABASE_PORT;
	@Value("${mongo.db.name}")
	private String DATABASE_NAME;
	@Value("${mongo.db.collection.name}")
	private String RESOURCE_COLLECTION_NAME;
	private MongoClient mongoClient;

	public PersistMetadata() {
	}

	@PostConstruct
	private void initialize() {
		try {
			mongoClient = new MongoClient(DATABASE_HOST, DATABASE_PORT);
		} catch (UnknownHostException exception) {
			System.out.println("Error connecting to MongoDB Instance.");
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