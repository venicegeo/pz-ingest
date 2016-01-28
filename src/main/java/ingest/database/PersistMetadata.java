package ingest.database;

import java.net.UnknownHostException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import model.job.metadata.ResourceMetadata;

import org.mongojack.JacksonDBCollection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 * Helper class to interact with and access the Mongo instance.
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
	 * Gets a reference to the MongoDB's Resource Collection.
	 * 
	 * @return
	 */
	public JacksonDBCollection<ResourceMetadata, String> getResourceCollection() {
		// MongoJack does not support the latest Mongo API yet. TODO: Check if
		// they plan to.
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(RESOURCE_COLLECTION_NAME);
		return JacksonDBCollection.wrap(collection, ResourceMetadata.class, String.class);
	}

	/**
	 * Inserts the Metadata object into the resource collection.
	 * 
	 * @param metadata
	 *            The metadata information to insert.
	 */
	public void insertMetadata(ResourceMetadata metadata) throws MongoException {
		getResourceCollection().insert(metadata);
	}
}