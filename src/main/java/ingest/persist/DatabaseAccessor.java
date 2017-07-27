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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.venice.piazza.common.hibernate.dao.DataResourceDao;
import org.venice.piazza.common.hibernate.entity.DataResourceEntity;

import exception.InvalidInputException;
import model.data.DataResource;
import model.job.metadata.ResourceMetadata;
import util.PiazzaLogger;

/**
 * Helper class to interact with PostgreSQL, which handles storing the DataResource information. This
 * contains the metadata on the ingested data, the locations, URLs and paths, and other important metadata. This is not
 * the data itself. Storing of the spatial data is handled via PostGIS, S3, or other stores.
 * 
 * @author Sonny.Saniev
 * 
 */
@Component
public class DatabaseAccessor {
	@Autowired
	private PiazzaLogger logger;

	@Autowired
	DataResourceDao dataResourceDao;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseAccessor.class);
	private static final String DATAID = "dataId";
	
	public DatabaseAccessor() {
		// Expected for Component instantiation
	}

	/**
	 * Deletes the item from the database matching the Data Id.
	 * 
	 * @param dataId
	 *            The Data Id to delete.
	 */
	public void deleteDataEntry(String dataId) {
		dataResourceDao.deleteRecord(dataId);
	}

	/**
	 * Inserts the DataResource into the database
	 * 
	 * @param dataResource
	 *            The data to insert.
	 */
	public void insertData(DataResource dataResource) {
		DataResourceEntity record = new DataResourceEntity();
		record.setDataResource(dataResource);
		dataResourceDao.save(record);
	}

	/**
	 * Gets the DataResource from the database DataResource by Id. This Id is typically what will be returned to the user
	 * as the result of their Job.
	 * 
	 * @param dataId
	 *            The Id of the DataResource
	 * @return DataResource object or null
	 */
	public DataResource getData(String dataId) {
		DataResourceEntity record = dataResourceDao.fineOneRecord(dataId);
		if(record != null)
		{
			return record.getDataResource();
		}
		return null;
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
		DataResourceEntity record = dataResourceDao.fineOneRecord(dataId);
		if (record == null) {
			throw new InvalidInputException(String.format("No Data Resource found matching Id %s", dataId));
		}

		// Merge the ResourceMetadata together
		record.getDataResource().getMetadata().merge(metadata, false);
		
		// Update the DataResource in the database
		dataResourceDao.save(record);
	}
}