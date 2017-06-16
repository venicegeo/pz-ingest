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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;

import exception.DataInspectException;
import exception.InvalidInputException;
import ingest.persist.PersistMetadata;
import model.data.DataResource;
import model.data.DataType;
import model.data.type.GeoJsonDataType;
import model.data.type.PointCloudDataType;
import model.data.type.RasterDataType;
import model.data.type.ShapefileDataType;
import model.data.type.TextDataType;
import model.data.type.WfsDataType;

/**
 * Inspects the incoming data in Job Request for information for the Ingest. Capable of inspecting files, or URLs.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class Inspector {
	@Autowired
	private PersistMetadata metadataPersist;
	@Autowired
	private ShapefileInspector shapefileInspector;
	@Autowired
	private TextInspector textInspector;
	@Autowired
	private WfsInspector wfsInspector;
	@Autowired
	private GeoTiffInspector geotiffInspector;
	@Autowired
	private PointCloudInspector pointCloudInspector;
	@Autowired
	private GeoJsonInspector geoJsonInspector;

	private final static Logger LOGGER = LoggerFactory.getLogger(Inspector.class);

	/**
	 * Inspects the DataResource passed into the Piazza system.
	 * 
	 * @param dataResource
	 *            The Data resource to be ingested
	 * @param host
	 *            True if Piazza should host the resource, false if not
	 */
	public void inspect(DataResource dataResource, boolean host) throws DataInspectException, InterruptedException {
		
		final DataResource finalDataResource;
		
		// Inspect the resource based on the type it is, and add any metadata if
		// possible. If hosted, the Inspector will handle this as well.
		try {
			InspectorType inspector = getInspector(dataResource);
			finalDataResource = inspector.inspect(dataResource, host);
		} catch (Exception exception) {
			// If any errors occur during inspection.
			String error = "Error Inspecting Data: " + exception.getMessage();
			LOGGER.error(error, exception);
			throw new DataInspectException(exception.getMessage());
		}

		// Store the metadata in the Resources collection
		try {
			metadataPersist.insertData(finalDataResource);
		} catch (MongoException exception) {
			LOGGER.error("Error Loading Data into Mongo.", exception);
			if (exception instanceof MongoInterruptedException) {
				throw new InterruptedException();
			} else {
				throw exception;
			}
		}
	}

	/**
	 * Small factory method that returns the InspectorType that is applicable for the DataResource based on the type of
	 * data it is. For a data format like a Shapefile, GeoTIFF, or External WFS to be parsed, an Inspector must be
	 * defined to do that work here.
	 * 
	 * @param dataResource
	 *            The Data to inspect
	 * @return The inspector capable of inspecting the data
	 */
	private InspectorType getInspector(DataResource dataResource) throws InvalidInputException {

		DataType dataType = dataResource.getDataType();
		if (dataType instanceof ShapefileDataType) {
			return shapefileInspector;
		}
		if (dataType instanceof WfsDataType) {
			return wfsInspector;
		}
		if (dataType instanceof RasterDataType) {
			return geotiffInspector;
		}
		if (dataType instanceof PointCloudDataType) {
			return pointCloudInspector;
		}
		if (dataType instanceof GeoJsonDataType) {
			return geoJsonInspector;
		}
		if (dataType instanceof TextDataType) {
			return textInspector;
		}

		throw new InvalidInputException("An Inspector was not found for the following data type: " + dataType.getClass().getSimpleName());
	}
}
