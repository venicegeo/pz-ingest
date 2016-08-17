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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;

import ingest.persist.PersistMetadata;
import model.data.DataResource;
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

	/**
	 * Inspects the DataResource passed into the Piazza system.
	 * 
	 * @param dataResource
	 *            The Data resource to be ingested
	 * @param host
	 *            True if Piazza should host the resource, false if not
	 */
	public void inspect(DataResource dataResource, boolean host) throws Exception {
		// Inspect the resource based on the type it is, and add any metadata if
		// possible. If hosted, the Inspector will handle this as well.
		try {
			InspectorType inspector = getInspector(dataResource);
			dataResource = inspector.inspect(dataResource, host);
		} catch (Exception exception) {
			// If any errors occur during inspection.
			System.out.println("Error Inspecting Data: " + exception.getMessage());
			throw exception;
		}

		// Store the metadata in the Resources collection
		try {
			metadataPersist.insertData(dataResource);
		} catch (MongoException exception) {
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
	private InspectorType getInspector(DataResource dataResource) throws Exception {

		String dataResourceType = dataResource.getDataType().getClass().getSimpleName();

		if (dataResourceType.equals((new ShapefileDataType()).getClass().getSimpleName())) {
			return shapefileInspector;
		}
		if (dataResourceType.equals((new WfsDataType()).getClass().getSimpleName())) {
			return wfsInspector;
		}
		if (dataResourceType.equals((new RasterDataType()).getClass().getSimpleName())) {
			return geotiffInspector;
		}
		if (dataResourceType.equals((new PointCloudDataType()).getClass().getSimpleName())) {
			return pointCloudInspector;
		}
		if (dataResourceType.equals((new GeoJsonDataType()).getClass().getSimpleName())) {
			return geoJsonInspector;
		}
		if (dataResourceType.equals((new TextDataType()).getClass().getSimpleName())) {
			return textInspector;
		}
		throw new Exception("An Inspector was not found for the following data type: " + dataResourceType);
	}
}
