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

import ingest.persist.PersistMetadata;
import model.data.DataResource;
import model.data.type.PointCloudResource;
import model.data.type.PostGISResource;
import model.data.type.RasterResource;
import model.data.type.ShapefileResource;
import model.data.type.TextResource;
import model.data.type.WfsResource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Inspects the incoming data in Job Request for information for the Ingest.
 * Capable of inspecting files, or URLs.
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
		metadataPersist.insertData(dataResource);
	}

	/**
	 * Small factory method that returns the InspectorType that is applicable
	 * for the DataResource based on the type of data it is. For a data format
	 * like a Shapefile, GeoTIFF, or External WFS to be parsed, an Inspector
	 * must be defined to do that work here.
	 * 
	 * @param dataResource
	 *            The Data to inspect
	 * @return The inspector capable of inspecting the data
	 */
	private InspectorType getInspector(DataResource dataResource) throws Exception {
		switch (dataResource.getDataType().getType()) {
		case ShapefileResource.type:
			return shapefileInspector;
		case TextResource.type:
			return textInspector;
		case WfsResource.type:
			return wfsInspector;
		case RasterResource.type:
			return geotiffInspector;
		case PointCloudResource.type:
			return pointCloudInspector;
		case PostGISResource.type:
		}
		throw new Exception("An Inspector was not found for the following data type: "
				+ dataResource.getDataType().getType());
	}
}
