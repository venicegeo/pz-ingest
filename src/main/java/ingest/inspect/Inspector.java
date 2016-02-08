package ingest.inspect;

import ingest.database.PersistMetadata;
import model.data.DataResource;
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

	/**
	 * Inspects the DataResource passed into the Piazza system.
	 * 
	 * @param dataResource
	 *            The Data resource to be ingested
	 * @param host
	 *            True if Piazza should host the resource, false if not
	 */
	public void inspect(DataResource dataResource, boolean host) {
		// Inspect the resource based on the type it is, and add any metadata if
		// possible.
		try {
			InspectorType inspector = getInspector(dataResource);
			dataResource = inspector.inspect(dataResource);
		} catch (Exception exception) {
			// If it could not be inspected, then the existing metadata is the
			// only thing that will be entered into the system.
			exception.printStackTrace();
		}

		// Store the metadata in the Resources collection
		metadataPersist.insertData(dataResource);

		// Persist any spatial/file data if necessary
		// TODO: 
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
			return new ShapefileInspector();
		case TextResource.type:
			return new TextInspector();
		case WfsResource.type:
			return new WfsInspector();
		}
		throw new Exception("An Inspector was not found for the following data type: "
				+ dataResource.getDataType().getType());
	}
}
