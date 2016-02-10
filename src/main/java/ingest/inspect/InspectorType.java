package ingest.inspect;

import model.data.DataResource;

/**
 * Interface for defining an inspector that parses a resource for metadata.
 * 
 * @author Patrick.Doody
 * 
 */
public interface InspectorType {
	/**
	 * Parses a DataResource object for metadata. Since a DataResource contains
	 * references to SpatialMetadata and other ResourceMetadata (and thus may be
	 * user defined from an Ingest request), this method will populate the
	 * object with additional metadata properties as able.
	 * 
	 * @param dataResource
	 *            The Data to inspect
	 * @return The input data, with additional metadata fields populated as
	 *         discovered through this process
	 */
	public DataResource inspect(DataResource dataResource) throws Exception;
}
