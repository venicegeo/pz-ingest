package ingest.inspect;

import model.data.DataResource;

/**
 * Inspects a remote WFS URL and parses out the relevant information for it.
 * 
 * @author Patrick.Doody
 * 
 */
public class WfsInspector implements InspectorType {

	@Override
	public DataResource inspect(DataResource dataResource) {
		return dataResource;
	}

}
