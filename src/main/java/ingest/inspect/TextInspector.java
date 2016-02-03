package ingest.inspect;

import model.data.DataResource;

/**
 * Inspects raw text input.
 * 
 * @author Patrick.Doody
 * 
 */
public class TextInspector implements InspectorType {

	@Override
	public DataResource inspect(DataResource dataResource) {
		// TODO: What is there to inspect for a Text resource?
		return dataResource;
	}

}
