package ingest.persist;

import java.io.File;

import model.data.DataResource;
import model.data.location.FileLocation;

/**
 * Factory class to abstract the details of obtaining a File from a
 * DataResource.
 * 
 * @author Patrick.Doody
 * 
 */
public class ResourceAccessor {
	/**
	 * Returns the File specified by the Data Resource. This abstracts out
	 * getting the file from the wide variety of Data Resource objects that
	 * we'll potentially be able to ingest from within Piazza.
	 * 
	 * @param dataResource
	 *            The Data Resource
	 * @return The File
	 */
	public static File getFileFromDataResource(DataResource dataResource) {
		if (dataResource.getDataType() instanceof FileLocation) {
			return ((FileLocation) dataResource.getDataType()).getFile();
		} else {
			throw new UnsupportedOperationException();
		}
	}
}
