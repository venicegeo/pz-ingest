package ingest.inspect;

import model.job.metadata.ResourceMetadata;
import model.job.type.IngestJob;

/**
 * When a file is uploaded to Piazza, this Inspector will look through the file
 * and determine its contents. Metadata will be parsed out based on the type of
 * file.
 * 
 * @author Patrick.Doody
 * 
 */
public class FileInspector {
	/**
	 * Inspect the IngestJob's file and determine file-specific information from
	 * it, and then delegate additional inspection/parsing to the appropriate
	 * File inspector.
	 * 
	 * @param job
	 *            The ingest job
	 * @return The metadata for the file
	 */
	public ResourceMetadata inspect(IngestJob job) {
		// Parse the S3 file and try to determine the format
		String filepath = job.getMetadata().filePath;
		// Grab the S3 file and attempt to read it

		// TODO: Return something
		return job.getMetadata();
	}
}
