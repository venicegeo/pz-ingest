package ingest.inspect;

import model.job.metadata.ResourceMetadata;
import model.job.type.IngestJob;

/**
 * Inspects a Shapefile and parses out the relevant Metadata information for it.
 * 
 * @author Patrick.Doody
 * 
 */
public class ShapefileInspector {
	/**
	 * Inspects the Shapefile uploaded to S3 via the Job request
	 * 
	 * @param job
	 *            The ingest job request
	 * @return The metadata information on the shape file
	 */
	public ResourceMetadata inspect(IngestJob job) {

		// TODO: Return something
		return job.getMetadata();
	}
}
