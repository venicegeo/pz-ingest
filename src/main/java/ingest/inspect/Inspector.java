package ingest.inspect;

import model.job.metadata.ResourceMetadata;
import model.job.type.IngestJob;

/**
 * Inspects the incoming data in Job Request for information for the Ingest.
 * Capable of inspecting files, or URLs.
 * 
 * @author Patrick.Doody
 * 
 */
public class Inspector {
	private FileInspector fileInspector = new FileInspector();
	private RemoteResourceInspector remoteResourceInspector = new RemoteResourceInspector();

	/**
	 * Inspects the Ingest Job and parses out metadata information, filling in
	 * additional metadata when able.
	 * 
	 * @param job
	 *            The ingestion Job information
	 * @return The metadata for the job, populated as much as possible
	 */
	public ResourceMetadata inspect(IngestJob job) {
		// Pass off the Job to the appropriate inspectors
		ResourceMetadata metadata;
		if (job.getMetadata().filePath.isEmpty() == false) {
			metadata = fileInspector.inspect(job);
		} else {
			metadata = remoteResourceInspector.inspect(job);
		}

		// Attempt to add any metadata information possible

		// Return metadata
		return metadata;
	}
}
