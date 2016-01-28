package ingest.inspect;

import model.job.metadata.ResourceMetadata;
import model.job.type.IngestJob;

/**
 * Inspects remote resources, such as Web Feature Services (WFS) or Web Map
 * Services (WMS), or other web services that are being ingested into Piazza
 * that reside elsewhere.
 * 
 * @author Patrick.Doody
 * 
 */
public class RemoteResourceInspector {
	public ResourceMetadata inspect(IngestJob job) {
		return null;
	}
}
