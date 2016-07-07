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
package ingest.event;

import java.util.HashMap;
import java.util.Map;

import model.data.DataResource;
import model.job.Job;
import model.job.type.IngestJob;

/**
 * Represents an Ingest Event that is fired to the pz-workflow component upon
 * successful ingest of any data that has been indexed into Piazza by this
 * component.
 * 
 * This populates the JSON Payload for the event that is fired to the
 * pz-workflow.
 * 
 * The template EventType for this Event has to be registered with the
 * pz-workflow before events of this type can be processed by that component.
 * 
 * @author Patrick.Doody
 * 
 */
public class IngestEvent {
	public String type;
	public String date;
	public Map<String, Object> data = new HashMap<String, Object>();

	/**
	 * Creates a new Ingest Event based on the Data Resource that was created by
	 * the Ingest component. This will parse the necessary properties from the
	 * Job and DataResource objects and insert them into the proper format
	 * defined by the Ingest EventType.
	 * 
	 * @param id
	 *            The ID of the Job (type) as registered initially with
	 *            pz-workflow
	 * @param job
	 *            The Job that was processed and resulted in the creation of the
	 *            Data Resource
	 * @param dataResource
	 *            The Data Resource created as a result of the Job
	 */
	public IngestEvent(String id, Job job, DataResource dataResource) {
		// The Type/ID as registered initially with the service.
		type = id;
		// Set the date
		date = job.getCreatedOnString();
		// Populate the Map fields with the Data Resource metadata.
		data.put("dataId", dataResource.getDataId());
		data.put("dataType", dataResource.getDataType().getClass().getSimpleName());
		data.put("epsg", dataResource.getSpatialMetadata().getEpsgCode());
		data.put("minX", dataResource.getSpatialMetadata().getMinX());
		data.put("minY", dataResource.getSpatialMetadata().getMinY());
		data.put("maxX", dataResource.getSpatialMetadata().getMaxX());
		data.put("maxY", dataResource.getSpatialMetadata().getMaxY());
		data.put("hosted", ((IngestJob) job.getJobType()).getHost());
	}
}
