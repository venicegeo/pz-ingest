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
	public DataResource inspect(DataResource dataResource, boolean host) {
		// Create Data Store for the WFS
		
		// Return the Populated Metadata
		return dataResource;
	}

}
