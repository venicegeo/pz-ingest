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
