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

import java.io.IOException;

import org.opengis.referencing.FactoryException;

import com.amazonaws.AmazonClientException;

import exception.DataInspectException;
import exception.InvalidInputException;
import model.data.DataResource;

/**
 * Interface for defining an inspector that parses a resource for metadata.
 * 
 * @author Patrick.Doody
 * 
 */
@FunctionalInterface
public interface InspectorType {
	/**
	 * Parses a DataResource object for metadata. Since a DataResource contains references to SpatialMetadata and other
	 * ResourceMetadata (and thus may be user defined from an Ingest request), this method will populate the object with
	 * additional metadata properties as able.
	 * 
	 * @param dataResource
	 *            The Data to inspect
	 * @param host
	 *            True if the data should be hosted in Piazza, false if the data should reside wherever it currently
	 *            resides.
	 * @return The input data, with additional metadata fields populated as discovered through this process
	 */
	public DataResource inspect(DataResource dataResource, boolean host)
			throws DataInspectException, AmazonClientException, InvalidInputException, IOException, FactoryException, Exception;
}
