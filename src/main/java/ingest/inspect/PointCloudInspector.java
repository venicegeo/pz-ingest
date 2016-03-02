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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import model.data.DataResource;
import model.data.location.FileAccessFactory;
import model.data.location.FileLocation;
import model.data.response.PointCloudResponse;
import model.data.type.RasterResource;
import model.data.type.PointCloudResource;
import model.job.Job;
import model.job.metadata.SpatialMetadata;
import model.response.PiazzaResponse;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.tomcat.util.codec.binary.Base64;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Inspects point cloud response file, parsing essential metadata from json.
 * 
 * @author Sonny.Saniev
 * 
 */
@Component
public class PointCloudInspector implements InspectorType {
	@Value("${s3.key.access:}")
	private String AMAZONS3_ACCESS_KEY;
	@Value("${s3.key.private:}")
	private String AMAZONS3_PRIVATE_KEY;

	@Value("${s3.pointcloud.file.url:}")
	private String AMAZONS3_POINT_CLOUD_URL;
	
	private static final String POINT_CLOUD_ENDPOINT = "http://pzsvc-pdal.cf.piazzageo.io/api/v1/pdal";
	
	@Override
	public DataResource inspect(DataResource dataResource, boolean host) throws Exception {
		
		// Load template
		ClassLoader classLoader = getClass().getClassLoader();
		String pointCloudTemplate = IOUtils.toString(classLoader.getResourceAsStream("templates/pointCloudRequest.json"));

		//get url from data source for point cloud file, how to get actual aws url?
		FileLocation fileLocation = ((PointCloudResource) dataResource.getDataType()).getLocation();
//		String awsS3Url = String.format("%s/%s", AMAZONS3_POINT_CLOUD_URL, fileLocation.getURI());
		String awsS3Url = "https://s3.amazonaws.com/venicegeo-sample-data/pointcloud/samp71-utm.laz";
	
		// Inject Metadata from the Data Resource into the Data Store Payload
		String dataStoreRequestBody = String.format(pointCloudTemplate, awsS3Url);
		
		// Post payload to point cloud endpoint for the response payload
		PointCloudResponse pointCloudResponse = postPointCloudTemplate(POINT_CLOUD_ENDPOINT, dataStoreRequestBody);

		// Set the Metadata
		SpatialMetadata spatialMetadata = new SpatialMetadata();
		spatialMetadata.setMaxX(pointCloudResponse.getMaxx());
		spatialMetadata.setMaxY(pointCloudResponse.getMaxy());
		spatialMetadata.setMaxZ(pointCloudResponse.getMaxz());
		spatialMetadata.setMinX(pointCloudResponse.getMinx());
		spatialMetadata.setMinY(pointCloudResponse.getMiny());
		spatialMetadata.setMinZ(pointCloudResponse.getMinz());
		spatialMetadata.setCoordinateReferenceSystem(pointCloudResponse.getSpatialreference());
		//which epsg code to pull from point cloud?? There are multiple in the response payload
		//spatialMetadata.setEpsgCode(CRS.lookupEpsgCode(coordinateReferenceSystem, true));

		// Set the DataResource Spatial Metadata
		dataResource.spatialMetadata = spatialMetadata;

		return dataResource;
	}

	/**
	 * Executes POST request to point cloud url to grab the payload
	 * 
	 * @param url
	 *            The url to post for point cloud api
	 * @return The PointCloudResponse object containing metadata.
	 * 
	 * @throws IOException 
	 * @throws JsonProcessingException 
	 */
	private PointCloudResponse postPointCloudTemplate(String url, String payload) throws JsonProcessingException, IOException {
		// Setup Basic Headers
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);

		// Create the Request template and execute post
		HttpEntity<String> request = new HttpEntity<String>(payload, headers);
		RestTemplate restTemplate = new RestTemplate();
		String response = restTemplate.postForObject(url, request, String.class);
		
		// Parse required fields from point cloud json response
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = mapper.readTree(response);
		double maxx = root.at("/response/metadata/maxx").asDouble();
		double maxy = root.at("/response/metadata/maxy").asDouble();
		double maxz = root.at("/response/metadata/maxz").asDouble();
		double minx = root.at("/response/metadata/minx").asDouble();
		double miny = root.at("/response/metadata/miny").asDouble();
		double minz = root.at("/response/metadata/minz").asDouble();
		String spatialreference = root.at("/response/metadata/spatialreference").asText();
		
		// Return the new PointCloudResponse object
		return new PointCloudResponse(spatialreference, maxx, maxy, maxz, minx, miny, minz);
	}
}
