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

import org.apache.commons.io.IOUtils;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import com.amazonaws.AmazonClientException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import exception.DataInspectException;
import exception.InvalidInputException;
import ingest.model.PointCloudResponse;
import ingest.utility.IngestUtilities;
import model.data.DataResource;
import model.data.location.FileAccessFactory;
import model.data.location.FileLocation;
import model.data.type.PointCloudDataType;
import model.job.metadata.SpatialMetadata;
import model.logger.AuditElement;
import model.logger.Severity;
import util.PiazzaLogger;

/**
 * Inspects Point Cloud response file, parsing essential metadata from json.
 * 
 * @author Sonny.Saniev
 * 
 */
@Component
public class PointCloudInspector implements InspectorType {
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private IngestUtilities ingestUtilities;
	@Value("${vcap.services.pz-blobstore.credentials.access_key_id:}")
	private String AMAZONS3_ACCESS_KEY;
	@Value("${vcap.services.pz-blobstore.credentials.secret_access_key:}")
	private String AMAZONS3_PRIVATE_KEY;
	@Value("${point.cloud.endpoint}")
	private String POINT_CLOUD_ENDPOINT;
	
	@Autowired
	private RestTemplate restTemplate;

	private static final Logger LOG = LoggerFactory.getLogger(PointCloudInspector.class);
	private static final String INGEST = "ingest";

	@Override
	public DataResource inspect(DataResource dataResource, boolean host)
			throws DataInspectException, AmazonClientException, InvalidInputException, IOException, FactoryException {
		logger.log(String.format("Begin parsing Point Cloud for Data %s", dataResource.getDataId()), Severity.INFORMATIONAL,
				new AuditElement(INGEST, "beginParsingPointCloud", dataResource.getDataId()));

		// Load point cloud post request template
		ClassLoader classLoader = getClass().getClassLoader();
		String pointCloudTemplate = null;
		InputStream templateStream = null;
		try {
			templateStream = classLoader.getResourceAsStream("templates" + File.separator + "pointCloudRequest.json");
			pointCloudTemplate = IOUtils.toString(templateStream);
		} finally {
			if (templateStream != null) {
				templateStream.close();
			}
		}

		// Obtain File URL from AWS S3 Bucket
		FileAccessFactory fileFactory = ingestUtilities.getFileFactoryForDataResource(dataResource);
		FileLocation fileLocation = ((PointCloudDataType) dataResource.getDataType()).getLocation();
		String awsS3Url = fileFactory.getFileUri(fileLocation);

		// Inject URL into the Post Payload
		String payloadBody = String.format(pointCloudTemplate, awsS3Url);

		// Attempt to populate Spatial metadata for the Point Cloud by pointing
		// to the Point Cloud metadata service.
		SpatialMetadata spatialMetadata = new SpatialMetadata();
		try {
			// Post payload to point cloud endpoint for the metadata response
			PointCloudResponse pointCloudResponse = postPointCloudTemplate(POINT_CLOUD_ENDPOINT, payloadBody);

			// Set the Metadata
			spatialMetadata.setMaxX(pointCloudResponse.getMaxx());
			spatialMetadata.setMaxY(pointCloudResponse.getMaxy());
			spatialMetadata.setMaxZ(pointCloudResponse.getMaxz());
			spatialMetadata.setMinX(pointCloudResponse.getMinx());
			spatialMetadata.setMinY(pointCloudResponse.getMiny());
			spatialMetadata.setMinZ(pointCloudResponse.getMinz());
			spatialMetadata.setCoordinateReferenceSystem(pointCloudResponse.getSpatialreference());

			String formattedSpatialreference = pointCloudResponse.getSpatialreference().replace("\\\"", "\"");
			// Decode CoordinateReferenceSystem and parse EPSG code
			CoordinateReferenceSystem worldCRS = CRS.parseWKT(formattedSpatialreference);
			spatialMetadata.setEpsgCode(CRS.lookupEpsgCode(worldCRS, true));

			// Populate the projected EPSG:4326 spatial metadata
			populateSpatialMetadata(spatialMetadata, dataResource.getDataId());
		} catch (Exception exception) {
			String error = String.format("Error populating Spatial Metadata for %s Point Cloud located at %s: %s", dataResource.getDataId(),
					awsS3Url, exception.getMessage());
			logger.log(error, Severity.WARNING);
			LOG.error(error, exception);
		}

		// Set the DataResource Spatial Metadata
		dataResource.spatialMetadata = spatialMetadata;

		logger.log(String.format("Completed parsing Point Cloud for Data %s", dataResource.getDataId()), Severity.INFORMATIONAL,
				new AuditElement(INGEST, "completeParsingPointCloud", dataResource.getDataId()));

		return dataResource;
	}

	private void populateSpatialMetadata(final SpatialMetadata spatialMetadata, final String dataId) {
		try {
			spatialMetadata.setProjectedSpatialMetadata(ingestUtilities.getProjectedSpatialMetadata(spatialMetadata));
		} catch (Exception exception) {
			String error = String.format("Could not project the spatial metadata for Data %s because of exception: %s",
					dataId, exception.getMessage());
			LOG.error(error, exception);
			logger.log(error, Severity.WARNING);
		}
	}
	
	/**
	 * Executes POST request to Point Cloud to grab the Payload
	 * 
	 * @param url
	 *            The URL to post for point cloud api
	 * @return The PointCloudResponse object containing metadata.
	 */
	private PointCloudResponse postPointCloudTemplate(String url, String payload) throws IOException {
		// Setup Basic Headers
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);

		// Create the Request template and execute post
		HttpEntity<String> request = new HttpEntity<String>(payload, headers);
		String response = "";
		try {
			logger.log("Sending Metadata Request to Point Cloud Service", Severity.INFORMATIONAL,
					new AuditElement(INGEST, "requestPointCloudMetadata", url));
			response = restTemplate.postForObject(url, request, String.class);
		} catch (HttpServerErrorException e) {
			String error = "Error occurred posting to: " + url + "\nPayload: \n" + payload
					+ "\nMost likely the payload source file is not accessible.";			
			// this exception will be thrown until the s3 file is accessible to external services

			LOG.error(error, e);
			throw new HttpServerErrorException(e.getStatusCode(), error);
		}

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
