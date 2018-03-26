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
import java.nio.file.Files;

import javax.media.jai.PlanarImage;

import org.apache.commons.io.FileUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.geotools.referencing.CRS;
import org.geotools.resources.image.ImageUtilities;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.AmazonClientException;

import exception.DataInspectException;
import exception.InvalidInputException;
import ingest.utility.IngestUtilities;
import model.data.DataResource;
import model.data.location.FileAccessFactory;
import model.data.type.RasterDataType;
import model.job.metadata.SpatialMetadata;
import model.logger.AuditElement;
import model.logger.Severity;
import util.PiazzaLogger;

/**
 * Inspects GeoTIFF file, parsing essential metadata from it.
 * 
 * @author Sonny.Saniev
 * 
 */
@Component
public class GeoTiffInspector implements InspectorType {
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private IngestUtilities ingestUtilities;
	@Value("${data.temp.path}")
	private String DATA_TEMP_PATH;
	@Value("${vcap.services.pz-blobstore.credentials.access_key_id:}")
	private String AMAZONS3_ACCESS_KEY;
	@Value("${vcap.services.pz-blobstore.credentials.secret_access_key:}")
	private String AMAZONS3_PRIVATE_KEY;

	private static final Logger LOG = LoggerFactory.getLogger(GeoTiffInspector.class);
	private static final String INGEST = "ingest";

	@Override
	public DataResource inspect(DataResource dataResource, boolean host)
			throws DataInspectException, AmazonClientException, InvalidInputException, IOException, FactoryException {
		// Gather GeoTIFF relevant metadata
		String fileName = String.format("%s%s%s.%s", DATA_TEMP_PATH, File.separator, dataResource.getDataId(), "tif");

		logger.log(String.format("Begin GeoTools Parsing for %s at temporary file %s", dataResource.getDataId(), fileName),
				Severity.INFORMATIONAL, new AuditElement(INGEST, "beginParsingGeoTiff", fileName));

		File geoTiffFile = new File(fileName);
		GridCoverage2DReader reader = getGridCoverage(dataResource, geoTiffFile);
		GridCoverage2D coverage = (GridCoverage2D) reader.read(null);
		CoordinateReferenceSystem coordinateReferenceSystem = coverage.getCoordinateReferenceSystem();
		double[] upperRightCorner = coverage.getEnvelope().getUpperCorner().getDirectPosition().getCoordinate();
		double[] lowerLeftCorner = coverage.getEnvelope().getLowerCorner().getDirectPosition().getCoordinate();

		// Set the Metadata
		SpatialMetadata spatialMetadata = new SpatialMetadata();
		spatialMetadata.setMinX(lowerLeftCorner[0]);
		spatialMetadata.setMinY(lowerLeftCorner[1]);
		spatialMetadata.setMaxX(upperRightCorner[0]);
		spatialMetadata.setMaxY(upperRightCorner[1]);

		// Get the SRS and EPSG codes
		spatialMetadata.setCoordinateReferenceSystem(coordinateReferenceSystem.toWKT());
		spatialMetadata.setEpsgCode(CRS.lookupEpsgCode(coordinateReferenceSystem, true));

		// Set the Spatial Metadata
		dataResource.spatialMetadata = spatialMetadata;

		// Populate the projected EPSG:4326 spatial metadata
		try {
			dataResource.spatialMetadata.setProjectedSpatialMetadata(ingestUtilities.getProjectedSpatialMetadata(spatialMetadata));
		} catch (Exception exception) {
			String error = String.format("Could not project the spatial metadata for Data %s because of exception: %s",
					dataResource.getDataId(), exception.getMessage());
			LOG.error(error, exception);
			logger.log(error, Severity.WARNING);
		}

		// Delete the file; cleanup.
		try {
			// Required to release the lock on the File for deletion
			PlanarImage planarImage = (PlanarImage) coverage.getRenderedImage();
			ImageUtilities.disposePlanarImageChain(planarImage);
			// Dispose reader and coverage
			reader.dispose();
			coverage.dispose(true);
			// Finally, delete the file.
			Files.deleteIfExists(geoTiffFile.toPath());
		} catch (Exception exception) {
			String error = String.format("Error cleaning up GeoTiff file for %s Load: %s", dataResource.getDataId(),
					exception.getMessage());
			LOG.error(error, exception, new AuditElement(INGEST, "failedToDeleteTemporaryGeoTiff", fileName));
			logger.log(error, Severity.WARNING);
		}

		logger.log(String.format("Completed GeoTools Parsing for %s at temporary file %s", dataResource.getDataId(), fileName),
				Severity.INFORMATIONAL, new AuditElement(INGEST, "completeParsingGeoTiff", fileName));

		// Return the metadata
		return dataResource;
	}

	/**
	 * Gets the GridCoverage2D for the GeoTIFF file.
	 * 
	 * @param dataResource
	 *            The DataResource to gather GeoTIFF source info
	 * @return GridCoverage2D grid coverage
	 */
	private GridCoverage2DReader getGridCoverage(DataResource dataResource, File file)
			throws AmazonClientException, InvalidInputException, IOException {
		// Get the file from S3
		FileAccessFactory fileFactory = ingestUtilities.getFileFactoryForDataResource(dataResource);
		InputStream tiffFileStream = fileFactory.getFile(((RasterDataType) dataResource.getDataType()).getLocation());
		FileUtils.copyInputStreamToFile(tiffFileStream, file);

		// Read the coverage file
		AbstractGridFormat format = GridFormatFinder.findFormat(file);
		GridCoverage2DReader reader = format.getReader(file);
		return reader;
	}
}
