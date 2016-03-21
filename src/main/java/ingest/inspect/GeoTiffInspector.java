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
import java.io.InputStream;

import model.data.DataResource;
import model.data.location.FileAccessFactory;
import model.data.type.RasterDataType;
import model.job.metadata.SpatialMetadata;

import org.apache.commons.io.FileUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Inspects GeoTIFF file, parsing essential metadata from it.
 * 
 * @author Sonny.Saniev
 * 
 */
@Component
public class GeoTiffInspector implements InspectorType {
	@Value("${data.temp.path}")
	private String DATA_TEMP_PATH;
	@Value("${vcap.services.pz-blobstore.credentials.access:}")
	private String AMAZONS3_ACCESS_KEY;
	@Value("${vcap.services.pz-blobstore.credentials.private:}")
	private String AMAZONS3_PRIVATE_KEY;

	@Override
	public DataResource inspect(DataResource dataResource, boolean host) throws Exception {

		// Gather GeoTIFF relevant metadata
		GridCoverage2D coverage = getGridCoverage(dataResource);
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
	private GridCoverage2D getGridCoverage(DataResource dataResource) throws Exception {
		// Get the file from S3
		FileAccessFactory fileFactory = new FileAccessFactory(AMAZONS3_ACCESS_KEY, AMAZONS3_PRIVATE_KEY);
		InputStream tiffFileStream = fileFactory.getFile(((RasterDataType) dataResource.getDataType()).getLocation());
		File geoTiffFile = new File(String.format("%s\\%s.%s", DATA_TEMP_PATH, dataResource.getDataId(), "tif"));
		FileUtils.copyInputStreamToFile(tiffFileStream, geoTiffFile);

		// Read the coverage file
		AbstractGridFormat format = GridFormatFinder.findFormat(geoTiffFile);
		GridCoverage2DReader reader = format.getReader(geoTiffFile);
		GridCoverage2D coverage = (GridCoverage2D) reader.read(null);

		return coverage;
	}
}
