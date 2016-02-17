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
import model.data.type.RasterResource;
import org.apache.commons.io.FileUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.springframework.stereotype.Component;

/**
 * Inspects GeoTIFF file, parsing essential metadata from it.
 * 
 * @author Sonny.Saniev
 * 
 */
@Component
public class GeoTiffInspector implements InspectorType {

	@Override
	public DataResource inspect(DataResource dataResource, boolean host) throws Exception {

		// Gather GeoTIFF relevant metadata
		GridCoverage2D coverage = getGridCoverage(dataResource);
		CoordinateReferenceSystem coordinateReferenceSystem = coverage.getCoordinateReferenceSystem();
		double[] upperRightCorner = coverage.getEnvelope().getUpperCorner().getDirectPosition().getCoordinate();
		double[] lowerLeftCorner = coverage.getEnvelope().getLowerCorner().getDirectPosition().getCoordinate();

		// Set the Metadata
		dataResource.getSpatialMetadata().setMinX(lowerLeftCorner[0]);
		dataResource.getSpatialMetadata().setMinY(lowerLeftCorner[1]);
		dataResource.getSpatialMetadata().setMaxX(upperRightCorner[0]);
		dataResource.getSpatialMetadata().setMaxY(upperRightCorner[1]);

		// Get the SRS and EPSG codes
		dataResource.getSpatialMetadata().setCoordinateReferenceSystem(coordinateReferenceSystem.toWKT());
		dataResource.getSpatialMetadata().setEpsgCode(CRS.lookupEpsgCode(coordinateReferenceSystem, true));

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
	private GridCoverage2D getGridCoverage(DataResource dataResource) throws IOException{
		
		File geoTiffFile = new File(String.format("%s_%s.%s", "geotiff", dataResource.getDataId(), "tif"));
		
		InputStream tiffFileStream = ((RasterResource) dataResource.getDataType()).getLocation().getFile();
		FileUtils.copyInputStreamToFile(tiffFileStream, geoTiffFile);
		
		AbstractGridFormat format = GridFormatFinder.findFormat( geoTiffFile );
		GridCoverage2DReader reader = format.getReader( geoTiffFile );
		GridCoverage2D coverage = (GridCoverage2D) reader.read(null);
		
		return coverage;
	}
}
