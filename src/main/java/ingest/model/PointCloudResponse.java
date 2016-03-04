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
package ingest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response object representing data from point cloud rest api payload
 * 
 * @author Sonny.Saniev
 * 
 */

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PointCloudResponse {

	@JsonProperty("spatialreference")
	public String spatialreference;
	
	@JsonProperty("maxx")
	public Double maxx;
	
	@JsonProperty("maxy")
	public Double maxy;
	
	@JsonProperty("maxz")
	public Double maxz;
	
	@JsonProperty("minx")
	public Double minx;
	
	@JsonProperty("miny")
	public Double miny;
	
	@JsonProperty("minz")
	public Double minz;
	

	public PointCloudResponse(String spatialreference, Double maxx, Double maxy, Double maxz, Double minx, Double miny, Double minz) {
		this.spatialreference = spatialreference;
		this.maxx = maxx;
		this.maxy = maxy;
		this.maxz = maxz;
		this.minx = minx;
		this.miny = miny;
		this.minz = minz;
	}

	public PointCloudResponse() {
	}

	public String getSpatialreference() {
		return spatialreference;
	}

	public void setSpatialreference(String spatialreference) {
		this.spatialreference = spatialreference;
	}

	public Double getMaxx() {
		return maxx;
	}

	public void setMaxx(Double maxx) {
		this.maxx = maxx;
	}

	public Double getMaxy() {
		return maxy;
	}

	public void setMaxy(Double maxy) {
		this.maxy = maxy;
	}

	public Double getMaxz() {
		return maxz;
	}

	public void setMaxz(Double maxz) {
		this.maxz = maxz;
	}

	public Double getMinx() {
		return minx;
	}

	public void setMinx(Double minx) {
		this.minx = minx;
	}

	public Double getMiny() {
		return miny;
	}

	public void setMiny(Double miny) {
		this.miny = miny;
	}

	public Double getMinz() {
		return minz;
	}

	public void setMinz(Double minz) {
		this.minz = minz;
	}

	@Override
	public String toString() {
		return "PointCloudResponse [spatialreference=" + spatialreference + ", maxx=" + maxx + ", maxy=" + maxy + ", maxz=" + maxz
				+ ", minx=" + minx + ", miny=" + miny + ", minz=" + minz + "]";
	}
}
