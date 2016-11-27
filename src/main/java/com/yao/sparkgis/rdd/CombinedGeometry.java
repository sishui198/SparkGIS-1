package com.yao.sparkgis.rdd;

import java.util.Set;

import com.vividsolutions.jts.geom.Geometry;

public class CombinedGeometry {
	
	private String id;
	
	private Geometry baseGeo;
	
	private Set<Geometry> overlayGeo;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Geometry getBaseGeo() {
		return baseGeo;
	}

	public void setBaseGeo(Geometry baseGeo) {
		this.baseGeo = baseGeo;
	}

	public Set<Geometry> getOverlayGeo() {
		return overlayGeo;
	}

	public void setOverlayGeo(Set<Geometry> overlayGeo) {
		this.overlayGeo = overlayGeo;
	}
	
}
