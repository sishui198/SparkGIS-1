package com.yao.sparkgis.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.geotools.data.CachingFeatureSource;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.shp.ShapefileException;
import org.geotools.data.shapefile.shp.ShapefileReader;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.map.FeatureLayer;
import org.geotools.map.Layer;
import org.geotools.styling.SLD;
import org.geotools.styling.Style;
import org.opengis.feature.Feature;
import org.opengis.feature.type.FeatureType;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class ShpAccess {

	public Layer readLayer(final String url) throws IOException {

		File file = new File(url);
		FileDataStore store = FileDataStoreFinder.getDataStore(file);
		SimpleFeatureSource featureSource = store.getFeatureSource();
		CachingFeatureSource cache = new CachingFeatureSource(featureSource);
		Style style = SLD.createSimpleStyle(featureSource.getSchema());
		Layer layer = new FeatureLayer(cache, style);
		return layer;
	}
	
	public SimpleFeatureCollection readFeatures(final String url) throws IOException {

		File file = new File(url);
		FileDataStore store = FileDataStoreFinder.getDataStore(file);
		SimpleFeatureSource featureSource = store.getFeatureSource();
		return featureSource.getFeatures();
	}

	public List<Geometry> readGeometries(final String url) throws ShapefileException, IOException {
		List<Geometry> _geometrys = new ArrayList<>();
		ShpFiles sf = new ShpFiles(url);
		ShapefileReader r = new ShapefileReader(sf, false, false,
				new GeometryFactory());
		while (r.hasNext()) {
			Geometry _shape = (Geometry) r.nextRecord().shape();
			_geometrys.add(_shape);
		}
		r.close();
		return _geometrys;
	}

	public void writeLayer(FeatureCollection<FeatureType, Feature> collection,
			final String url) throws IOException {
		File file = new File(url);
	}
}
