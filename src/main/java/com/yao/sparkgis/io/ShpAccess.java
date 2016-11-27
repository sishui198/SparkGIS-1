package com.yao.sparkgis.io;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.geotools.data.CachingFeatureSource;
import org.geotools.data.FeatureSource;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.shp.ShapefileException;
import org.geotools.data.shapefile.shp.ShapefileReader;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.map.FeatureLayer;
import org.geotools.map.Layer;
import org.geotools.styling.SLD;
import org.geotools.styling.Style;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
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
		store.dispose();
		return featureSource.getFeatures();
	}

	 /**
     * 使用ShapefileReader读取
     *
     * @param file
     *            shapefile
     * @return 几何形状列表
     * @throws Exception
     */
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
	
	 /**
     * 使用ShapefileDataStore读取
     *
     * @param file
     *            shapefile
     * @return 几何形状列表
     * @throws Exception
     */
	public List<Geometry> readGeometries2(final String url) throws Exception {
        List<Geometry> _geometrys = new ArrayList<>();
        ShapefileDataStore _shpDataStore = new ShapefileDataStore(new File(url).toURI().toURL());
        _shpDataStore.setCharset(Charset.forName("latin1"));
        String _typeName = _shpDataStore.getTypeNames()[0];
        FeatureSource<SimpleFeatureType, SimpleFeature> _featureSource = (FeatureSource<SimpleFeatureType, SimpleFeature>) _shpDataStore.getFeatureSource(_typeName);
        FeatureCollection<SimpleFeatureType, SimpleFeature> _collection = _featureSource.getFeatures();
        FeatureIterator<SimpleFeature> _itertor = _collection.features();
        while (_itertor.hasNext()) {
            SimpleFeature _feature = _itertor.next();
            Geometry _geometry = (Geometry) _feature.getDefaultGeometry();
            _geometrys.add(_geometry);
        }
        _itertor.close();
        return _geometrys;
    }
	

	public void writeLayer(FeatureCollection<FeatureType, Feature> collection,
			final String url) throws IOException {
		File file = new File(url);
	}
	
	public static void main(String[] args) {
		
	}
}
