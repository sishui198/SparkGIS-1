package com.yao.sparkgis.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.geotools.data.shapefile.shp.ShapefileException;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;
import com.yao.sparkgis.io.ShpAccess;
import com.yao.sparkgis.operation.OverlayOpCpy;

import scala.Tuple2;

public class SparkGis1 {

	public static void main(String[] args) throws ShapefileException, IOException {
		SparkConf conf = new SparkConf().setAppName("SparkGIS");
		//conf.set("spark.akka.frameSize", "512m");
		conf.set("spark.executor.memory", "30g");
		conf.set("spark.master", "local[*]");
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoRegistrator");
		conf.set("spark.kryo.registrator", "com.yao.sparkgis.utils.SparkGisRegistrator");
		JavaSparkContext sc = new JavaSparkContext(conf);
		ShpAccess access = new ShpAccess();
		
		SimpleFeatureCollection collection1 = access.readFeatures("/home/iprobe/yaoxiao/data/ict_landuse.shp");
		access = null;
		ShpAccess access1 = new ShpAccess();
		SimpleFeatureCollection collection2 = access1.readFeatures("/home/iprobe/yaoxiao/data/ict_counties.shp");
		
		SimpleFeatureIterator iterator1 = collection1.features();
		SimpleFeatureIterator iterator2 = collection2.features();
		
		List<SimpleFeature> features1 = new ArrayList<>();
		List<SimpleFeature> features2 = new ArrayList<>();
		
		while (iterator1.hasNext()) {
			features1.add(iterator1.next());
		}
		
		while (iterator2.hasNext()) {
			features2.add(iterator2.next());
		}
		
		JavaRDD<SimpleFeature> featureRDD1 = sc.parallelize(features1);
		JavaRDD<SimpleFeature> featureRDD2 = sc.parallelize(features2);
		
		JavaPairRDD<String, Geometry> pairRDD1 = featureRDD1.mapToPair(new PairFunction<SimpleFeature, String, Geometry>() {

			@Override
			public Tuple2<String, Geometry> call(SimpleFeature t) throws Exception {
				return new Tuple2(t.getID(), t.getDefaultGeometry());
			}
		});
		
		JavaPairRDD<String, Geometry> pairRDD2 = featureRDD2.mapToPair(new PairFunction<SimpleFeature, String, Geometry>() {

			@Override
			public Tuple2<String, Geometry> call(SimpleFeature t) throws Exception {
				return new Tuple2(t.getID(), t.getDefaultGeometry());
			}
		});
		
		JavaPairRDD<SimpleFeature,SimpleFeature> result = featureRDD2.cartesian(featureRDD1);
		
		System.out.println(result.reduceByKey(new Function2<SimpleFeature, SimpleFeature, SimpleFeature>() {

			
			@Override
			public SimpleFeature call(SimpleFeature v1, SimpleFeature v2) throws Exception {
				
				String fID1 = v1.getID();
				String fID2 = v2.getID();
				
				Geometry g1 = (Geometry) v1.getDefaultGeometry();
				Geometry g2 = (Geometry) v2.getDefaultGeometry();
				
				if (g1.intersects(g2) || g1.contains(g2) || g2.contains(g1)) {
					OverlayOpCpy op = new OverlayOpCpy(g1, g2);
					//i++;
					op.getResultGeometry(OverlayOpCpy.INTERSECTION);
				}
				return null;
			}
		}).count());
		
		
	}
}
