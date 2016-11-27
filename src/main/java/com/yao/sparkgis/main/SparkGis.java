package com.yao.sparkgis.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.geotools.data.shapefile.shp.ShapefileException;

import com.vividsolutions.jts.geom.Geometry;
import com.yao.sparkgis.io.ShpAccess;
import com.yao.sparkgis.operation.OverlayOpCpy;

public class SparkGis {

	public static void main(String[] args) throws ShapefileException, IOException {
		SparkConf conf = new SparkConf().setAppName("SparkGIS");
		//conf.set("spark.akka.frameSize", "512m");
		conf.set("spark.executor.memory", "4g");
		//conf.set("spark.master", "local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		ShpAccess access = new ShpAccess();

//		String jarPath = "/home/spark/yaoxiao/exec/lib/";
//		File jarFiles = new File(jarPath);
//		File[] files = jarFiles.listFiles();
//		for (File file : files) {
//			sc.addJar(file.getPath());
//		}
		
		List<Geometry> baseGeos = access.readGeometries("/home/spark/data/ict_landuse.shp");
		List<Geometry> overlayGeos = access.readGeometries("/home/spark/data/ict_counties.shp");
		long start = System.currentTimeMillis();
		JavaRDD<Geometry> geosRDD = sc.parallelize(baseGeos, 10);

		JavaRDD<List<Geometry>> resultRDD = geosRDD.map(new Function<Geometry, List<Geometry>>() {

			@Override
			public List<Geometry> call(Geometry geo1) throws Exception {
				List<Geometry> result = new ArrayList<>();
				for (Geometry geo2 : overlayGeos) {
					if (geo1.intersects(geo2) || geo1.contains(geo2) || geo2.contains(geo1)) {
						OverlayOpCpy op = new OverlayOpCpy(geo1, geo2);
						result.add(op.getResultGeometry(OverlayOpCpy.INTERSECTION));
					}
				}
				return result;
			}
		});
		int i = 0;
		for (List<Geometry> geos : resultRDD.collect()) {
			i += geos.size();
		}
		System.out.println("result count:" + i);
		long end = System.currentTimeMillis();
	}
}
