package com.yao.sparkgis.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.geotools.data.shapefile.shp.ShapefileException;

import com.vividsolutions.jts.geom.Geometry;
import com.yao.sparkgis.io.ShpAccess;
import com.yao.sparkgis.operation.OverlayOpCpy;

public class ShpToHBase {

	public static void main(String[] args) throws ShapefileException, IOException {
		SparkConf conf = new SparkConf().setAppName("SparkGIS");
		//conf.set("spark.akka.frameSize", "512m");
		conf.set("spark.executor.memory", "30g");
		conf.set("spark.master", "local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		ShpAccess access = new ShpAccess();
		
		

	}
}
