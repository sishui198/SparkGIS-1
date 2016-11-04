package com.yao.sparkgis.io;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

public class TestHbaseAccess {
	
	@Test
	public void testCreateTable() {
		String[] family = new String[2];
		family[0] = "geodata";
		family[1] = "attributes";
		try {
			HbaseAcess.createTable("sparkgis_ict_counties", family);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void queryAll() throws IOException {
		Result result = HbaseAcess.get("sparkgis_ict_counties", "ict_counties.1001");
	}
	
	public static void main(String[] args) throws IOException {
		ShpAccess shpAccess = new ShpAccess();
		SimpleFeatureCollection featureCollection = shpAccess.readFeatures("D:\\data\\ict_counties.shp");
//		List<AttributeDescriptor> descriptors = featureCollection.getSchema().getAttributeDescriptors();
//		for (AttributeDescriptor descriptor : descriptors) {
//			System.out.println(descriptor.getType().getName());
//			System.out.println(descriptor.getLocalName());
//		}
		SimpleFeatureIterator iterator = featureCollection.features();
		
		while (iterator.hasNext()) {
			SimpleFeature feature = iterator.next();
			String[] geoDataColumn = new String[1];
			String[] geoDataColumnVal = new String[1];
			
			String[] attrColumn = new String[4];
			String[] attrColumnVal = new String[4];
			
			String featureId = feature.getID();
			Geometry geometry = (Geometry) feature.getAttribute(0);
			WKBWriter wkbWriter = new WKBWriter();
			byte[] wkb = wkbWriter.write(geometry);
			geoDataColumn[0] = "geometry";
			geoDataColumnVal[0] = new String(wkb);
			String rowKey = featureId;
			
			String name = new String(((String)feature.getAttribute(1)).getBytes("latin1"), "GBK");
			Integer code = (Integer) feature.getAttribute(2);
			Double shapeLeng = (Double) feature.getAttribute(3);
			Double shapeArea = (Double) feature.getAttribute(4);
		
			attrColumn[0] = "name";
			attrColumnVal[0] = String.valueOf(name);
			attrColumn[1] = "code";
			attrColumnVal[1] = String.valueOf(code);
			attrColumn[2] = "shape_length";
			attrColumnVal[2] = String.valueOf(shapeLeng);
			attrColumn[3] = "shape_area";
			attrColumnVal[3] = String.valueOf(shapeArea);
			HbaseAcess.addData(rowKey, "sparkgis_ict_counties", geoDataColumn, geoDataColumnVal, attrColumn, attrColumnVal);
		}
	}
}
