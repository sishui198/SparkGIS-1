package com.yao.sparkgis.io;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;

public class HbaseAcess {

	private static Configuration configuration = null;

	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "192.168.2.56:2181,192.168.2.57:2181,192.168.2.58:2181");
		// configuration.set("hbase.zookeeper.property.clientPort", "2181");
	}

	public static void createTable(String tableName, String[] family)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = new HBaseAdmin(configuration);
		TableName name = TableName.valueOf(tableName);
		HTableDescriptor descriptor = new HTableDescriptor(name);
		for (int i = 0; i < family.length; i++) {
			descriptor.addFamily(new HColumnDescriptor(family[i]));
		}
		if (admin.tableExists(tableName)) {
			System.out.println("table exists");
			System.exit(0);
		} else {
			admin.createTable(descriptor);
			System.out.println("create table success!");
		}
	}

	public static void add(String tableName, SimpleFeatureCollection featureCollection) throws IOException {

		HTable table = new HTable(configuration, Bytes.toBytes(tableName));
		HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
		List<AttributeDescriptor> descriptors = featureCollection.getSchema().getAttributeDescriptors();

		SimpleFeatureIterator iterator = featureCollection.features();
		WKBWriter wkbWriter = new WKBWriter();
		while (iterator.hasNext()) {
			SimpleFeature feature = iterator.next();
			Put put = new Put(Bytes.toBytes(feature.getID()));
			for (int i = 0; i < columnFamilies.length; i++) {
				String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
				if (familyName.equals("geodata")) { // geodata列族put数据
					for (AttributeDescriptor descriptor : descriptors) {
						if (descriptor.getLocalName().equals("the_geom")) {
							put.add(Bytes.toBytes(familyName), Bytes.toBytes(descriptor.getLocalName()),
									wkbWriter.write((Geometry) feature.getDefaultGeometry()));
						}
					}
				}
				if (familyName.equals("attributes")) { // attrdata列族put数据
					for (AttributeDescriptor descriptor : descriptors) {
						if (!descriptor.getLocalName().equals("the_geom")) {
							put.add(Bytes.toBytes(familyName), Bytes.toBytes(descriptor.getLocalName()),
									wkbWriter.write((Geometry) feature.getDefaultGeometry()));
						}
					}
				}
			}
			table.put(put);
		}
	}

	/**
	 * 两个列族
	 * 
	 * @param rowKey
	 * @param tableName
	 * @param column1
	 * @param value1
	 * @param column2
	 * @param value2
	 * @throws IOException
	 */
	public static void addData(String rowKey, String tableName, String[] column1, String[] value1, String[] column2,
			String[] value2) throws IOException {
		Put put = new Put(Bytes.toBytes(rowKey));
		HTable table = new HTable(configuration, Bytes.toBytes(tableName));

		HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();

		for (int i = 0; i < columnFamilies.length; i++) {
			String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
			if (familyName.equals("geodata")) { // geodata列族put数据
				for (int j = 0; j < column1.length; j++) {
					put.add(Bytes.toBytes(familyName), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
				}
			}
			if (familyName.equals("attributes")) { // attrdata列族put数据
				for (int j = 0; j < column2.length; j++) {
					put.add(Bytes.toBytes(familyName), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
				}
			}
		}
		table.put(put);
		System.out.println("add data Success!");
	}

	/*
	 * 根据rwokey查询
	 * 
	 * @rowKey rowKey
	 * 
	 * @tableName 表名
	 */
	public static Result get(String tableName, String rowKey) throws IOException {
		Get get = new Get(Bytes.toBytes(rowKey));
		HTable table = new HTable(configuration, Bytes.toBytes(tableName));// 获取表
		Result result = table.get(get);
		for (KeyValue kv : result.list()) {
			System.out.println("family:" + Bytes.toString(kv.getFamily()));
			System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
			System.out.println("value:" + Bytes.toString(kv.getValue()));
			System.out.println("Timestamp:" + kv.getTimestamp());
			System.out.println("-------------------------------------------");
		}
		return result;
	}

}
