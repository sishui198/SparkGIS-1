package com.yao.sparkgis.utils;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;
import com.yao.sparkgis.io.ShpAccess;

public class SparkGisRegistrator implements KryoRegistrator {

	@Override
	public void registerClasses(Kryo kryo) {
		kryo.register(ShpAccess.class);
	}

}
