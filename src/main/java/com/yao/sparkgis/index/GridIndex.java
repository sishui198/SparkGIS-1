package com.yao.sparkgis.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.BoundingBox;

import com.vividsolutions.jts.geom.Envelope;

public class GridIndex {

	private Map<Integer, Map<Integer, List<String>>> grid = new HashMap<>();
	private int n;//横向或者纵向网格的个数
	private double widthPerGrid;//每个网格的宽度
	private double heightPerGrid;//每个网格的高度
	
	public GridIndex(int n) {
		super();
		this.n = n;
	}

	public void calIndex(SimpleFeatureCollection collection) {
		Envelope envelope = collection.getBounds();
		double widthPerGrid = envelope.getWidth() / n;
		double heightPerGrid = envelope.getHeight() / n;
	
		SimpleFeatureIterator iterator = collection.features();

		while (iterator.hasNext()) {
			SimpleFeature feature = iterator.next();
			String featureID = feature.getID();
			BoundingBox box = feature.getBounds();
			int indexX = getIndexX(box.getMinX(), box.getMaxX(), widthPerGrid);
			int indexY = getIndexY(box.getMinY(), box.getMaxY(), heightPerGrid);
		
			if (!grid.containsKey(indexX)) {
				List<String> ids = new ArrayList<>();
				ids.add(featureID);
				Map<Integer, List<String>> map = new HashMap<>();
				map.put(indexY, ids);
				grid.put(indexX, map);
			} else if (grid.get(indexX).containsKey(indexY)) {
				Map<Integer, List<String>> map = grid.get(indexX);
				List<String> ids = map.get(indexY);
				ids.add(featureID);
				map.put(indexY, ids);
				grid.put(indexX, map);
			} else {
				Map<Integer, List<String>> map = grid.get(indexX);
				List<String> ids = new ArrayList<>();
				ids.add(featureID);
				map.put(indexY, ids);
				grid.put(indexX, map);
			}
		}
	}

	public List<String> rangeQuery(double minX, double maxX, double minY, double maxY) {
		int indexX = getIndexX(minX, maxX, widthPerGrid);
		int indexY = getIndexY(minY, maxY, heightPerGrid);
		if (!grid.containsKey(indexX)) {
			return null;
		} else if (!grid.get(indexX).containsKey(indexY)) {
			return null;
		} else {
			return grid.get(indexX).get(indexY);
		}
	}
	
	private int getIndexX (double minX, double maxX, double lengthPerGrid) {
		return (int) ((minX + (maxX - minX) / 2) / lengthPerGrid);
	}
	
	private int getIndexY (double minY, double maxY, double lengthPerGrid) {
		return (int) ((minY + (maxY - minY) / 2) / lengthPerGrid);
	}
	
	public void disp() {
		int i = 0;
		int j = 0;
		for (Integer k1 : grid.keySet()) {
			Map<Integer, List<String>> item = grid.get(k1);
			for (Integer k2 : item.keySet()) {
				List<String> list = item.get(k2);
				for (String id : list) {
					System.out.println(id + "--");
					i++;
				}
				
			}
			j++;
			System.out.println("***************");
		}
		System.out.println(i);
		System.out.println(j);
	}
	
}
