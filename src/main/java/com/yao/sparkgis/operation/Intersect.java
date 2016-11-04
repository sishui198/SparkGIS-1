package com.yao.sparkgis.operation;

import java.util.ArrayList;
import java.util.List;

import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.FeatureCollection;

import com.vividsolutions.jts.algorithm.BoundaryNodeRule;
import com.vividsolutions.jts.algorithm.LineIntersector;
import com.vividsolutions.jts.algorithm.RobustLineIntersector;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geomgraph.GeometryGraph;

public class Intersect {
	
	 protected GeometryGraph[] arg; 
	 
	 protected final LineIntersector li = new RobustLineIntersector();

	private List<Point> insectPoint(FeatureCollection c1, FeatureCollection c2) {
		List<Point> points = new ArrayList<Point>();
		SimpleFeatureIterator iterator1 = (SimpleFeatureIterator) c1.features();
		while (iterator1.hasNext()) {
			MultiPolygon g1 = (MultiPolygon) iterator1.next().getAttribute("the_geom");
			SimpleFeatureIterator iterator2 = (SimpleFeatureIterator) c2.features();
			while (iterator2.hasNext()) {
				MultiPolygon g2 = (MultiPolygon) iterator1.next().getAttribute("the_geom");
				arg = new GeometryGraph[2];
			    arg[0] = new GeometryGraph(0, g1, BoundaryNodeRule.OGC_SFS_BOUNDARY_RULE);
			    arg[1] = new GeometryGraph(1, g2, BoundaryNodeRule.OGC_SFS_BOUNDARY_RULE);
			    
			    arg[0].computeSelfNodes(li, false);
			    arg[1].computeSelfNodes(li, false);
			 // compute intersections between edges of the two input geometries
			    arg[0].computeEdgeIntersections(arg[1], li, true);
			}
			iterator2.close();
		}
		return null;
	}
}
