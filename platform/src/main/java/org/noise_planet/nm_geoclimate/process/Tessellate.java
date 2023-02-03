package org.noise_planet.nm_geoclimate.process;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.prep.PreparedPolygon;
import org.noise_planet.noisemodelling.pathfinder.LayerDelaunayError;
import org.noise_planet.noisemodelling.pathfinder.LayerTinfour;
import org.noise_planet.noisemodelling.pathfinder.Triangle;

import java.util.ArrayList;
import java.util.List;

/**
 * Drop-in replacement of the poly2tree tessellate code - not used
 */
public class Tessellate {
    public static MultiPolygon tessellate(Polygon polygon) throws LayerDelaunayError {
        List<Polygon> polygons = new ArrayList<>();
        PreparedPolygon preparedPolygon = new PreparedPolygon(polygon);
        LayerTinfour layerTinfour = new LayerTinfour();
        layerTinfour.addPolygon(polygon, 0);
        layerTinfour.processDelaunay();
        List<Triangle> triangles = layerTinfour.getTriangles();
        List<Coordinate> vertices = layerTinfour.getVertices();
        GeometryFactory factory = polygon.getFactory();
        for(Triangle triangle : triangles) {
            final Coordinate a = vertices.get(triangle.getA());
            final Coordinate b = vertices.get(triangle.getB());
            final Coordinate c = vertices.get(triangle.getC());
            final double x = (a.x + b.x + c.x) / 3;
            final double y = (a.y + b.y + c.y) / 3;
            Coordinate centroid = new Coordinate(x, y);
            if(preparedPolygon.contains(factory.createPoint(centroid))) {
                polygons.add(factory.createPolygon(new Coordinate[]{a, b, c, a}));
            }
        }
        return factory.createMultiPolygon(polygons.toArray(new Polygon[0]));
    }
}
