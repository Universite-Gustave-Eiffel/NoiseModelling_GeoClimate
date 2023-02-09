
package org.noise_planet.wps_process

import groovy.sql.Sql;
import org.orbisgis.geoclimate.osm.WorkflowOSM

import javax.sql.DataSource
import java.sql.Connection

/**
 *
 * @param dataSource
 * @param location_to_extract a place name. e.g Redon, a bbox defined by an array of 4 values, a point and a distance defined by an array of 3 values
 * @return
 */
def static Map<String, Object> exec(Connection connection, String location_to_extract, distance=0, max_bounding_box_size=1e10) {
    def regex = "^[0-9]+(\\.[0-9]+)?(,[0-9]+(\\.[0-9]+)?){3}\$"
    def location = location_to_extract
    // If the location is a latitude longitude bounding box
    // Convert into array of real numbers
    if(location_to_extract.matches(regex)) {
        location = location_to_extract.split(",").collect { Double.parseDouble(it)}
    }
    def result = WorkflowOSM.extractOSMZone(new Sql(connection), location, distance, max_bounding_box_size)
    return [outputZoneTable        : result.outputZoneTable,
            outputZoneEnvelopeTable: result.outputZoneEnvelopeTable,
            envelope               : result.envelope,
            geometry               : result.geometry
    ]
}