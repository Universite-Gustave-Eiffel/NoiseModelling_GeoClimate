
import org.orbisgis.geoclimate.osm.WorkflowOSM

import javax.sql.DataSource

/**
 *
 * @param dataSource
 * @param location_to_extract a place name. e.g Redon, a bbox defined by an array of 4 values, a point and a distance defined by an array of 3 values
 * @return
 */
def static exec(DataSource dataSource, String location_to_extract, distance=0, max_bounding_box_size=1e10) {
    def regex = "^[0-9]+(\\.[0-9]+)?(,[0-9]+(\\.[0-9]+)?){3}\$"
    def location = location_to_extract
    if(location_to_extract.matches(regex)) {
        location = location_to_extract.split(",").collect { Double.parseDouble(it)}
    }
    def result = WorkflowOSM.extractOSMZone(dataSource, location, distance, max_bounding_box_size)
    print(result["outputZoneTable"])
}