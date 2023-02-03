
import org.orbisgis.geoclimate.osm.WorkflowOSM

/**
 *
 * @param dataSource
 * @param location_to_extract a place name. e.g Redon, a bbox defined by an array of 4 values, a point and a distance defined by an array of 3 values
 * @return
 */
def static exec(def dataSource, def location_to_extract, distance=0, max_bounding_box_size=1e10) {
    def result = WorkflowOSM.extractOSMZone(dataSource, location_to_extract, distance, max_bounding_box_size)
    print(result["outputZoneTable"])
}