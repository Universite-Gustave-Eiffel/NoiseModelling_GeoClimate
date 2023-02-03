package org.noise_planet.nm_geoclimate.csvmerge;

import org.h2gis.utilities.JDBCUtilities;
import org.junit.Test;
import org.noise_planet.nm_geoclimate.process.NoiseModellingRunner;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class TestMergeCSV {

    @Test
    public void testMergeCSV() throws Exception {
        DataSource ds = NoiseModellingRunner.createDataSource("", "",
                "out/", "testcsv", false);
        try(Connection connection = ds.getConnection()) {
            String csvFolderPath = new File(TestMergeCSV.class.getResource("out_0_POPULATION_EXPOSURE.csv").getFile()).getParent();
            NoiseModellingRunner.mergeCSV(connection, csvFolderPath, "out_", "_", Collections.singleton("POPULATION_EXPOSURE"));
            assertTrue(JDBCUtilities.tableExists(connection, "POPULATION_EXPOSURE"));
            Statement st = connection.createStatement();
            st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN EXPOSEDPEOPLE INT;");
            st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN EXPOSEDAREA REAL;");
            st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN EXPOSEDDWELLINGS INT;");
            st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN EXPOSEDHOSPITALS INT;");
            st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN EXPOSEDSCHOOLS INT;");
            st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN CPI INT;");
            st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN HA INT;");
            st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN HSD INT;");
            assertEquals(25*4, JDBCUtilities.getRowCount(connection, "POPULATION_EXPOSURE"));
            ResultSet rs = st.executeQuery("SELECT SUM(EXPOSEDPEOPLE) sumpeople FROM POPULATION_EXPOSURE ");
            assertTrue(rs.next());
            assertEquals(17016, rs.getInt(1));
            assertFalse(JDBCUtilities.tableExists(connection, "METADATA"));
        }
    }
}
