package org.noise_planet.nmcluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import org.apache.commons.text.StringSubstitutor;
import org.cts.crs.CRSException;
import org.cts.op.CoordinateOperationException;
import org.h2.util.OsgiDataSourceFactory;
import org.h2.util.ScriptReader;
import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.api.ProgressVisitor;
import org.h2gis.functions.factory.H2GISFunctions;
import org.h2gis.utilities.GeometryTableUtilities;
import org.h2gis.utilities.JDBCUtilities;
import org.h2gis.utilities.TableLocation;
import org.h2gis.utilities.dbtypes.DBTypes;
import org.h2gis.utilities.dbtypes.DBUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.geom.prep.PreparedPolygon;
import org.locationtech.jts.operation.overlay.OverlayOp;
import org.noise_planet.noisemodelling.jdbc.BezierContouring;
import org.noise_planet.noisemodelling.jdbc.LDENComputeRaysOut;
import org.noise_planet.noisemodelling.jdbc.LDENConfig;
import org.noise_planet.noisemodelling.jdbc.LDENPointNoiseMapFactory;
import org.noise_planet.noisemodelling.jdbc.LDENPropagationProcessData;
import org.noise_planet.noisemodelling.jdbc.PointNoiseMap;
import org.noise_planet.noisemodelling.pathfinder.IComputeRaysOut;
import org.noise_planet.noisemodelling.pathfinder.utils.JVMMemoryMetric;
import org.noise_planet.noisemodelling.pathfinder.utils.KMLDocument;
import org.noise_planet.noisemodelling.pathfinder.utils.PowerUtils;
import org.noise_planet.noisemodelling.pathfinder.utils.ProfilerThread;
import org.noise_planet.noisemodelling.pathfinder.utils.ProgressMetric;
import org.noise_planet.noisemodelling.pathfinder.utils.ReceiverStatsMetric;
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData;
import org.osgi.service.jdbc.DataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Processing of noise maps
 * @author Pierre Aumond, Université Gustave Eiffel
 * @author Nicolas Fortin, Université Gustave Eiffel
 * @author Gwendall Petit, Lab-STICC CNRS UMR 6285
 */
public class NoiseModellingInstance {
    private static final int BATCH_MAX_SIZE = 100;
    public static final int CBS_GRID_SIZE = 10;
    public static final int CBS_MAIN_GRID_SIZE = 800;
    public enum SOURCE_TYPE { SOURCE_TYPE_RAIL, SOURCE_TYPE_ROAD}
    public static final String[] EXPOSITION_TABLES = new String[] {"POPULATION_EXPOSURE"};
    Logger logger = LoggerFactory.getLogger(NoiseModellingInstance.class);
    Connection connection;
    String workingDirectory;
    int configurationId = 0;
    String outputPrefix = "";
    boolean isExportDomain = false;
    String outputFolder;

    // LDEN classes for A maps : 55-59, 60-64, 65-69, 70-74 et >75 dB
    final List<Double> isoLevelsLDEN = Arrays.asList(55.0d,60.0d,65.0d,70.0d,75.0d,200.0d);
    // LNIGHT classes for A maps : 50-54, 55-59, 60-64, 65-69 et >70 dB
    final List<Double>  isoLevelsLNIGHT = Arrays.asList(50.0d,55.0d,60.0d,65.0d,70.0d,200.0d);

    // LDEN classes for C maps: >68 dB
    final List<Double>  isoCLevelsLDEN = Arrays.asList(68.0d,200.0d);
    // LNIGHT classes for C maps : >62 dB
    final List<Double>  isoCLevelsLNIGHT = Arrays.asList(62.0d,200.0d);

    // LDEN classes for C maps and : >73 dB
    final List<Double>  isoCFerConvLevelsLDEN= Arrays.asList(73.0d,200.0d);
    // LNIGHT classes for C maps : >65 dB
    final List<Double>  isoCFerConvLevelsLNIGHT= Arrays.asList(65.0d,200.0d);

    // LDEN classes for C maps and : >68 dB
    final List<Double>  isoCFerLGVLevelsLDEN= Arrays.asList(68.0d,200.0d);
    // LNIGHT classes for C maps : >62 dB
    final List<Double>  isoCFerLGVLevelsLNIGHT= Arrays.asList(62.0d,200.0d);

    String cbsARoadLden;
    String cbsARoadLnight;
    String cbsAFerLden;
    String cbsAFerLnight;
    String cbsCRoadLden;
    String cbsCRoadLnight;
    String cbsCFerLGVLden;
    String cbsCFerLGVLnight;
    String cbsCFerCONVLden;
    String cbsCFerCONVLnight;


    public NoiseModellingInstance(Connection connection, String workingDirectory) {
        this.connection = connection;
        this.workingDirectory = workingDirectory;
        this.outputFolder = workingDirectory;
    }

    public String getOutputFolder() {
        return outputFolder;
    }

    public void setOutputFolder(String outputFolder) {
        this.outputFolder = outputFolder;
    }

    public int getConfigurationId() {
        return configurationId;
    }

    public void setConfigurationId(int configurationId) {
        this.configurationId = configurationId;
    }

    public String getOutputPrefix() {
        return outputPrefix;
    }

    public void setOutputPrefix(String outputPrefix) {
        this.outputPrefix = outputPrefix;
    }

    public static DataSource createDataSource(String user, String password, String dbDirectory, String dbName, boolean debug) throws SQLException {
        // Create H2 memory DataSource
        org.h2.Driver driver = org.h2.Driver.load();
        OsgiDataSourceFactory dataSourceFactory = new OsgiDataSourceFactory(driver);
        Properties properties = new Properties();
        String databasePath = "jdbc:h2:" + new File(dbDirectory, dbName).getAbsolutePath();
        properties.setProperty(DataSourceFactory.JDBC_URL, databasePath);
        properties.setProperty(DataSourceFactory.JDBC_USER, user);
        properties.setProperty(DataSourceFactory.JDBC_PASSWORD, password);
        if(debug) {
            properties.setProperty("TRACE_LEVEL_FILE", "3"); // enable debug
        }
        DataSource dataSource = dataSourceFactory.createDataSource(properties);
        // Init spatial ext
        try (Connection connection = dataSource.getConnection()) {
            H2GISFunctions.load(connection);
        }
        return dataSource;

    }

    public void exportDomain(LDENPropagationProcessData inputData, String path, int epsg) {
        try {
            logger.info("Export domain : Cell number " + inputData.cellId);
            FileOutputStream outData = new FileOutputStream(path);
            KMLDocument kmlDocument = new KMLDocument(outData);
            kmlDocument.setInputCRS("EPSG:" + epsg);
            kmlDocument.writeHeader();
            kmlDocument.writeTopographic(inputData.profileBuilder.getTriangles(), inputData.profileBuilder.getVertices());
            kmlDocument.writeBuildings(inputData.profileBuilder);
            kmlDocument.writeFooter();
        } catch (IOException | CRSException | XMLStreamException | CoordinateOperationException ex) {
            logger.error("Error while exporting domain", ex);
        }
    }

    public static class JobElement implements Comparable<JobElement> {
        public List<String> roadsUueid = new ArrayList<>();
        public List<String> railsUueid = new ArrayList<>();
        public double totalSourceLineLength = 0;


        @Override
        public int compareTo(JobElement o) {
            return Double.compare(o.totalSourceLineLength, totalSourceLineLength);
        }
    }

    public static class ClusterConfiguration {
        public List<String> roadsUueid = new ArrayList<>();
        public List<String> railsUueids = new ArrayList<>();
        public List<JobElement> jobElementList = new ArrayList<>();
    }

    public static ClusterConfiguration loadClusterConfiguration(String workingDirectory, int nodeId) throws IOException {
        // Load Json cluster configuration file
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(new File(workingDirectory, "cluster_config.json"));
        JsonNode v;
        ClusterConfiguration configuration = new ClusterConfiguration();
        if (node instanceof ArrayNode) {
            ArrayNode aNode = (ArrayNode) node;
            for (JsonNode cellNode : aNode) {
                JsonNode nodeIdProp = cellNode.get("nodeId");
                if(nodeIdProp != null && nodeIdProp.canConvertToInt() && nodeIdProp.intValue() == nodeId) {
                    if(cellNode.get("roads_uueids") instanceof ArrayNode) {
                        for (JsonNode uueidNode : cellNode.get("roads_uueids")) {
                            configuration.roadsUueid.add(uueidNode.asText());
                        }
                    }
                    if(cellNode.get("rails_uueids") instanceof ArrayNode) {
                        for (JsonNode uueidNode : cellNode.get("rails_uueids")) {
                            configuration.railsUueids.add(uueidNode.asText());
                        }
                    }
                    break;
                }
            }
        }
        return configuration;
    }

    public static NoiseModellingInstance startNoiseModelling(Connection connection, ProgressVisitor progressVisitor,
                                                             ClusterConfiguration clusterConfiguration,
                                                             String workingDir, int nodeId) throws SQLException, IOException {
        Sql sql = new Sql(connection);
        // Fetch configuration ID
        Map groovyRowResult = sql.firstRow("SELECT * from metadata");
        int confId = (Integer)groovyRowResult.get("GRID_CONF");
        NoiseModellingInstance nm = new NoiseModellingInstance(connection, workingDir);
        nm.setConfigurationId(confId);
        nm.setOutputPrefix(String.format(Locale.ROOT, "out_%d_", nodeId));

        nm.createExpositionTables(new EmptyProgressVisitor(), clusterConfiguration.roadsUueid, clusterConfiguration.railsUueids);

        int subProcessCount = 0;
        if(!clusterConfiguration.roadsUueid.isEmpty()) {
            subProcessCount++;
        }
        if(!clusterConfiguration.railsUueids.isEmpty()) {
            subProcessCount++;
        }
        ProgressVisitor uueidVisitor = progressVisitor.subProcess(subProcessCount);
        nm.uueidsLoop(uueidVisitor, clusterConfiguration.roadsUueid, NoiseModellingInstance.SOURCE_TYPE.SOURCE_TYPE_ROAD);
        nm.uueidsLoop(uueidVisitor, clusterConfiguration.railsUueids, NoiseModellingInstance.SOURCE_TYPE.SOURCE_TYPE_RAIL);

        // export metadata
        PreparedStatement ps = connection.prepareStatement("CALL CSVWRITE(?, ?)");
        ps.setString(1,new File(nm.outputFolder,
                nm.outputPrefix + "METADATA.csv").getAbsolutePath() );
        ps.setString(2, "SELECT *, (EXTRACT(EPOCH FROM ROAD_END) - EXTRACT(EPOCH FROM ROAD_START)) ROAD_TOTAL,(EXTRACT(EPOCH FROM GRID_END) - EXTRACT(EPOCH FROM GRID_START)) GRID_TOTAL  FROM METADATA");
        ps.execute();

        sql.execute("ALTER TABLE POPULATION_EXPOSURE DROP COLUMN POP_ACCURATE, MIN_LAEQ, INTERVAL_LAEQ, MAX_LAEQ");

        // Export exposition tables
        for(String tableName : EXPOSITION_TABLES) {
            ps.setString(1, new File(nm.outputFolder, nm.outputPrefix + tableName + ".csv").getAbsolutePath());
            ps.setString(2, "SELECT * FROM " + tableName);
            ps.execute();
        }
        return nm;
    }

    /**
     *
     * @param progressLogger Progression instance
     * @param uueidList List of UUEID
     * @param sourceType source type
     * @throws SQLException
     * @throws IOException
     */
    public void uueidsLoop(ProgressVisitor progressLogger, List<String> uueidList, SOURCE_TYPE sourceType) throws SQLException, IOException {
        File outputDir = new File(outputFolder);
        if(!outputDir.exists()) {
            if(!outputDir.mkdir()) {
                logger.error("Cannot create " + outputDir.getAbsolutePath());
            }
        }
        if(uueidList.isEmpty()) {
            return;
        }
        Sql sql = new Sql(connection);

        List<String> outputTable = recreateCBS(sourceType);

        GroovyRowResult rs = sql.firstRow("SELECT * FROM CONF WHERE CONFID = ?", new Object[]{configurationId});
        int maxSrcDist = (Integer)rs.get("confmaxsrcdist");

        ProgressVisitor uueidVisitor = progressLogger.subProcess(uueidList.size());
        for(String uueid : uueidList) {
            // Keep only receivers near selected UUEID
            String conditionReceiver = "";
            // keep only receiver from contouring noise map
            // conditionReceiver = " RCV_TYPE = 2 AND ";
            sql.execute("DROP TABLE IF EXISTS SOURCES_SPLITED;");
            if(sourceType == SOURCE_TYPE.SOURCE_TYPE_RAIL) {
                sql.execute("CREATE TABLE SOURCES_SPLITED AS SELECT * from ST_EXPLODE('(SELECT ST_ToMultiSegments(THE_GEOM) THE_GEOM FROM RAIL_SECTIONS WHERE UUEID = ''"+uueid+"'')');");
            } else {
                sql.execute("CREATE TABLE SOURCES_SPLITED AS SELECT * from ST_EXPLODE('(SELECT ST_ToMultiSegments(THE_GEOM) THE_GEOM FROM ROADS WHERE UUEID = ''"+uueid+"'')');");
            }
            logger.info("Fetch receivers near uueid " + uueid);
            sql.execute("CREATE SPATIAL INDEX ON SOURCES_SPLITED(THE_GEOM)");
            sql.execute("DROP TABLE IF EXISTS RECEIVERS_UUEID");
            sql.execute("CREATE TABLE RECEIVERS_UUEID(THE_GEOM geometry, PK integer not null, PK_1 integer, RCV_TYPE integer) AS SELECT R.* FROM RECEIVERS R WHERE "+conditionReceiver+" EXISTS (SELECT 1 FROM SOURCES_SPLITED R2 WHERE ST_EXPAND(R.THE_GEOM, "+maxSrcDist+", "+maxSrcDist+") && R2.THE_GEOM AND ST_DISTANCE(R.THE_GEOM, R2.THE_GEOM) <= "+maxSrcDist+" LIMIT 1)");
            logger.info("Create primary key and index on filtered receivers");
            sql.execute("ALTER TABLE RECEIVERS_UUEID ADD PRIMARY KEY(PK)");
            sql.execute("CREATE INDEX RECEIVERS_UUEID_PK1 ON RECEIVERS_UUEID(PK_1)");
            sql.execute("CREATE SPATIAL INDEX RECEIVERS_UUEID_SPI ON RECEIVERS_UUEID (THE_GEOM)");
            // Filter only sound source that match the UUEID
            logger.info("Fetch sound sources that match with uueid " + uueid);
            sql.execute("DROP TABLE IF EXISTS LW_UUEID");
            if(sourceType == SOURCE_TYPE.SOURCE_TYPE_RAIL) {
                sql.execute("CREATE TABLE LW_UUEID AS SELECT LW.* FROM LW_RAILWAY LW WHERE UUEID = '" + uueid + "'");
            } else {
                sql.execute("CREATE TABLE LW_UUEID AS SELECT LW.* FROM LW_ROADS LW, ROADS R WHERE LW.PK = R.PK AND R.UUEID = '" + uueid + "'");
            }
            sql.execute("ALTER TABLE LW_UUEID ALTER COLUMN PK INTEGER NOT NULL");
            sql.execute("ALTER TABLE LW_UUEID ADD PRIMARY KEY(PK)");
            sql.execute("CREATE SPATIAL INDEX ON LW_UUEID(THE_GEOM)");
            int nbSources = asInteger(sql.firstRow("SELECT COUNT(*) CPT FROM LW_UUEID").get("CPT"));
            logger.info(String.format(Locale.ROOT, "There is %d sound sources with this UUEID", nbSources));
            int nbReceivers = asInteger(sql.firstRow("SELECT COUNT(*) CPT FROM RECEIVERS_UUEID").get("CPT"));
            logger.info(String.format(Locale.ROOT, "There is %d receivers with this UUEID", nbReceivers));


            if(nbSources > 0) {
                doPropagation(uueidVisitor, uueid, sourceType);
                isoSurface(uueid, sourceType);
                if(sourceType == SOURCE_TYPE.SOURCE_TYPE_RAIL) {
                    insertExpositionRailwayTable(new EmptyProgressVisitor(), uueid);
                } else {
                    insertExpositionRoadsTable(new EmptyProgressVisitor(), uueid);
                }
            }
        }

        logger.info("Write output tables");
        for(String tableName : outputTable) {
            // Export result tables as GeoJSON
            sql.execute("CALL GEOJSONWRITE('" + new File(outputFolder,  outputPrefix + tableName + ".geojson").getAbsolutePath()+"', '" + tableName + "', TRUE);");
        }
    }

    public static Double asDouble(Object v) {
        if(v instanceof Number) {
            return ((Number)v).doubleValue();
        } else {
            return null;
        }
    }

    public static Integer asInteger(Object v) {
        if(v instanceof Number) {
            return ((Number)v).intValue();
        } else {
            return null;
        }
    }

    /**
     * Evalute SQL scripts passed in parameters, and replace {$keyName} into the script by the provided map entries
     * @param script
     * @param valuesMap
     * @param progressVisitor
     * @throws IOException
     * @throws SQLException
     */
    public void executeScript(InputStream script, Map<String, String> valuesMap, ProgressVisitor progressVisitor) throws IOException, SQLException {
        StringSubstitutor stringSubstitutor = new StringSubstitutor(valuesMap);
        if(script != null) {
            List<String> statementList = new LinkedList<>();
            try(InputStreamReader reader  = new InputStreamReader(script)) {
                ScriptReader scriptReader = new ScriptReader(reader);
                scriptReader.setSkipRemarks(true);
                String statement = scriptReader.readStatement();
                while (statement != null && !statement.trim().isEmpty()) {
                    statementList.add(stringSubstitutor.replace(statement));
                    statement = scriptReader.readStatement();
                }
            }
            int idStatement = 0;
            final int nbStatements = statementList.size();
            ProgressVisitor evalProgress = progressVisitor.subProcess(nbStatements);
            Statement st = connection.createStatement();
            for(String statement : statementList) {
                logger.info(String.format(Locale.ROOT, "%d/%d %s", (idStatement++) + 1, nbStatements, statement.trim()));
                st.execute(statement);
                evalProgress.endStep();
                if(evalProgress.isCanceled()) {
                    throw new SQLException("Canceled by user");
                }
            }
        }
    }

    private static class CbsSplitedEntry {
        double noiseLevel;
        double cellIntersectionArea;

        public CbsSplitedEntry(double noiseLevel, double cellIntersectionArea) {
            this.noiseLevel = noiseLevel;
            this.cellIntersectionArea = cellIntersectionArea;
        }
    }


    private static class CbsIntersectedEntry {
        PointNoiseMap.CellIndex mainIndex;
        PointNoiseMap.CellIndex cellIndex;
        CbsSplitedEntry cbsSplitedEntry;

        public CbsIntersectedEntry(PointNoiseMap.CellIndex mainIndex, PointNoiseMap.CellIndex cellIndex,
                                   CbsSplitedEntry cbsSplitedEntry) {
            this.mainIndex = mainIndex;
            this.cellIndex = cellIndex;
            this.cbsSplitedEntry = cbsSplitedEntry;
        }
    }

    private static class IsoEntry {
        int index;
        Geometry cell;
        String isoLvl;

        public IsoEntry(int index, Geometry cell, String isoLvl) {
            this.index = index;
            this.cell = cell;
            this.isoLvl = isoLvl;
        }
    }

    /**
     * Merge UUEID in CBS over a regular grid
     * @param connection
     * @return
     */
    public static List<String> mergeCBS(Connection connection, int gridSize, int mainGridSize, ProgressVisitor progressVisitor) throws SQLException {
        Logger logger = LoggerFactory.getLogger(NoiseModellingInstance.class);
        AtomicBoolean exceptionPrinted = new AtomicBoolean(false);
        List<String> allTables = JDBCUtilities.getTableNames(connection, null, null, null,
                new String[]{"TABLE"});
        GeometryFactory geometyFactory = new GeometryFactory();
        Map<String, Double[]> isoLabelToLevel = NoiseModellingInstance.getIntervals();
        ArrayList<String> outputTables = new ArrayList<>();
        ArrayList<TableLocation> cbsTables = new ArrayList<>();
        for(String tableName : allTables) {
            TableLocation tableLocation = TableLocation.parse(tableName, DBTypes.H2GIS);
            if (!tableLocation.getTable().endsWith("_MERGED") && //not already merged
                    (tableLocation.getTable().startsWith("CBS_"))) { // CBS Table
                List<String> fields = JDBCUtilities.getColumnNames(connection, tableLocation);
                if (!(fields.contains("UUEID") && fields.contains("PERIOD") && fields.contains("NOISELEVEL"))) {
                    continue;
                }
                cbsTables.add(tableLocation);
            }
        }
        ProgressVisitor tableProgress = progressVisitor.subProcess(cbsTables.size());
        for(TableLocation tableLocation : cbsTables) {
            String outputTable = tableLocation.getTable() + "_MERGED";
            try(Statement st = connection.createStatement()) {
                Envelope tableExtent = GeometryTableUtilities.getEstimatedExtent(connection, tableLocation).getEnvelopeInternal();
                double minX = tableExtent.getMinX();
                double minY = tableExtent.getMinY();
                double cellHeight = tableExtent.getHeight();
                int maxJ = (int)Math.ceil(cellHeight / gridSize);
                Map<PointNoiseMap.CellIndex, ArrayList<CbsIntersectedEntry>> cellIndices = new HashMap<>();
                long startMerge = System.currentTimeMillis();
                final int indexFactor = mainGridSize / gridSize;

                ProgressVisitor tableProcessing = tableProgress.subProcess(2);
                int insertedPolygons = 0;
                Map<String, ArrayList<Polygon>> geometriesToGrid = new HashMap<>();
                try(ResultSet rs = st.executeQuery("SELECT THE_GEOM, NOISELEVEL FROM " + tableLocation)) {
                    while (rs.next()) {
                        Geometry inputGeometry = (Geometry) rs.getObject(1);
                        ArrayList<Polygon> polygonsList = geometriesToGrid.computeIfAbsent(rs.getString(2),
                                s -> new ArrayList<>(Math.max(10, inputGeometry.getNumGeometries())));
                        for (int idGeometry = 0; idGeometry < inputGeometry.getNumGeometries(); idGeometry++) {
                            insertedPolygons++;
                            Geometry geom = inputGeometry.getGeometryN(idGeometry);
                            if(geom instanceof Polygon) {
                                polygonsList.add((Polygon) geom);
                            }
                        }
                    }
                }
                int nbPolygons = geometriesToGrid.values().stream().
                        mapToInt(ArrayList::size).sum();
                logger.info(String.format(Locale.ROOT, "Collect cells of %s from %d polygons..",tableLocation
                        , nbPolygons));
                ProgressVisitor geomLoading = tableProcessing.subProcess(nbPolygons);
                geometriesToGrid.forEach((s, polygons) -> {
                    Double noiseLevel = isoLabelToLevel.get(s)[1]; //take mid range
                    ConcurrentLinkedDeque<CbsIntersectedEntry> entries = new ConcurrentLinkedDeque<>();
                    polygons.parallelStream().forEach(triangle -> {
                        PreparedPolygon preparedPolygon = new PreparedPolygon(triangle);
                        Envelope triEnv = triangle.getEnvelopeInternal();
                        int startI = (int) ((triEnv.getMinX() - minX) / gridSize);
                        int startJ = (int) ((triEnv.getMinY() - minY) / gridSize);
                        int endI = (int) Math.ceil((triEnv.getMaxX() - minX) / gridSize);
                        int endJ = (int) Math.ceil((triEnv.getMaxY() - minY) / gridSize);
                        for (int i = startI; i < endI; i++) {
                            for (int j = startJ; j < endJ; j++) {
                                PointNoiseMap.CellIndex cell = new PointNoiseMap.CellIndex(i, j);
                                int mainI = cell.getLongitudeIndex() / indexFactor;
                                int mainJ = cell.getLatitudeIndex() / indexFactor;
                                PointNoiseMap.CellIndex key = new PointNoiseMap.CellIndex(mainI, mainJ);
                                double x1 = minX + (double) cell.getLongitudeIndex() * gridSize;
                                double y1 = minY + (double) cell.getLatitudeIndex() * gridSize;
                                double x2 = minX + (double) (cell.getLongitudeIndex() + 1) * gridSize;
                                double y2 = minY + (double) (cell.getLatitudeIndex() + 1) * gridSize;
                                Polygon gridCell = geometyFactory.createPolygon(new Coordinate[]{new Coordinate(x1, y1), new Coordinate(x2, y1), new Coordinate(x2, y2), new Coordinate(x1, y2), new Coordinate(x1, y1)});
                                if (preparedPolygon.intersects(gridCell)) {
                                    OverlayOp overlayOp = new OverlayOp(gridCell, triangle);
                                    try {
                                        Geometry intersectedGeom = overlayOp.getResultGeometry(OverlayOp.INTERSECTION);
                                        if (!intersectedGeom.isEmpty()) {
                                            CbsSplitedEntry cbsSplitedEntry = new CbsSplitedEntry(noiseLevel, intersectedGeom.getArea());
                                            entries.add(new CbsIntersectedEntry(key, cell, cbsSplitedEntry));
                                        }
                                    } catch (TopologyException ex) {
                                        // Rare case of topology exception while doing intersection
                                        // no other choice than ignoring this polygon
                                        if(exceptionPrinted.compareAndSet(false, true)) {
                                            logger.warn(ex.getLocalizedMessage(), ex);
                                        }
                                    }
                                }
                            }
                        }
                        geomLoading.endStep();
                    });
                    polygons.clear();
                    for (CbsIntersectedEntry intersectedEntry : entries) {
                        ArrayList<CbsIntersectedEntry> ar = cellIndices.computeIfAbsent(intersectedEntry.mainIndex, k -> new ArrayList<>());
                        ar.add(intersectedEntry);
                    }
                });
                // Iterate over grid
                int srid = GeometryTableUtilities.getSRID(connection, tableLocation);
                st.execute("DROP TABLE IF EXISTS " + outputTable);
                st.execute("CREATE TABLE "+outputTable+"(ID INTEGER, THE_GEOM GEOMETRY(MULTIPOLYGON,"+srid+"), NOISELEVEL VARCHAR(20))");

                boolean isDay = tableLocation.getTable().contains("_LD_");
                boolean isTypeA = tableLocation.getTable().startsWith("CBS_A_");
                boolean isTrainConventional = tableLocation.getTable().contains("_CONV_");

                long endCells = System.currentTimeMillis();
                logger.info(String.format(Locale.ROOT, "Collect cells in %d ms. Final polygons count is %d" +
                        " Compute intersection of iso-cells..", (int)(endCells - startMerge) , insertedPolygons));
                Deque<IsoEntry> deque = new ConcurrentLinkedDeque<>();
                ProgressVisitor mainGridProgress = tableProcessing.subProcess(cellIndices.size());
                cellIndices.entrySet().parallelStream().forEach(mainGridEntry -> {
                    Map<String, ArrayList<IsoEntry>> mainCellsPolys = new HashMap<>();
                    Map<PointNoiseMap.CellIndex, ArrayList<CbsSplitedEntry>> cbsPerCellIndex = new HashMap<>();
                    // Collect all cbs that share the same cell
                    for (CbsIntersectedEntry cbsIntersectedEntry : mainGridEntry.getValue()) {
                        ArrayList<CbsSplitedEntry> cbsForThisCellIndex = cbsPerCellIndex.computeIfAbsent(cbsIntersectedEntry.cellIndex, k -> new ArrayList<>());
                        cbsForThisCellIndex.add(cbsIntersectedEntry.cbsSplitedEntry);
                    }
                    // Sum the power of all geometries inside this cell
                    cbsPerCellIndex.forEach((cellIndex, cbsSplitedEntries) -> {
                        double x1 = minX + (double) cellIndex.getLongitudeIndex() * gridSize;
                        double y1 = minY + (double) cellIndex.getLatitudeIndex() * gridSize;
                        double x2 = minX + (double) (cellIndex.getLongitudeIndex() + 1) * gridSize;
                        double y2 = minY + (double) (cellIndex.getLatitudeIndex() + 1) * gridSize;
                        Polygon gridCell = geometyFactory.createPolygon(new Coordinate[]{new Coordinate(x1, y1), new Coordinate(x2, y1), new Coordinate(x2, y2), new Coordinate(x1, y2), new Coordinate(x1, y1)});
                        gridCell.setSRID(srid);
                        int cellId = cellIndex.getLongitudeIndex() * maxJ + cellIndex.getLatitudeIndex();
                        double area = gridCell.getArea();
                        double sumPower = 0;
                        for (CbsSplitedEntry cbsSplitedEntry : cbsSplitedEntries) {
                            double intersectionArea = cbsSplitedEntry.cellIntersectionArea;
                            sumPower += PowerUtils.dbaToW(cbsSplitedEntry.noiseLevel) * (intersectionArea / area);
                        }
                        if (sumPower > 0) {
                            // insert entry
                            double summedNoiseLevel = PowerUtils.wToDba(sumPower);
                            String noiseLevel = "";
                            if (isTypeA) {
                                if (isDay) {
                                    if (summedNoiseLevel < 60) {
                                        noiseLevel = "Lden5559";
                                    } else if (summedNoiseLevel < 65) {
                                        noiseLevel = "Lden6064";

                                    } else if (summedNoiseLevel < 70) {
                                        noiseLevel = "Lden6569";

                                    } else if (summedNoiseLevel < 75) {
                                        noiseLevel = "Lden7074";

                                    } else {
                                        noiseLevel = "LdenGreaterThan75";
                                    }
                                } else {
                                    if (summedNoiseLevel < 55) {
                                        noiseLevel = "Lnight5054";
                                    } else if (summedNoiseLevel < 60) {
                                        noiseLevel = "Lnight5559";

                                    } else if (summedNoiseLevel < 65) {
                                        noiseLevel = "Lnight6064";

                                    } else if (summedNoiseLevel < 70) {
                                        noiseLevel = "Lnight6569";
                                    } else {
                                        noiseLevel = "LnightGreaterThan70";
                                    }
                                }
                            } else {
                                // Type C map
                                if (isDay) {
                                    if (!isTrainConventional) {
                                        // LGV or roads type C
                                        if (summedNoiseLevel > 68) {
                                            noiseLevel = "LdenGreaterThan68";
                                        } else {
                                            noiseLevel = "";
                                        }
                                    } else {
                                        // Fer. Conv.
                                        if (summedNoiseLevel > 73) {
                                            noiseLevel = "LdenGreaterThan73";
                                        } else {
                                            noiseLevel = "";
                                        }
                                    }
                                } else {
                                    if (!isTrainConventional) {
                                        // LGV or roads type C
                                        if (summedNoiseLevel > 62) {
                                            noiseLevel = "LnightGreaterThan62";
                                        } else {
                                            noiseLevel = "";
                                        }
                                    } else {
                                        // Fer. Conv.
                                        if (summedNoiseLevel > 65) {
                                            noiseLevel = "LnightGreaterThan65";
                                        } else {
                                            noiseLevel = "";
                                        }
                                    }
                                }
                            }
                            if (!noiseLevel.isEmpty()) {
                                IsoEntry isoEntry = new IsoEntry(cellId, gridCell, noiseLevel);
                                ArrayList<IsoEntry> isoList = mainCellsPolys.putIfAbsent(noiseLevel, new ArrayList<>(Collections.singleton(isoEntry)));
                                if (isoList != null) {
                                    isoList.add(isoEntry);
                                }
                            }
                        }
                    });
                    mainCellsPolys.forEach((isoLevel, isoEntries) -> {
                        List<Polygon> multiPoly = new ArrayList<>(isoEntries.size());
                        for(IsoEntry isoEntry : isoEntries) {
                            Geometry cell = isoEntry.cell;
                            for(int idGeom = 0; idGeom < cell.getNumGeometries(); idGeom++) {
                                Geometry subGeom = cell.getGeometryN(idGeom);
                                if(subGeom instanceof Polygon) {
                                    multiPoly.add((Polygon) subGeom);
                                }
                            }
                        }
                        // The best situation for using buffer(0) is the trivial case where there is no overlap between the input geometries.
                        // this is the case here
                        Geometry unionGeom = geometyFactory.createMultiPolygon(multiPoly.toArray(new Polygon[0])).buffer(0);
                        int mainIndex = (maxJ / indexFactor) * mainGridEntry.getKey().getLongitudeIndex() +
                                mainGridEntry.getKey().getLatitudeIndex();
                        deque.add(new IsoEntry(mainIndex, unionGeom, isoLevel));

                    });
                    mainGridProgress.endStep();
                });
                // Insert entries into the database
                long endOfIntersects = System.currentTimeMillis();
                logger.info(String.format(Locale.ROOT, "Intersection of iso-cells in %d ms." +
                        " Begin insertion in database..", (int)(endOfIntersects - endCells)));
                PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO "+outputTable+" VALUES (?, ?, ?)");
                int batchSize = 0;
                for(IsoEntry isoEntry : deque) {
                    Geometry geom = isoEntry.cell;
                    if(geom instanceof Polygon) {
                        geom = geometyFactory.createMultiPolygon(new Polygon[]{(Polygon) geom});
                    }
                    geom.setSRID(srid);
                    insertStatement.setInt(1, isoEntry.index);
                    insertStatement.setObject(2, geom);
                    insertStatement.setString(3, isoEntry.isoLvl);
                    insertStatement.addBatch();
                    batchSize++;
                    if (batchSize >= BATCH_MAX_SIZE) {
                        insertStatement.executeBatch();
                        batchSize = 0;
                    }
                }
                if (batchSize > 0) {
                    insertStatement.executeBatch();
                }
                long endOfInsertion = System.currentTimeMillis();
                outputTables.add(outputTable);
                logger.info(String.format(Locale.ROOT,
                        "NoiseMap grid cell inserted into %s. Total operation time in %d ms" ,
                        outputTable,(int)(endOfInsertion - startMerge)));
            }

        }
        return outputTables;
    }

    public static Map<String, Double[]> getIntervals() {
        Map<String, Double[]> levelRoadsInterval = new TreeMap<>();
        levelRoadsInterval.put("Lden5559", new Double[]{55.0, 57.5, 60.0});
        levelRoadsInterval.put("Lden6064",  new Double[]{60.0, 62.5, 65.0});
        levelRoadsInterval.put("Lden6569",  new Double[]{65.0, 67.5, 70.0});
        levelRoadsInterval.put("Lden7074",  new Double[]{70.0, 72.5, 75.0});
        levelRoadsInterval.put("LdenGreaterThan75",  new Double[]{75.0, 77.5, 200.0});
        levelRoadsInterval.put("Lnight5054",  new Double[]{50.0, 52.5, 55.0});
        levelRoadsInterval.put("Lnight5559", new Double[]{55.0, 57.5, 60.0});
        levelRoadsInterval.put("Lnight6064", new Double[]{60.0, 62.5, 65.0});
        levelRoadsInterval.put("Lnight6569",  new Double[]{65.0, 67.5, 70.0});
        levelRoadsInterval.put("LnightGreaterThan70", new Double[]{70.0, 72.5, 200.0});
        levelRoadsInterval.put("LnightGreaterThan62", new Double[]{62.0, 64.5, 200.0});
        levelRoadsInterval.put("LdenGreaterThan68", new Double[]{68.0, 70.5, 200.0});
        levelRoadsInterval.put("LdenGreaterThan73", new Double[]{73.0, 75.5, 200.0});
        levelRoadsInterval.put("LnightGreaterThan65", new Double[]{65.0, 67.5, 200.0});
        return levelRoadsInterval;
    }

    public void createExpositionTables(ProgressVisitor progressVisitor, List<String> roadsUUEID, List<String> railsUUEID) throws SQLException, IOException {
        // push uueids
        String[] levelsRoadsExcludingAgglo = new String[] {"Lden5559", "Lden6064", "Lden6569", "Lden7074",
                "LdenGreaterThan75", "Lnight5054", "Lnight5559", "Lnight6064", "Lnight6569",
                "LnightGreaterThan70"};

        String[] levelsRoadsIncludingAgglo = new String[] {"Lden5559", "Lden6064", "Lden6569", "Lden7074",
                "LdenGreaterThan75", "Lnight5054", "Lnight5559", "Lnight6064", "Lnight6569",
                "LnightGreaterThan70", "Lden55", "Lden65", "Lden75", "LdenGreaterThan68", "LnightGreaterThan62"};

        String[] levelsTrainLGVExcludingAgglo = new String[] {"Lden5559", "Lden6064", "Lden6569", "Lden7074",
                "LdenGreaterThan75","LdenGreaterThan73", "Lnight5054", "Lnight5559", "Lnight6064", "Lnight6569",
                "LnightGreaterThan70"};

        String[] levelsTrainLGVIncludingAgglo = new String[] {
                "Lden5559",
                "Lden6064",
                "Lden6569",
                "Lden7074",
                "LdenGreaterThan75",
                "Lden55",
                "Lden65",
                "Lden75",
                "Lnight5054",
                "Lnight5559",
                "Lnight6064",
                "Lnight6569",
                "LnightGreaterThan70",
                "LdenGreaterThan68",
                "LnightGreaterThan62"
        };

        String[] levelsTrainConvExcludingAgglo = new String[] {
                "Lden5559",
                "Lden6064",
                "Lden6569",
                "Lden7074",
                "LdenGreaterThan75",
                "Lnight5054",
                "Lnight5559",
                "Lnight6064",
                "Lnight6569",
                "LnightGreaterThan70"
        };

        String[] levelsTrainConvIncludingAgglo = new String[] {
                "Lden5559",
                "Lden6064",
                "Lden6569",
                "Lden7074",
                "LdenGreaterThan75",
                "Lden55",
                "Lden65",
                "Lden75",
                "Lnight5054",
                "Lnight5559",
                "Lnight6064",
                "Lnight6569",
                "LnightGreaterThan70",
                "LdenGreaterThan73",
                "LnightGreaterThan65"
        };

        Map<String, Double[]> levelRoadsInterval = getIntervals();
        Sql sql = new Sql(connection);

        Statement st = connection.createStatement();
        st.execute("DROP TABLE IF EXISTS UUEIDS_LEVELS");
        st.execute("CREATE TABLE UUEIDS_LEVELS(UUEID VARCHAR, NOISELEVEL VARCHAR, exposureType VARCHAR," +
                " MIN_LAEQ DOUBLE DEFAULT(0), INTERVAL_LAEQ DOUBLE DEFAULT(0), MAX_LAEQ DOUBLE DEFAULT(0))");
        PreparedStatement ps = connection.prepareStatement("INSERT INTO UUEIDS_LEVELS VALUES (?, ?, ?, ?, ?, ?)");
        String exposureType = "mostExposedFacade";
        for(String roadUUEID : roadsUUEID) {
            for(String level : levelsRoadsExcludingAgglo) {
                Double[] levelIntervals = levelRoadsInterval.getOrDefault(level, null);
                ps.setString(1, roadUUEID);
                ps.setString(2, level);
                ps.setString(3, exposureType);
                ps.setDouble(4, levelIntervals == null ? Double.NaN : levelIntervals[0]);
                ps.setDouble(5, levelIntervals == null ? Double.NaN : levelIntervals[1]);
                ps.setDouble(6, levelIntervals == null ? Double.NaN : levelIntervals[2]);
                ps.execute();
            }
        }
        for(String railUUEID : railsUUEID) {
            int typeLigne = Integer.parseInt((String)sql.firstRow(Collections.singletonMap("uueid", railUUEID),
                    "SELECT LINETYPE  FROM RAIL_SECTIONS WHERE UUEID=:uueid LIMIT 1").getAt(0));
            String[] levelsRails = typeLigne == 1 ? levelsTrainConvExcludingAgglo : levelsTrainLGVExcludingAgglo;
            for(String level : levelsRails) {
                Double[] levelIntervals = levelRoadsInterval.getOrDefault(level, null);
                ps.setString(1, railUUEID);
                ps.setString(2, level);
                ps.setString(3, exposureType);
                ps.setDouble(4, levelIntervals == null ? Double.NaN : levelIntervals[0]);
                ps.setDouble(5, levelIntervals == null ? Double.NaN : levelIntervals[1]);
                ps.setDouble(6, levelIntervals == null ? Double.NaN : levelIntervals[2]);
                ps.execute();
            }
        }
        exposureType = "mostExposedFacadeIncludingAgglomeration";
        for(String roadUUEID : roadsUUEID) {
            for(String level : levelsRoadsIncludingAgglo) {
                Double[] levelIntervals = levelRoadsInterval.getOrDefault(level, null);
                ps.setString(1, roadUUEID);
                ps.setString(2, level);
                ps.setString(3, exposureType);
                ps.setDouble(4, levelIntervals == null ? Double.NaN : levelIntervals[0]);
                ps.setDouble(5, levelIntervals == null ? Double.NaN : levelIntervals[1]);
                ps.setDouble(6, levelIntervals == null ? Double.NaN : levelIntervals[2]);
                ps.execute();
            }
        }
        for(String railUUEID : railsUUEID) {
            int typeLigne = Integer.parseInt((String)sql.firstRow(Collections.singletonMap("uueid", railUUEID),
                    "SELECT LINETYPE  FROM RAIL_SECTIONS WHERE UUEID=:uueid LIMIT 1").getAt(0));
            String[] levelsRails = typeLigne == 1 ? levelsTrainConvIncludingAgglo : levelsTrainLGVIncludingAgglo;
            for(String level : levelsRails) {
                Double[] levelIntervals = levelRoadsInterval.getOrDefault(level, null);
                ps.setString(1, railUUEID);
                ps.setString(2, level);
                ps.setString(3, exposureType);
                ps.setDouble(4, levelIntervals == null ? Double.NaN : levelIntervals[0]);
                ps.setDouble(5, levelIntervals == null ? Double.NaN : levelIntervals[1]);
                ps.setDouble(6, levelIntervals == null ? Double.NaN : levelIntervals[2]);
                ps.execute();
            }
        }

        Map<String, String> valuesMap = new HashMap<>();
        try(InputStream s = NoiseModellingInstance.class.getResourceAsStream("create_indicators.sql")) {
            executeScript(s, valuesMap, progressVisitor);
        }
    }

    public void insertExpositionRoadsTable(ProgressVisitor progressVisitor, String uueid) throws SQLException, IOException {
        Map<String, String> valuesMap = new HashMap<>();
        valuesMap.put("UUEID", uueid);
        try(InputStream s = NoiseModellingInstance.class.getResourceAsStream("output_indicators_roads.sql")) {
            executeScript(s, valuesMap, progressVisitor);
        }
    }


    public void insertExpositionRailwayTable(ProgressVisitor progressVisitor, String uueid) throws SQLException, IOException {
        Map<String, String> valuesMap = new HashMap<>();
        valuesMap.put("UUEID", uueid);
        try(InputStream s = NoiseModellingInstance.class.getResourceAsStream("output_indicators_rails.sql")) {
            executeScript(s, valuesMap, progressVisitor);
        }
    }
    /**
     *
     * @param sourceType Road = 0 Rail=1
     * @return CBS Tables
     * @throws SQLException
     */
    public List<String> recreateCBS(SOURCE_TYPE sourceType) throws SQLException {

        List<String> outputTables = new ArrayList<>();

        // List of input tables : inputTable

        Sql sql = new Sql(connection);

        // -------------------
        // Initialisation des tables dans lesquelles on stockera les surfaces par tranche d'iso, par type d'infra et d'indice

        String nuts = (String)sql.firstRow("SELECT NUTS FROM METADATA").get("NUTS");
        cbsARoadLden = "CBS_A_R_LD_"+nuts;
        cbsARoadLnight = "CBS_A_R_LN_"+nuts;
        cbsAFerLden = "CBS_A_F_LD_"+nuts;
        cbsAFerLnight = "CBS_A_F_LN_"+nuts;

        cbsCRoadLden = "CBS_C_R_LD_"+nuts;
        cbsCRoadLnight = "CBS_C_R_LN_"+nuts;
        cbsCFerLGVLden = "CBS_C_F_LGV_LD_"+nuts;
        cbsCFerLGVLnight = "CBS_C_F_LGV_LN_"+nuts;
        cbsCFerCONVLden = "CBS_C_F_CONV_LD_"+nuts;
        cbsCFerCONVLnight = "CBS_C_F_CONV_LN_"+nuts;

        // output string, the information given back to the user
        StringBuilder resultString = new StringBuilder("Le processus est terminé - Les tables de sortie sont ");



        // Tables are created according to the input parameter "rail" or "road"
        if (sourceType == SOURCE_TYPE.SOURCE_TYPE_RAIL){
            outputTables.add(cbsAFerLden);
            outputTables.add(cbsAFerLnight);
            outputTables.add(cbsCFerLGVLden);
            outputTables.add(cbsCFerLGVLnight);
            outputTables.add(cbsCFerCONVLden);
            outputTables.add(cbsCFerCONVLnight);
            // For A maps
            sql.execute("DROP TABLE IF EXISTS "+ cbsAFerLden);
            sql.execute("CREATE TABLE "+ cbsAFerLden +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
            sql.execute("DROP TABLE IF EXISTS "+ cbsAFerLnight);
            sql.execute("CREATE TABLE "+ cbsAFerLnight +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");

            // For C maps
            sql.execute("DROP TABLE IF EXISTS "+ cbsCFerLGVLden);
            sql.execute("CREATE TABLE "+ cbsCFerLGVLden +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
            sql.execute("DROP TABLE IF EXISTS "+ cbsCFerLGVLnight);
            sql.execute("CREATE TABLE "+ cbsCFerLGVLnight +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");

            sql.execute("DROP TABLE IF EXISTS "+ cbsCFerCONVLden);
            sql.execute("CREATE TABLE "+ cbsCFerCONVLden +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
            sql.execute("DROP TABLE IF EXISTS "+ cbsCFerCONVLnight);
            sql.execute("CREATE TABLE "+ cbsCFerCONVLnight +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");


            sql.execute("DROP TABLE IF EXISTS "+ cbsCFerLGVLden);
            sql.execute("CREATE TABLE "+ cbsCFerLGVLden +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
            sql.execute("DROP TABLE IF EXISTS "+ cbsCFerLGVLnight);
            sql.execute("CREATE TABLE "+ cbsCFerLGVLnight +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");

        } else{
            outputTables.add(cbsARoadLden);
            outputTables.add(cbsARoadLnight);
            outputTables.add(cbsCRoadLden);
            outputTables.add(cbsCRoadLnight);
            // For A maps
            sql.execute("DROP TABLE IF EXISTS "+ cbsARoadLden);
            sql.execute("CREATE TABLE "+ cbsARoadLden +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
            sql.execute("DROP TABLE IF EXISTS "+ cbsARoadLnight);
            sql.execute("CREATE TABLE "+ cbsARoadLnight +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
            // For C maps
            sql.execute("DROP TABLE IF EXISTS "+ cbsCRoadLden);
            sql.execute("CREATE TABLE "+ cbsCRoadLden +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
            sql.execute("DROP TABLE IF EXISTS "+ cbsCRoadLnight);
            sql.execute("CREATE TABLE "+ cbsCRoadLnight +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
        }
        for(String outputTable : outputTables) {
            resultString.append(", ").append(outputTable);
        }

        logger.info(resultString.toString());
        return outputTables;
    }

    void generateIsoSurfaces(String inputTable, List<Double> isoClasses, Connection connection, String uueid,
                             String cbsType, String period, SOURCE_TYPE sourceType) throws SQLException {

        if(!JDBCUtilities.tableExists(connection, inputTable)) {
            logger.info("La table "+inputTable+" n'est pas présente");
            return;
        }
        DBTypes dbTypes = DBUtils.getDBType(connection);
        int srid = GeometryTableUtilities.getSRID(connection, TableLocation.parse(inputTable, dbTypes));

        BezierContouring bezierContouring = new BezierContouring(isoClasses, srid);

        bezierContouring.setPointTable(inputTable);
        bezierContouring.setTriangleTable("TRIANGLES_DELAUNAY");
        bezierContouring.setSmooth(false);

        bezierContouring.createTable(connection);

        Sql sql = new Sql(connection);

        // Forces the SRID, as it is lost in the previous steps
        sql.execute("UPDATE CONTOURING_NOISE_MAP SET THE_GEOM=ST_SetSRID(THE_GEOM, (SELECT SRID FROM METADATA))");

        // Generate temporary table to store ISO areas
        sql.execute("DROP TABLE IF EXISTS ISO_AREA");
        sql.execute("CREATE TABLE ISO_AREA (the_geom geometry, pk varchar, UUEID varchar, CBSTYPE varchar, PERIOD varchar, noiselevel varchar, AREA float) AS SELECT ST_ACCUM(the_geom) the_geom, null, '"+uueid+"', '"+cbsType+"', '"+period+"', ISOLABEL, SUM(ST_AREA(the_geom)) AREA FROM CONTOURING_NOISE_MAP GROUP BY ISOLABEL");

        // For A maps
        if(cbsType.equalsIgnoreCase("A")) {
            if(period.equalsIgnoreCase("LD")) {
                // Update noise classes for LDEN
                sql.execute("UPDATE ISO_AREA SET NOISELEVEL = (CASE WHEN NOISELEVEL = '55-60' THEN 'Lden5559' WHEN NOISELEVEL = '60-65' THEN 'Lden6064' WHEN NOISELEVEL = '65-70' THEN 'Lden6569' WHEN NOISELEVEL = '70-75' THEN 'Lden7074' WHEN NOISELEVEL = '> 75' THEN 'LdenGreaterThan75' END);");
            } else {
                // Update noise classes for LNIGHT
                sql.execute("UPDATE ISO_AREA SET NOISELEVEL = (CASE WHEN NOISELEVEL = '50-55' THEN 'Lnight5054' WHEN NOISELEVEL = '55-60' THEN 'Lnight5559' WHEN NOISELEVEL = '60-65' THEN 'Lnight6064' WHEN NOISELEVEL = '65-70' THEN 'Lnight6569' WHEN NOISELEVEL = '> 70' THEN 'LnightGreaterThan70' END);");
            }
        } else {
            // For C maps
            // Update noise classes for LDEN
            if(period.equalsIgnoreCase("LD")) {
                sql.execute("UPDATE ISO_AREA SET NOISELEVEL = (CASE WHEN NOISELEVEL = '> 68' THEN 'LdenGreaterThan68' WHEN NOISELEVEL = '> 73' THEN 'LdenGreaterThan73' END);");
            } else {
                // Update noise classes for LNIGHT
                sql.execute("UPDATE ISO_AREA SET NOISELEVEL = (CASE WHEN NOISELEVEL = '> 62' THEN 'LnightGreaterThan62' WHEN NOISELEVEL = '> 65' THEN 'LnightGreaterThan65' END);");
            }
        }

        sql.execute("DELETE FROM ISO_AREA WHERE NOISELEVEL IS NULL");

        // Generate the PK
        sql.execute("UPDATE ISO_AREA SET pk = CONCAT(uueid, '_',noiselevel)");
        // Forces the SRID, as it is lost in the previous steps
        sql.execute("UPDATE ISO_AREA SET THE_GEOM = ST_SetSRID(THE_GEOM, (SELECT SRID FROM METADATA))");

        // Insert iso areas into common table, according to rail or road input parameter
        // The noiselevel field will filtering for A or D
        sql.execute("UPDATE POPULATION_EXPOSURE SET " +
                "EXPOSEDAREA = COALESCE((SELECT SUM(ST_AREA(THE_GEOM)) / 1e6 TOTAREA_SQKM FROM ISO_AREA I" +
                " WHERE I.noiselevel = POPULATION_EXPOSURE.noiselevel), EXPOSEDAREA)  WHERE UUEID = '" + uueid + "' " +
                "AND EXPOSURETYPE = 'mostExposedFacadeIncludingAgglomeration'");
        if (sourceType == SOURCE_TYPE.SOURCE_TYPE_RAIL){
            if(cbsType.equalsIgnoreCase("A")) {
                if(period.equalsIgnoreCase("LD")) {
                    sql.execute("INSERT INTO " + cbsAFerLden + " SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA");
                } else {
                    sql.execute("INSERT INTO " + cbsAFerLnight + " SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA");
                }
            } else {
                if(period.equalsIgnoreCase("LD")) {
                    sql.execute("INSERT INTO " + cbsCFerLGVLden + " SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE NOISELEVEL = 'LdenGreaterThan68'");
                    sql.execute("INSERT INTO " + cbsCFerCONVLden + " SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE NOISELEVEL = 'LdenGreaterThan73'");
                } else {
                    sql.execute("INSERT INTO " + cbsCFerLGVLnight + " SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE NOISELEVEL = 'LnightGreaterThan62'");
                    sql.execute("INSERT INTO " + cbsCFerCONVLnight + " SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE NOISELEVEL = 'LnightGreaterThan65'");
                }
            }
        } else {
            if(cbsType.equalsIgnoreCase("A")) {
                if(period.equalsIgnoreCase("LD")) {
                    sql.execute("INSERT INTO " + cbsARoadLden + " SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA");
                } else {
                    sql.execute("INSERT INTO " + cbsARoadLnight + " SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA");
                }
            } else {
                if(period.equalsIgnoreCase("LD")) {
                    sql.execute("INSERT INTO " + cbsCRoadLden + " SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA");
                } else {
                    sql.execute("INSERT INTO " + cbsCRoadLnight + " SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA");
                }
            }
        }

        sql.execute("DROP TABLE IF EXISTS CONTOURING_NOISE_MAP, ISO_AREA");
        logger.info("End : Compute Isosurfaces");
    }

    public void isoSurface(String uueid, SOURCE_TYPE sourceType) throws SQLException {
        Sql sql = new Sql(connection);

        String tableDEN, tableNIGHT;
        if (sourceType== SOURCE_TYPE.SOURCE_TYPE_RAIL){
            tableDEN = "LDEN_RAILWAY";
            tableNIGHT = "LNIGHT_RAILWAY";
        } else{
            tableDEN = "LDEN_ROADS";
            tableNIGHT = "LNIGHT_ROADS";
        }

        String ldenOutput = uueid + "_CONTOURING_LDEN";
        String lnightOutput = uueid + "_CONTOURING_LNIGHT";

        sql.execute("DROP TABLE IF EXISTS "+ ldenOutput +", "+ lnightOutput +", RECEIVERS_DELAUNAY_NIGHT, RECEIVERS_DELAUNAY_DEN");

        logger.info(String.format("Create RECEIVERS_DELAUNAY_NIGHT for uueid= %s", uueid));
        sql.execute("create table RECEIVERS_DELAUNAY_NIGHT(PK INT NOT NULL, THE_GEOM GEOMETRY, LAEQ DECIMAL(6,2)) as SELECT RE.PK_1, RE.THE_GEOM, LAEQ FROM "+tableNIGHT+" L INNER JOIN RECEIVERS_UUEID RE ON L.IDRECEIVER = RE.PK WHERE RE.RCV_TYPE = 2;");
        sql.execute("ALTER TABLE RECEIVERS_DELAUNAY_NIGHT ADD PRIMARY KEY (PK)");
        logger.info(String.format("Create RECEIVERS_DELAUNAY_DEN for uueid= %s", uueid));
        sql.execute("create table RECEIVERS_DELAUNAY_DEN(PK INT NOT NULL, THE_GEOM GEOMETRY, LAEQ DECIMAL(6,2)) as SELECT RE.PK_1, RE.THE_GEOM, LAEQ FROM "+tableDEN+" L INNER JOIN RECEIVERS_UUEID RE ON L.IDRECEIVER = RE.PK WHERE RE.RCV_TYPE = 2;");
        sql.execute("ALTER TABLE RECEIVERS_DELAUNAY_DEN ADD PRIMARY KEY (PK)");



        logger.info("Generate iso surfaces");

        String ldenInput = "RECEIVERS_DELAUNAY_DEN";
        String lnightInput = "RECEIVERS_DELAUNAY_NIGHT";

        // For A maps
        // Produce isocontours for LNIGHT (LN)
        generateIsoSurfaces(lnightInput, isoLevelsLNIGHT, connection, uueid, "A", "LN", sourceType);
        // Produce isocontours for LDEN (LD)
        generateIsoSurfaces(ldenInput, isoLevelsLDEN, connection, uueid, "A", "LD", sourceType);

        // For C maps
        if (sourceType == SOURCE_TYPE.SOURCE_TYPE_RAIL){
            int typeLigne = Integer.parseInt((String)sql.firstRow(Collections.singletonMap("uueid", uueid),
                    "SELECT LINETYPE  FROM RAIL_SECTIONS WHERE UUEID=:uueid LIMIT 1").getAt(0));
            if(typeLigne == 1) // conventional
            {
                generateIsoSurfaces(lnightInput, isoCFerConvLevelsLNIGHT, connection, uueid, "C", "LN", sourceType);
                generateIsoSurfaces(ldenInput, isoCFerConvLevelsLDEN, connection, uueid, "C", "LD", sourceType);
            } else {
                generateIsoSurfaces(lnightInput, isoCFerLGVLevelsLNIGHT, connection, uueid, "C", "LN", sourceType);
                generateIsoSurfaces(ldenInput, isoCFerLGVLevelsLDEN, connection, uueid, "C", "LD", sourceType);
            }
        } else {
            // Produce isocontours for LNIGHT (LN)
            generateIsoSurfaces(lnightInput, isoCLevelsLNIGHT, connection, uueid, "C", "LN", sourceType);
            // Produce isocontours for LDEN (LD)
            generateIsoSurfaces(ldenInput, isoCLevelsLDEN, connection, uueid, "C", "LD", sourceType);
        }

        sql.execute("DROP TABLE IF EXISTS "+ldenInput + ", " + lnightInput);
    }

    public void doPropagation(ProgressVisitor progressLogger, String uueid, SOURCE_TYPE sourceType) throws SQLException, IOException {

        String sourceTable = "LW_UUEID";
        String receiversTable = "RECEIVERS_UUEID";

        DBTypes dbTypes = DBUtils.getDBType(connection);
        int sridBuildings = GeometryTableUtilities.getSRID(connection, TableLocation.parse("BUILDINGS_SCREENS", dbTypes));
        PointNoiseMap pointNoiseMap = new PointNoiseMap("BUILDINGS_SCREENS",
                sourceTable, receiversTable);


        LDENConfig ldenConfig_propa = new LDENConfig(LDENConfig.INPUT_MODE.INPUT_MODE_LW_DEN);

        Sql sql = new Sql(connection);
        sql.execute("UPDATE metadata SET road_start = NOW();");
        GroovyRowResult rs = sql.firstRow("SELECT * FROM CONF WHERE CONFID = ?", new Object[]{configurationId});
        int reflectionOrder = asInteger(rs.get("confreflorder"));
        int maxSrcDist = asInteger(rs.get("confmaxsrcdist"));
        int maxReflectionDistance = asInteger(rs.get("confmaxrefldist"));
        double wallAlpha = asDouble(rs.get("wall_alpha"));
        // overwrite with the system number of thread - 1
        Runtime runtime = Runtime.getRuntime();
        int nThread = Math.max(1, runtime.availableProcessors() - 1);

        boolean compute_vertical_edge_diffraction = (Boolean)rs.get("confdiffvertical");
        boolean compute_horizontal_edge_diffraction = (Boolean)rs.get("confdiffhorizontal");
        ldenConfig_propa.setComputeLDay(!(Boolean)rs.get("confskiplday"));
        ldenConfig_propa.setComputeLEvening(!(Boolean)rs.get("confskiplevening"));
        ldenConfig_propa.setComputeLNight(!(Boolean)rs.get("confskiplnight"));
        ldenConfig_propa.setComputeLDEN(!(Boolean)rs.get("confskiplden"));
        ldenConfig_propa.setMergeSources(true);
        if(sourceType == SOURCE_TYPE.SOURCE_TYPE_ROAD) {
            ldenConfig_propa.setlDayTable("LDAY_ROADS");
            ldenConfig_propa.setlEveningTable("LEVENING_ROADS");
            ldenConfig_propa.setlNightTable("LNIGHT_ROADS");
            ldenConfig_propa.setlDenTable("LDEN_ROADS");
        } else {
            ldenConfig_propa.setlDayTable("LDAY_RAILWAY");
            ldenConfig_propa.setlEveningTable("LEVENING_RAILWAY");
            ldenConfig_propa.setlNightTable("LNIGHT_RAILWAY");
            ldenConfig_propa.setlDenTable("LDEN_RAILWAY");
        }
        ldenConfig_propa.setComputeLAEQOnly(true);

        LDENPointNoiseMapFactory ldenProcessing = new LDENPointNoiseMapFactory(connection, ldenConfig_propa);
        ldenProcessing.insertTrainDirectivity();

        // setComputeVerticalDiffraction its vertical diffraction (over horizontal edges)
        pointNoiseMap.setComputeVerticalDiffraction(compute_horizontal_edge_diffraction);
        pointNoiseMap.setComputeHorizontalDiffraction(compute_vertical_edge_diffraction);
        pointNoiseMap.setSoundReflectionOrder(reflectionOrder);

        // Set environmental parameters
        GroovyRowResult row_zone = sql.firstRow("SELECT * FROM ZONE");
        String[] fieldTemperature = new String[] {"TEMP_D" ,"TEMP_E" ,"TEMP_N"};
        String[] fieldHumidity = new String[] {"HYGRO_D", "HYGRO_E", "HYGRO_N"};
        String[] fieldPFav = new String[] {"PFAV_06_18", "PFAV_18_22", "PFAV_22_06"};

        for(int idTime = 0; idTime < LDENConfig.TIME_PERIOD.values().length; idTime++) {
            PropagationProcessPathData environmentalData = new PropagationProcessPathData(false);
            double confHumidity = asDouble(row_zone.get(fieldHumidity[idTime]));
            double confTemperature = asDouble(row_zone.get(fieldTemperature[idTime]));
            String confFavorableOccurrences = (String) row_zone.get(fieldPFav[idTime]);
            environmentalData.setHumidity(confHumidity);
            environmentalData.setTemperature(confTemperature);
            StringTokenizer tk = new StringTokenizer(confFavorableOccurrences, ",");
            double[] favOccurrences = new double[PropagationProcessPathData.DEFAULT_WIND_ROSE.length];
            for (int i = 0; i < favOccurrences.length; i++) {
                favOccurrences[i] = Math.max(0, Math.min(1, Double.parseDouble(tk.nextToken().trim())));
            }
            environmentalData.setWindRose(favOccurrences);
            pointNoiseMap.setPropagationProcessPathData(LDENConfig.TIME_PERIOD.values()[idTime], environmentalData);
            logger.info("For " + fieldPFav[idTime] + " :");
            logger.info(String.format(Locale.ROOT, "Temperature: %.2f °C", confTemperature));
            logger.info(String.format(Locale.ROOT, "Humidity: %.2f %%", confHumidity));
            logger.info("Favorable conditions probability: " + confFavorableOccurrences);
        }
        pointNoiseMap.setThreadCount(nThread);
        logger.info(String.format("PARAM : Number of thread used %d ", nThread));
        // Building height field name
        pointNoiseMap.setHeightField("HEIGHT");
        // Import table with Snow, Forest, Grass, Pasture field polygons. Attribute G is associated with each polygon
        pointNoiseMap.setSoilTableName("landcover");
        // Point cloud height above sea level POINT(X Y Z)
        pointNoiseMap.setDemTable("DEM");


        pointNoiseMap.setMaximumPropagationDistance(maxSrcDist);
        pointNoiseMap.setMaximumReflectionDistance(maxReflectionDistance);
        pointNoiseMap.setWallAbsorption(wallAlpha);


        ProfilerThread profilerThread = new ProfilerThread(new File(outputFolder, "profile_"+uueid+".csv"));
        profilerThread.addMetric(ldenProcessing);
        profilerThread.addMetric(new ProgressMetric(progressLogger));
        profilerThread.addMetric(new JVMMemoryMetric());
        profilerThread.addMetric(new ReceiverStatsMetric());
        pointNoiseMap.setProfilerThread(profilerThread);

        // Set of already processed receivers
        Set<Long> receivers = new HashSet<>();

        // Do not propagate for low emission or far away sources
        // Maximum error in dB
        pointNoiseMap.setMaximumError(0.2d);

        // --------------------------------------------
        pointNoiseMap.setComputeRaysOutFactory(ldenProcessing);
        pointNoiseMap.setPropagationProcessDataFactory(ldenProcessing);

        // Init Map
        pointNoiseMap.initialize(connection, new EmptyProgressVisitor());
        try {
            ldenProcessing.start();
            new Thread(profilerThread).start();
            // Iterate over computation areas
            AtomicInteger k = new AtomicInteger();
            Map<PointNoiseMap.CellIndex, Integer> cells = pointNoiseMap.searchPopulatedCells(connection);
            ProgressVisitor progressVisitor = progressLogger.subProcess(cells.size());
            for (PointNoiseMap.CellIndex cellIndex : new TreeSet<>(cells.keySet())) {// Run ray propagation
                logger.info(String.format("Compute... %d cells remaining (%d receivers in this cell)", cells.size() - k.getAndIncrement(), cells.get(cellIndex)));
                IComputeRaysOut ro = pointNoiseMap.evaluateCell(connection, cellIndex.getLatitudeIndex(), cellIndex.getLongitudeIndex(), progressVisitor, receivers);
                if(isExportDomain && ro instanceof LDENComputeRaysOut && ((LDENComputeRaysOut)ro).inputData instanceof LDENPropagationProcessData) {
                    String path = new File(outputFolder,
                            String.format("domain_%s_part_%d_%d.kml",uueid, cellIndex.getLatitudeIndex(),
                                    cellIndex.getLongitudeIndex())).getAbsolutePath();
                    exportDomain((LDENPropagationProcessData)(((LDENComputeRaysOut)ro).inputData), path,
                            sridBuildings);
                }
                if(progressVisitor.isCanceled()) {
                    throw new IllegalStateException("Calculation has been canceled, an error may have been raised");
                }
            }
        } finally {
            profilerThread.stop();
            ldenProcessing.stop();
        }
        sql.execute("UPDATE metadata SET road_end = NOW();");
    }

}



