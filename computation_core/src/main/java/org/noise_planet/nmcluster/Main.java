/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */
package org.noise_planet.nmcluster;

import org.apache.log4j.PropertyConfigurator;
import org.h2gis.utilities.wrapper.ConnectionWrapper;
import org.noise_planet.noisemodelling.pathfinder.RootProgressVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.noise_planet.nmcluster.NoiseModellingInstance.loadClusterConfiguration;


public class Main {
    public static final int SECONDS_BETWEEN_PROGRESSION_PRINT = 5;

    public static void printBuildIdentifiers(Logger logger) {
        try {
            String columnFormat = "%-35.35s %-35.35s %-20.20s %-30.30s";
            String[] columns = new String[] {"name", "last-modified", "version", "commit"};
            Enumeration<URL> resources = Main.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append( "Loaded libraries:\n");
            stringBuilder.append(String.format(Locale.ROOT, columnFormat,
                    (Object[]) columns));
            stringBuilder.append( "\n");
            Map<String, ArrayList<String>> rows = new HashMap<>();
            for (String column : columns) {
                rows.put(column, new ArrayList<>());
            }
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
                    "EEE, d MMM yyyy HH:mm:ss Z", Locale.getDefault());
            int nbRows = 0;
            while (resources.hasMoreElements()) {
                try {
                    Manifest manifest = new Manifest(resources.nextElement().openStream());
                    Attributes attributes = manifest.getMainAttributes();
                    String bundleName = attributes.getValue("Bundle-Name");
                    String bundleVersion = attributes.getValue("Bundle-Version");
                    String gitCommitId = attributes.getValue("Implementation-Build");
                    String lastModifier = attributes.getValue("Bnd-LastModified");
                    if(bundleName != null) {
                        nbRows++;
                        rows.get(columns[0]).add(bundleName);
                        if(lastModifier != null) {
                            long lastModifiedLong = Long.parseLong(lastModifier);
                            rows.get(columns[1]).add(simpleDateFormat.format(new Date(lastModifiedLong)));
                        } else {
                            rows.get(columns[1]).add(" - ");
                        }
                        rows.get(columns[2]).add(bundleVersion != null ? bundleVersion : " - ");
                        rows.get(columns[3]).add(gitCommitId != null ? gitCommitId : " - ");
                    }
                } catch (IOException ex) {
                    logger.error(ex.getLocalizedMessage(), ex);
                }
            }
            for(int idRow = 0; idRow < nbRows; idRow++) {
                String[] rowValues = new String[columns.length];
                for (int idColumn = 0; idColumn < columns.length; idColumn++) {
                    String column = columns[idColumn];
                    rowValues[idColumn] = rows.get(column).get(idRow);
                }
                stringBuilder.append(String.format(Locale.ROOT, columnFormat,
                        (Object[]) rowValues));
                stringBuilder.append("\n");
            }
            logger.info(stringBuilder.toString());
        } catch (IOException ex) {
            logger.error("Error while accessing resources", ex);
        }
    }

    public static void main(String... args) throws Exception {
        PropertyConfigurator.configure(Main.class.getResource("log4j.properties"));

        Logger logger = LoggerFactory.getLogger("org.noise_planet");

        printBuildIdentifiers(logger);
        try {
            // Read node id parameter
            int nodeId = -1;
            String workingDir = "";
            for (int i = 0; args != null && i < args.length; i++) {
                String a = args[i];
                if(a == null) {
                    continue;
                }
                if (a.startsWith("-n")) {
                    nodeId = Integer.parseInt(a.substring(2));
                } else if (a.startsWith("-w")) {
                    workingDir = a.substring(2);
                    if(!(new File(workingDir).exists())) {
                        logger.error(workingDir + " folder does not exists");
                        workingDir = "";
                    }
                }
            }
            if (nodeId == -1) {
                logger.info("Command line arguments :");
                for (String arg : args) {
                    logger.info("Got argument [" + arg + "]");
                }
                throw new IllegalArgumentException("Missing node identifier. -n[nodeId]");
            }
            NoiseModellingInstance.ClusterConfiguration clusterConfiguration = loadClusterConfiguration(workingDir, nodeId);
            logger.info(String.format(Locale.ROOT, "For job %d, will compute the following UUEID (%s)",
                    nodeId, Stream.concat(clusterConfiguration.roadsUueid.stream(),
                            clusterConfiguration.railsUueids.stream()).collect(Collectors.joining(","))));
            // Open database
            DataSource ds = NoiseModellingInstance.createDataSource("", "", new File(workingDir).getAbsolutePath(), "h2gisdb", false);

            RootProgressVisitor progressVisitor = new RootProgressVisitor(1, true,
                    SECONDS_BETWEEN_PROGRESSION_PRINT);

            try (Connection connection = new ConnectionWrapper(ds.getConnection())) {
                NoiseModellingInstance.startNoiseModelling(connection, progressVisitor, clusterConfiguration,
                        workingDir, nodeId);
            } catch (SQLException ex) {
                while (ex != null) {
                    logger.error(ex.getLocalizedMessage(), ex);
                    ex = ex.getNextException();
                }
                System.exit(1);
            }
        } catch (Exception ex) {
            logger.error(ex.getLocalizedMessage(), ex);
            System.exit(1);
        }
    }
}