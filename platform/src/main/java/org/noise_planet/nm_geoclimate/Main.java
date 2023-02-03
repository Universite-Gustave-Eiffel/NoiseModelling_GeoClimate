/*
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user-friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */


package org.noise_planet.nm_geoclimate;

import com.jcraft.jsch.JSch;
import org.apache.log4j.PropertyConfigurator;
import org.noise_planet.nm_geoclimate.api.ApiEndpoints;
import org.noise_planet.nm_geoclimate.api.ApiModule;
import org.noise_planet.nm_geoclimate.auth.AuthModule;
import org.noise_planet.nm_geoclimate.config.AuthConfig;
import org.noise_planet.nm_geoclimate.process.ExecutorServiceModule;
import org.pac4j.oidc.client.GoogleOidcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.groovy.template.TextTemplateModule;
import ratpack.guice.Guice;
import ratpack.hikari.HikariModule;
import ratpack.pac4j.RatpackPac4j;
import ratpack.server.BaseDir;
import ratpack.server.RatpackServer;
import ratpack.service.Service;
import ratpack.service.StartEvent;
import ratpack.session.SessionModule;
import ratpack.thymeleaf.ThymeleafModule;

import javax.sql.DataSource;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Objects;

import static ratpack.handling.Handlers.redirect;

/**
 * Starts the NoiseModelling GeoClimate application.
 * @Author Nicolas Fortin, Université Gustave Eiffel
 */
public class Main {
    private static final int DATABASE_VERSION = 4;
    private static String dbUrl;
    private static String dataSourceClassName;

    public static void main(String... args) throws Exception {
        // Init loggers
        PropertyConfigurator.configure(Objects.requireNonNull(
                Main.class.getResource("log4j.properties")).getFile());
        JSch.setLogger(new JschSlf4jLogger(LoggerFactory.getLogger("JSCH")));

        Path basePath = BaseDir.find();
        File configPath = new File(basePath.toFile(), "config.yaml");
        if(!configPath.exists()) {
            // Configuration file does not exists
            // Copy example configuration file
            File sourceConfigPath = new File(basePath.toFile(), "config.demo.yaml");
            Files.copy(sourceConfigPath.toPath(), configPath.toPath());
        }

        String databasePath = Paths.get("database").toAbsolutePath().toString();
        dbUrl = "jdbc:h2:" + databasePath + ";DB_CLOSE_DELAY=30";
        dataSourceClassName = "org.h2.jdbcx.JdbcDataSource";

        RatpackServer.start(s -> s.serverConfig(c -> c.yaml("config.yaml").port(9590).env().require("/auth", AuthConfig.class)
                .baseDir(basePath).build()).registry(Guice.registry(b -> b.module(ApiModule.class)
                .module(AuthModule.class)
                .module(TextTemplateModule.class)
                .module(ThymeleafModule.class)
                .module(SessionModule.class)
                .module(ExecutorServiceModule.class)
                .module(HikariModule.class, hikariConfig -> {
            hikariConfig.setDataSourceClassName(dataSourceClassName);
            hikariConfig.addDataSourceProperty("URL", dbUrl); // H2 database path and config
        }).bind(InitDb.class))).handlers(chain ->
                chain.path(redirect(301, "index.html"))
                        .files(files -> files.files("css")) // share all static files from css folder
                        .files(files -> files.files("js"))  //  share all static files from js folder
                        .files(files -> files.files("img"))  //  share all static files from img folder
                        .files(files -> files.files("fonts"))  //  share all static files from fonts folder
                        .all(RatpackPac4j.authenticator(chain.getRegistry().get(GoogleOidcClient.class))).insert(ApiEndpoints.class)));
    }

    private static class JschSlf4jLogger implements com.jcraft.jsch.Logger {
        private Logger logger;

        public JschSlf4jLogger(Logger logger) {
            this.logger = logger;
        }

        @Override
        public boolean isEnabled(int level) {
            return true;
        }

        @Override
        public void log(int level, String message) {
            switch (level) {
                case DEBUG:
                    logger.debug(message);
                    break;
                case INFO:
                    logger.info(message);
                    break;
                case WARN:
                    logger.warn(message);
                    break;
                default:
                    logger.error(message);
                    break;
            }
        }
    }

    static class InitDb implements Service {
        public void onStart(StartEvent startEvent) throws Exception {
            DataSource dataSource = startEvent.getRegistry().get(DataSource.class);
            try (Connection connection = dataSource.getConnection()) {
                Statement st = connection.createStatement();
                st.executeUpdate("CREATE TABLE IF NOT EXISTS ATTRIBUTES(DATABASE_VERSION INTEGER)");
                ResultSet rs = st.executeQuery("SELECT * FROM ATTRIBUTES");
                int databaseVersion;
                if(rs.next()) {
                    databaseVersion = rs.getInt("DATABASE_VERSION");
                } else {
                    databaseVersion = DATABASE_VERSION;
                    PreparedStatement pst = connection.prepareStatement("INSERT INTO ATTRIBUTES(DATABASE_VERSION) VALUES(?);");
                    pst.setInt(1, DATABASE_VERSION);
                    pst.execute();
                    // First database
                    st.executeUpdate("CREATE TABLE USERS(PK_USER SERIAL, USER_OID VARCHAR)");
                    st.executeUpdate("CREATE TABLE USER_ASK_INVITATION(PK_INVITE SERIAL, USER_OID VARCHAR, MAIL VARCHAR)");
                    st.executeUpdate("CREATE TABLE IF NOT EXISTS JOBS(PK_JOB SERIAL,REMOTE_JOB_FOLDER VARCHAR NOT NULL," +
                            " BEGIN_DATE TIMESTAMP WITHOUT TIME ZONE, END_DATE TIMESTAMP WITHOUT TIME ZONE," +
                            " PROGRESSION REAL DEFAULT 0, CONF_ID INTEGER, INSEE_DEPARTMENT VARCHAR," +
                            " PK_USER INTEGER NOT NULL, STATE VARCHAR, LOCAL_JOB_FOLDER VARCHAR DEFAULT ''," +
                            " SLURM_JOB_ID BIGINT)");

                }
                // In the future check databaseVersion for database upgrades
                if(databaseVersion != DATABASE_VERSION) {
                    if(databaseVersion == 1) {
                        databaseVersion = 2;
                        st.execute("ALTER TABLE JOBS ADD COLUMN STATE VARCHAR");
                    }
                    if(databaseVersion == 2) {
                        st.execute("ALTER TABLE JOBS ADD COLUMN SLURM_JOB_ID BIGINT");
                        databaseVersion = 3;
                    }
                    if(databaseVersion == 3) {
                        st.execute("ALTER TABLE JOBS ADD COLUMN LOCAL_JOB_FOLDER VARCHAR DEFAULT ''");
                        databaseVersion = 4;
                    }
                    st.executeUpdate("UPDATE ATTRIBUTES SET DATABASE_VERSION = " + databaseVersion);
                }
            }
        }
    }
}
