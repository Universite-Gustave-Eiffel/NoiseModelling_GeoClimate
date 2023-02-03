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

/*
  @Author Nicolas Fortin, Université Gustave Eiffel
 */
package org.noise_planet.nm_geoclimate.api.secure;

import com.google.common.collect.Maps;
import org.locationtech.jts.geom.Coordinate;
import org.noise_planet.nmcluster.NoiseModellingInstance;
import org.noise_planet.nm_geoclimate.process.NoiseModellingProfileReport;
import org.noise_planet.nm_geoclimate.process.NoiseModellingRunner;
import org.pac4j.core.profile.CommonProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Promise;
import ratpack.form.Form;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.pac4j.RatpackPac4j;
import ratpack.thymeleaf.Template;

import javax.sql.DataSource;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Create new report
 */
public class PostGenerateReport implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(PostGenerateReport.class);

    @Override
    public void handle(Context ctx) throws Exception {
        Promise<Form> form = ctx.parse(Form.class);
        form.then(f -> RatpackPac4j.userProfile(ctx).then(commonProfile -> {
            if (commonProfile.isPresent()) {
                CommonProfile profile = commonProfile.get();
                final Map<String, Object> model = Maps.newHashMap();
                final String uueid = f.getOrDefault("UUEID", "");
                final String databaseFolder = f.get("NOISEMODELLING_DATABASE");
                final String latitudeString = f.get("LATITUDE");
                final String longitudeString = f.get("LONGITUDE");
                final String receiverHeightString = f.getOrDefault("HEIGHT", "4");
                final String maxRaysString = f.getOrDefault("MAX_RAYS", "10");
                final String sourceType = f.getOrDefault("SOURCE_TYPE", "ROAD");
                model.put("profile", profile);
                if(latitudeString == null || latitudeString.equals("")) {
                    model.put("message", "Missing required field LATITUDE");
                    ctx.render(Template.thymeleafTemplate(model, "generate_report"));
                    return;
                }
                if(longitudeString == null || longitudeString.equals("")) {
                    model.put("message", "Missing required field LONGITUDE");
                    ctx.render(Template.thymeleafTemplate(model, "generate_report"));
                    return;
                }
                if(databaseFolder == null || databaseFolder.equals("")) {
                    model.put("message", "Missing required field databaseFolder");
                    ctx.render(Template.thymeleafTemplate(model, "generate_report"));
                    return;
                }
                SecureEndpoint.getUserPk(ctx, profile).then(pkUser -> {
                    if(pkUser > -1) {
                        String fullPathDatabase = new File(NoiseModellingRunner.MAIN_JOBS_FOLDER, databaseFolder).getAbsolutePath();
                        String resultReportFileName = "report_" + latitudeString + "_" + longitudeString + "_"+ uueid + ".html";
                        File resultReportFile = new File(fullPathDatabase, resultReportFileName);
                        String resultReportLink = "/rjobs/"+databaseFolder+"/"+resultReportFileName;
                        NoiseModellingProfileReport noiseModellingProfileReport = new NoiseModellingProfileReport();
                        NoiseModellingInstance.SOURCE_TYPE sourceTypeEnum = sourceType.equals("ROAD") ? NoiseModellingInstance.SOURCE_TYPE.SOURCE_TYPE_ROAD :
                                NoiseModellingInstance.SOURCE_TYPE.SOURCE_TYPE_RAIL;
                        Executors.newSingleThreadExecutor().submit(() -> {
                            Logger logger = LoggerFactory.getLogger(PostGenerateReport.class);
                            try {
                                noiseModellingProfileReport.testDebugNoiseProfile(fullPathDatabase,
                                        new Coordinate(Double.parseDouble(longitudeString),
                                                Double.parseDouble(latitudeString),
                                                Double.parseDouble(receiverHeightString)), uueid, sourceTypeEnum,
                                        Integer.parseInt(maxRaysString), resultReportFile);
                            } catch (Throwable ex) {
                                logger.error(ex.getLocalizedMessage(), ex);
                            }
                        });
                        model.put("message", "You will find the result <a href=\""+resultReportLink+"\" target=\"_blank\">here</a>");
                        ctx.render(Template.thymeleafTemplate(model, "generate_report"));
                    }
                });
            }
        }));
    }

    private static class ProgressionTracker implements PropertyChangeListener {
        DataSource dataSource;
        int jobPk;
        private final Logger logger = LoggerFactory.getLogger(ProgressionTracker.class);
        String lastProg = "";
        long lastProgressionUpdate = 0;
        private static final long TABLE_UPDATE_DELAY = 5000;

        public ProgressionTracker(DataSource dataSource, int jobPk) {
            this.dataSource = dataSource;
            this.jobPk = jobPk;
        }

        @Override
        public void propertyChange(PropertyChangeEvent evt) {
            if(evt.getNewValue() instanceof Double) {
                String newLogProgress = String.format("%.2f", (Double)(evt.getNewValue()) * 100.0D);
                if(!lastProg.equals(newLogProgress)) {
                    lastProg = newLogProgress;
                    long t = System.currentTimeMillis();
                    if(t - lastProgressionUpdate > TABLE_UPDATE_DELAY) {
                        lastProgressionUpdate = t;
                        try (Connection connection = dataSource.getConnection()) {
                            PreparedStatement st = connection.prepareStatement("UPDATE JOBS SET PROGRESSION = ? WHERE PK_JOB = ?");
                            st.setDouble(1, (Double) (evt.getNewValue()) * 100.0);
                            st.setInt(2, jobPk);
                            st.setQueryTimeout((int)(TABLE_UPDATE_DELAY / 1000));
                            st.execute();
                        } catch (SQLException ex) {
                            logger.error(ex.getLocalizedMessage(), ex);
                        }
                    }
                }
            }
        }
    }
}
