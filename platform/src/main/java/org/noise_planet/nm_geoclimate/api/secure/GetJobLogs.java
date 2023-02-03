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
 * @Author Nicolas Fortin, Université Gustave Eiffel
 */
package org.noise_planet.nm_geoclimate.api.secure;

import com.google.common.collect.Maps;
import org.noise_planet.nm_geoclimate.process.NoiseModellingRunner;
import org.pac4j.core.profile.CommonProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.pac4j.RatpackPac4j;
import ratpack.thymeleaf.Template;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

public class GetJobLogs implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(GetJobLogs.class);
    static final int FETCH_NUMBER_OF_LINES = 1000;

    @Override
    public void handle(Context ctx) throws Exception {
        RatpackPac4j.userProfile(ctx).then(commonProfile -> {
            if (commonProfile.isPresent()) {
                CommonProfile profile = commonProfile.get();
                SecureEndpoint.getUserPk(ctx, profile).then(pkUser -> {
                    if (pkUser != -1) {
                        final Map<String, Object> model = Maps.newHashMap();
                        final String jobId = ctx.getAllPathTokens().get("jobid");
                        List<String> rows;
                        DataSource dataSource = ctx.get(DataSource.class);
                        String workingDir = "";
                        try (Connection connection = dataSource.getConnection()) {
                            PreparedStatement st = connection.prepareStatement("SELECT * FROM JOBS WHERE pk_job = ? AND pk_user = ?");
                            st.setInt(1, Integer.parseInt(jobId));
                            st.setInt(2, pkUser);
                            try (ResultSet rs = st.executeQuery()) {
                                if (rs.next()) {
                                    workingDir = rs.getString("LOCAL_JOB_FOLDER");
                                }
                            }
                        }
                        File logFilePath = new File(NoiseModellingRunner.MAIN_JOBS_FOLDER + File.separator + workingDir, NoiseModellingRunner.JOB_LOG_FILENAME);
                        if(workingDir.isEmpty() || !logFilePath.exists()) {
                            // If log file is not written use the main application log file
                            rows = NoiseModellingRunner.getAllLines(jobId, FETCH_NUMBER_OF_LINES);
                            LOG.info(String.format("Got %d log rows", rows.size()));
                        } else {
                            rows = NoiseModellingRunner.getLastLines(logFilePath, FETCH_NUMBER_OF_LINES, "");
                            LOG.info(String.format("Got %d log rows from %s", rows.size(), logFilePath.getPath()));
                        }
                        model.put("rows", rows);
                        model.put("profile", profile);
                        ctx.render(Template.thymeleafTemplate(model, "joblogs"));
                    }
                });
            } else {
                ctx.render(Template.thymeleafTemplate("blank"));
            }
        });
    }
}
