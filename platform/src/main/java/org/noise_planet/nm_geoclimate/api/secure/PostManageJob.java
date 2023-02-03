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

import org.noise_planet.nm_geoclimate.process.JobExecutorService;
import org.noise_planet.nm_geoclimate.process.NoiseModellingRunner;
import org.pac4j.core.profile.CommonProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Blocking;
import ratpack.exec.Promise;
import ratpack.form.Form;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.pac4j.RatpackPac4j;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * Stop And/Or Delete jobs
 */
public class PostManageJob implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(PostManageJob.class);

    @Override
    public void handle(Context ctx) throws Exception {
        Promise<Form> form = ctx.parse(Form.class);
        form.then(f -> {
            RatpackPac4j.userProfile(ctx).then(commonProfile -> {
                if (commonProfile.isPresent()) {
                    CommonProfile profile = commonProfile.get();
                    final List<String> deleteJobsList = f.getAll("delete");
                    final List<String> cancelJobsList = f.getAll("cancel");
                    SecureEndpoint.getUserPk(ctx, profile).then(pkUser -> {
                        if(pkUser > -1) {
                            Blocking.get(() -> {
                                DataSource dataSource = ctx.get(DataSource.class);
                                JobExecutorService pool = ctx.get(JobExecutorService.class);
                                try (Connection connection = dataSource.getConnection()) {
                                    for(String jobId : cancelJobsList) {
                                        for(NoiseModellingRunner instance : pool.getNoiseModellingInstance()) {
                                            if(instance.getConfiguration().getTaskPrimaryKey() == Integer.parseInt(jobId)
                                            && instance.getConfiguration().getUserPK() == pkUser) {
                                                instance.cancel(false);
                                                break;
                                            }
                                        }
                                    }
                                    for(String jobId : deleteJobsList) {
                                        // Delete big files
                                        PreparedStatement st = connection.prepareStatement(
                                                "SELECT * FROM JOBS WHERE pk_job = ? AND pk_user = ?");
                                        st.setInt(1, Integer.parseInt(jobId));
                                        st.setInt(2, pkUser);
                                        try(ResultSet rs = st.executeQuery()) {
                                            if(rs.next()) {
                                                String workingDir = rs.getString("LOCAL_JOB_FOLDER");
                                                File dataBaseFile = new File(
                                                        NoiseModellingRunner.MAIN_JOBS_FOLDER + File.separator
                                                                + workingDir,
                                                        NoiseModellingRunner.H2GIS_DATABASE_NAME + ".mv.db");
                                                if(dataBaseFile.exists()) {
                                                    if(dataBaseFile.delete()) {
                                                        LOG.info("File " + dataBaseFile.getAbsolutePath() + "deleted");
                                                    } else {
                                                        LOG.info("Could not delete file " + dataBaseFile.getAbsolutePath());
                                                    }
                                                } else {
                                                    LOG.info("File " + dataBaseFile.getAbsolutePath() + " does not exists");
                                                }
                                            }
                                        }
                                        st = connection.prepareStatement("DELETE FROM JOBS WHERE pk_job = ? AND pk_user = ?");
                                        st.setInt(1, Integer.parseInt(jobId));
                                        st.setInt(2, pkUser);
                                        st.execute();
                                    }
                                }
                                return true;
                            }).then(ok -> {
                                ctx.redirect("/manage/job_list");
                            });
                        }
                    });
                }
            });
        });
    }
}
