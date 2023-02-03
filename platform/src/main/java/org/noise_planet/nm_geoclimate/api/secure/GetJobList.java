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


/**
 * @Author Nicolas Fortin, Université Gustave Eiffel
 */
package org.noise_planet.nm_geoclimate.api.secure;

import com.google.common.collect.Maps;
import org.h2gis.utilities.JDBCUtilities;
import org.pac4j.core.profile.CommonProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Blocking;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.pac4j.RatpackPac4j;
import ratpack.thymeleaf.Template;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

import static ratpack.jackson.Jackson.json;

public class GetJobList implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(GetJobList.class);

    public static final int DURATION_DAYS = 0, DURATION_HOURS = 1, DURATION_MINUTES = 2, DURATION_SECONDS = 3;

    public static int[] splitDuration(Duration duration) {
        long totalTimeSeconds = duration.getSeconds();
        long days = totalTimeSeconds / 86400L;
        totalTimeSeconds = totalTimeSeconds % 86400L;
        long hours = totalTimeSeconds / 3600L;
        totalTimeSeconds = totalTimeSeconds % 3600;
        long minutes = totalTimeSeconds / 60L;
        totalTimeSeconds = totalTimeSeconds % 60;
        return new int[] {(int) days, (int) hours, (int) minutes, (int) totalTimeSeconds};
    }

    @Override
    public void handle(Context ctx) throws Exception {
        RatpackPac4j.userProfile(ctx).then(commonProfile -> {
            if (commonProfile.isPresent()) {
                CommonProfile profile = commonProfile.get();
                SecureEndpoint.getUserPk(ctx, profile).then(pkUser -> {
                    if (pkUser == -1) {
                        Blocking.get(() -> {
                            try (Connection connection = ctx.get(DataSource.class).getConnection()) {
                                PreparedStatement statement = connection.prepareStatement(
                                        "MERGE INTO USER_ASK_INVITATION(USER_OID, MAIL) KEY(USER_OID) VALUES (?,?)");
                                statement.setString(1, profile.getId());
                                statement.setString(2, profile.getEmail());
                                statement.execute();
                            }
                            return true;
                        }).then(ok -> {
                            final Map<String, Object> model = Maps.newHashMap();
                            model.put("message", "Please wait for account approval..");
                            model.put("profile", profile);
                            ctx.render(Template.thymeleafTemplate(model, "blank"));
                        });
                    } else {
                        Blocking.get(() -> {
                            List<Map<String, Object>> table = new ArrayList<>();
                            try (Connection connection = ctx.get(DataSource.class).getConnection()) {
                                PreparedStatement statement = connection.prepareStatement("SELECT * FROM JOBS " +
                                        "ORDER BY BEGIN_DATE DESC");
                                //statement.setInt(1, pkUser);
                                DecimalFormat f = (DecimalFormat)(DecimalFormat.getInstance(Locale.ROOT));
                                f.applyPattern("#.### '%'");
                                try (ResultSet rs = statement.executeQuery()) {
                                    List<String> fields = JDBCUtilities.getColumnNames(rs.getMetaData());
                                    while (rs.next()) {
                                        Map<String, Object> row = new HashMap<>();
                                        for (int idField = 1; idField <= fields.size(); idField += 1) {
                                            DateFormat mediumDateFormatEN =
                                                    new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss");
                                            Integer pkJob = rs.getInt("pk_job");
                                            row.put("pk_job", pkJob);
                                            row.put("canManage", rs.getInt("PK_USER") == pkUser);
                                            Timestamp bDate = rs.getTimestamp("BEGIN_DATE");
                                            row.put("startDate", !rs.wasNull() ? mediumDateFormatEN.format(bDate) : "-");
                                            Timestamp eDate = rs.getTimestamp("END_DATE");
                                            String endDate = "-";
                                            String duration = "-";
                                            Duration computationTime = null;
                                            if(!rs.wasNull()) {
                                                endDate = mediumDateFormatEN.format(eDate);
                                                computationTime = Duration.ofMillis(
                                                        eDate.getTime()  - bDate.getTime());
                                            } else if(bDate != null){
                                                computationTime = Duration.ofMillis(
                                                        System.currentTimeMillis() - bDate.getTime());
                                            }
                                            if(computationTime != null) {
                                                int[] durationArray = splitDuration(computationTime);
                                                if (durationArray[DURATION_DAYS] > 0) {
                                                    duration = String.format(Locale.ROOT, "%dd %dh %dm %ds", durationArray[DURATION_DAYS], durationArray[DURATION_HOURS], durationArray[DURATION_MINUTES], durationArray[DURATION_SECONDS]);
                                                } else {
                                                    duration = String.format(Locale.ROOT, "%dh %dm %ds", durationArray[DURATION_HOURS], durationArray[DURATION_MINUTES], durationArray[DURATION_SECONDS]);
                                                }
                                            }
                                            row.put("endDate", endDate);
                                            row.put("duration", duration);
                                            row.put("status", rs.getString("STATE"));
                                            row.put("progression", f.format(rs.getDouble("PROGRESSION")));
                                            row.put("inseeDepartment", rs.getString("INSEE_DEPARTMENT"));
                                            row.put("conf_id", rs.getInt("CONF_ID"));
                                            String jobFolder = rs.getString("REMOTE_JOB_FOLDER");
                                            String localJobFolder = rs.getString("LOCAL_JOB_FOLDER");
                                            row.put("result", "<a href=\"/rjobs/"+localJobFolder+"\" target='_blank'>Result</a> <a href=\"/manage/job/"+pkJob+"/logs\">Logs</a>");
                                            row.put("remote_job_folder", rs.getString("REMOTE_JOB_FOLDER"));
                                        }
                                        table.add(row);
                                    }
                                }
                            }
                            return table;
                        }).then(jobList -> {
                            final Map<String, Object> model = Maps.newHashMap();
                            model.put("jobs", jobList);
                            model.put("profile", profile);
                            ctx.render(Template.thymeleafTemplate(model, "joblist"));
                        });
                    }
                });
            } else {
                ctx.render(Template.thymeleafTemplate("blank"));
            }
        });
    }
}
