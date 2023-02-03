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
import groovy.util.Eval;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static ratpack.jackson.Jackson.json;

/**
 * Display generate report form
 */
public class GetGenerateReport implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(GetGenerateReport.class);

    @Override
    public void handle(Context ctx) throws Exception {
        RatpackPac4j.userProfile(ctx).then(commonProfile -> {
            if (commonProfile.isPresent()) {
                CommonProfile profile = commonProfile.get();
                SecureEndpoint.getUserPk(ctx, profile).then(pkUser -> {
                    if (pkUser == -1) {
                        ctx.render(json(Eval.me("[errorCode : 403, error : 'Forbidden', redirect: '/manage#subscribe']")));
                    } else {
                        Blocking.get(() -> {
                            try (Connection connection = ctx.get(DataSource.class).getConnection()) {
                                PreparedStatement statement = connection.prepareStatement(
                                        "SELECT LOCAL_JOB_FOLDER,END_DATE   FROM JOBS where state != 'FAILED' order by LOCAL_JOB_FOLDER desc");
                                try(ResultSet rs = statement.executeQuery()) {
                                    List<String> jobsFolder = new ArrayList<>();
                                    while(rs.next()) {
                                        jobsFolder.add(rs.getString(1));
                                    }
                                    return jobsFolder;
                                }
                            }
                        }).then(jobsFolder -> {
                            final Map<String, Object> model = Maps.newHashMap();
                            model.put("profile", profile);
                            model.put("databases", jobsFolder);
                            ctx.render(Template.thymeleafTemplate(model, "generate_report"));
                        });
                    }
                });
            } else {
                ctx.render(json(Collections.singletonMap("Error", "Not authenticated")));
            }
        });
    }
}
