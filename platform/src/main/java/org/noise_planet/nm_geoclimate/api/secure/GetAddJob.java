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
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.pac4j.RatpackPac4j;
import ratpack.thymeleaf.Template;

import java.util.*;

import static ratpack.jackson.Jackson.json;

/**
 * Display add job form
 */
public class GetAddJob implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(GetAddJob.class);

    @Override
    public void handle(Context ctx) throws Exception {
        RatpackPac4j.userProfile(ctx).then(commonProfile -> {
            if (commonProfile.isPresent()) {
                CommonProfile profile = commonProfile.get();
                SecureEndpoint.getUserPk(ctx, profile).then(pkUser -> {
                    if (pkUser == -1) {
                        ctx.render(json(Eval.me("[errorCode : 403, error : 'Forbidden', redirect: '/manage#subscribe']")));
                    } else {
                        final Map<String, Object> model = Maps.newHashMap();
                        model.put("profile", profile);
                        ctx.render(Template.thymeleafTemplate(model, "add_job"));
                    }
                });
            } else {
                ctx.render(json(Collections.singletonMap("Error", "Not authenticated")));
            }
        });
    }
}
