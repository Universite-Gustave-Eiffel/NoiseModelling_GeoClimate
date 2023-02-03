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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.noise_planet.nm_geoclimate.config.AdminConfig;
import org.pac4j.core.profile.CommonProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Blocking;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.pac4j.RatpackPac4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;

import static ratpack.jackson.Jackson.json;

/**
 * Accept user request to join job manager group
 */
public class AcceptUser implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(AcceptUser.class);

    @Override
    public void handle(Context ctx) throws Exception {
        final String userOoid = ctx.getAllPathTokens().get("userooid");
        RatpackPac4j.userProfile(ctx).then(commonProfile -> {
            if (commonProfile.isPresent()) {
                CommonProfile profile = commonProfile.get();
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                AdminConfig adminConfig = mapper.readValue(ctx.file("config.yaml").toFile(), AdminConfig.class);
                if(adminConfig.contains(profile.getId())) {
                    Blocking.get(() -> {
                        try (Connection connection = ctx.get(DataSource.class).getConnection()) {
                            PreparedStatement statement = connection.prepareStatement("INSERT INTO USERS(USER_OID) VALUES (?)");
                            statement.setString(1, userOoid);
                            statement.execute();
                            statement = connection.prepareStatement("DELETE FROM USER_ASK_INVITATION WHERE USER_OID = ?");
                            statement.setString(1, userOoid);
                            return statement.executeUpdate();
                        }
                    }).then(affected -> {
                        ctx.redirect("/manage/users");
                    });
                }
            } else {
                ctx.render(json(Collections.singletonMap("Error", "Not authenticated")));
            }
        });
    }
}
