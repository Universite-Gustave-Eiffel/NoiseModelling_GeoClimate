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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Maps;
import org.noise_planet.nm_geoclimate.config.AdminConfig;
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
import java.util.*;

import static ratpack.jackson.Jackson.json;

public class GetUsers implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(GetUsers.class);

    @Override
    public void handle(Context ctx) throws Exception {
        RatpackPac4j.userProfile(ctx).then(commonProfile -> {
            if (commonProfile.isPresent()) {
                CommonProfile profile = commonProfile.get();
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                AdminConfig adminConfig = mapper.readValue(ctx.file("config.yaml").toFile(), AdminConfig.class);
                if(adminConfig.contains(profile.getId())) {
                    Blocking.get(() -> {
                        List<Map<String, Object>> table = new ArrayList<>();
                        try (Connection connection = ctx.get(DataSource.class).getConnection()) {
                            PreparedStatement statement = connection.prepareStatement("SELECT * FROM USER_ASK_INVITATION");
                            try (ResultSet rs = statement.executeQuery()) {
                                while (rs.next()) {
                                    Map<String, Object> row = new HashMap<>();
                                    row.put("ooid", rs.getString("USER_OID"));
                                    row.put("email", rs.getString("MAIL"));
                                    table.add(row);
                                }
                            }
                        }
                        return table;
                    }).then(userList -> {
                        final Map<String, Object> model = Maps.newHashMap();
                        model.put("profile", profile);
                        model.put("users", userList);
                        ctx.render(Template.thymeleafTemplate(model, "users"));
                    });
                } else {
                    final Map<String, Object> model = Maps.newHashMap();
                    model.put("message", "This account does not have an administrator access");
                    model.put("profile", profile);
                    ctx.render(Template.thymeleafTemplate(model, "blank"));
                }
            } else {
                ctx.render(json(Collections.singletonMap("Error", "Not authenticated")));
            }
        });
    }
}
