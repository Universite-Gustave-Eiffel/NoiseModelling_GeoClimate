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

import org.pac4j.core.profile.CommonProfile;
import ratpack.exec.Blocking;
import ratpack.exec.Promise;
import ratpack.handling.Context;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class SecureEndpoint {

    public static Promise<Integer> getUserPk(Context ctx, CommonProfile profile) {
        return Blocking.get(() -> {
            try (Connection connection = ctx.get(DataSource.class).getConnection()) {
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM USERS" + " WHERE " +
                        "USER_OID = ?");
                statement.setString(1, profile.getId());
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        // found user
                        return rs.getInt("PK_USER");
                    } else {
                        // Nope
                        return -1;
                    }
                }
            }
        });
    }
}
