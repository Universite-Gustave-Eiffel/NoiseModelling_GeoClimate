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
package org.noise_planet.nm_geoclimate.auth;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import org.pac4j.oidc.client.GoogleOidcClient;
import org.pac4j.oidc.config.OidcConfiguration;
import org.noise_planet.nm_geoclimate.config.AuthConfig;

import java.util.Collections;

public class AuthModule extends AbstractModule {

    @Override
    protected void configure() {
        // Noop
    }

    @Provides
    @Inject
    public GoogleOidcClient googleOidcClient(AuthConfig authConfig) {
        OidcConfiguration oidcConfig = new OidcConfiguration();
        oidcConfig.setClientId(authConfig.clientId);
        oidcConfig.setSecret(authConfig.clientSecret);
        oidcConfig.setUseNonce(true);
        oidcConfig.setCustomParams(Collections.singletonMap("prompt", "select_account"));

        return new GoogleOidcClient(oidcConfig);
    }
}
