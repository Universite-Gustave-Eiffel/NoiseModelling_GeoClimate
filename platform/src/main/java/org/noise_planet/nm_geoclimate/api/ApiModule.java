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
package org.noise_planet.nm_geoclimate.api;

import com.google.inject.AbstractModule;
import org.noise_planet.nm_geoclimate.api.secure.*;

public class ApiModule extends AbstractModule {
    
    @Override
    protected void configure() {
        bind(ApiEndpoints.class);
        bind(GetUsers.class);
        bind(AcceptUser.class);
        bind(RefuseUser.class);
        bind(GetJobList.class);
        bind(GetAddJob.class);
        bind(PostAddJob.class);
        bind(GetJobLogs.class);
        bind(PostManageJob.class);
        bind(GetGenerateReport.class);
        bind(PostGenerateReport.class);
    }
}
