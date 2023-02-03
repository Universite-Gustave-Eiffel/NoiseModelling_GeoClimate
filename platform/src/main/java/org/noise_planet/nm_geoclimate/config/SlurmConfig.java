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

package org.noise_planet.nm_geoclimate.config;

public class SlurmConfig {
    public String host;
    public int port;
    public String user;
    public String sshFilePassword;
    public String sshFile;
    public int maxJobs;
    public String serverKey;
    public String serverKeyType;
    public String serverTempFolder;
}

