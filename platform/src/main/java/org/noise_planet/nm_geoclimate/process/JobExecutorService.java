package org.noise_planet.nm_geoclimate.process;

import java.util.List;
import java.util.concurrent.ExecutorService;

public interface JobExecutorService extends ExecutorService {
    /**
     * @return Return the instances of all queued and running noisemodelling instances
     */
    List<NoiseModellingRunner> getNoiseModellingInstance();
}
