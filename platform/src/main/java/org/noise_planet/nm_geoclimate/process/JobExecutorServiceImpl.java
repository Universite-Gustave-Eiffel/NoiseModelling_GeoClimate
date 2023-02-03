package org.noise_planet.nm_geoclimate.process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class JobExecutorServiceImpl extends ThreadPoolExecutor implements JobExecutorService {
    ArrayList<NoiseModellingRunner> instances = new ArrayList<>();

    public JobExecutorServiceImpl(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                  BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public JobExecutorServiceImpl(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                  BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public JobExecutorServiceImpl(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                  BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public JobExecutorServiceImpl(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                  BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    public List<NoiseModellingRunner> getNoiseModellingInstance() {
        return Collections.unmodifiableList(new ArrayList<>(instances));
    }

    @Override
    public void execute(Runnable command) {
        // add runnable in list
        if(command instanceof NoiseModellingRunner) {
            instances.add((NoiseModellingRunner) command);
        }
        super.execute(command);
    }
}
