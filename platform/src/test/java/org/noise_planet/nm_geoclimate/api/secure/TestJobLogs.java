package org.noise_planet.nm_geoclimate.api.secure;

import org.junit.Assert;
import org.junit.Test;
import org.noise_planet.nm_geoclimate.process.NoiseModellingRunner;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class TestJobLogs {

    @Test
    public void testExtractLogs() throws IOException {
        List<String> lastLine = NoiseModellingRunner.getLastLines(
                new File(TestJobLogs.class.getResource("geoserver.log").getFile()),
                10, "");
        Assert.assertEquals(10, lastLine.size());
        Assert.assertEquals("2021-08-03 17:00:56,628 DEBUG [e", lastLine.remove(0).substring(0, 32));
        Assert.assertEquals("2021-08-03 17:00:56,628 DEBUG [e", lastLine.remove(0).substring(0, 32));
        Assert.assertEquals("2021-08-03 17:00:56,628 TRACE [u", lastLine.remove(0).substring(0, 32));
        Assert.assertEquals("2021-08-03 17:00:56,628 TRACE [u", lastLine.remove(0).substring(0, 32));
        Assert.assertEquals("2021-08-03 17:00:56,686 INFO [re", lastLine.remove(0).substring(0, 32));
    }

    @Test
    public void testFilter() throws IOException {
        List<String> lastLine = NoiseModellingRunner.getLastLines(
                new File(TestJobLogs.class.getResource("test.log").getFile()),
                10, "JOB_4");
        Assert.assertEquals(10, lastLine.size());
        String line = lastLine.remove(lastLine.size() - 1);
        Assert.assertEquals(" - Begin processing of cell 354 / 625", line.substring(line.lastIndexOf(" - "), line.length()));
        line = lastLine.remove(lastLine.size() - 1);
        Assert.assertEquals(" - Compute... 56.480 % (4728 receivers in this cell)",  line.substring(line.lastIndexOf(" - "), line.length()));
        line = lastLine.remove(lastLine.size() - 1);
        Assert.assertEquals(" - This computation area contains 4757 receivers 0 sound sources and 0 buildings",  line.substring(line.lastIndexOf(" - "), line.length()));

    }

}
