package com.mapr.workGroup;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProgressLogTest {
    @Test
    public void testRoundTrip() throws IOException {
        File logFile = File.createTempFile("foo", "log");
        ProgressLog log = ProgressLog.open("test", logFile);

        log.note("phase 1");
        log.increment("k", 3);
        log.increment("x", 1);
        log.increment("k", 2);
        log.note("phase 2");
        log.increment("x", 10);

        log.finish();

        Map<String, Double> r = ProgressLog.readTotals(logFile);
        assertEquals(5.0, r.get("k"), 0.0);
        assertEquals(11.0, r.get("x"), 0.0);
        assertTrue(log.finished());
        assertTrue(log.success());
    }

    @Test
    public void testBackTrace() throws IOException {
        File logFile = File.createTempFile("foo", "log");
        ProgressLog log = ProgressLog.open("test", logFile);

        log.note("phase 1");
        log.increment("k", 3);
        log.increment("x", 1);
        log.fail("failed", new Exception());

        Map<String, Double> r = ProgressLog.readTotals(logFile);
        assertEquals(3, r.get("k"), 0.0);
        assertEquals(1.0, r.get("x"), 0.0);
        assertTrue(log.finished());
        assertFalse(log.success());
    }
}
