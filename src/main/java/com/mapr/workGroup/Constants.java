package com.mapr.workGroup;

import com.google.common.io.Resources;

import java.io.IOException;
import java.util.Properties;

public class Constants {
    private static Properties props = new Properties();

    static {
        loadDefaults();
    }

    public static void loadDefaults() {
        loadResource("work-group.properties");
    }

    public static void setProperty(String name, String value) {
        props.setProperty(name, value);
    }

    public static void loadResource(String resourceName) {
        try {
            props.load(Resources.getResource(resourceName).openStream());
        } catch (IOException e) {
            throw new RuntimeException("Cannot load work group properties");
        }
    }

    public static String getCoordinatorLock() {
        return props.getProperty("coordinator.lock");
    }

    public static int getDefaultMaxPollingDelay() {
        return Integer.parseInt(props.getProperty("default.max.polling.delay"));
    }

    public static int getDefaultMinPollingDelay() {
        return Integer.parseInt(props.getProperty("default.min.polling.delay"));
    }

    public static int getMaxPollingInterval() {
        return Integer.parseInt(props.getProperty("max.polling.interval"));
    }

    public static double getPollingIntervalGrowthRate() {
        return Double.parseDouble(props.getProperty("polling.interval.growth.rate"));
    }

    public static double getPollingIntervalShrinkRate() {
        return Double.parseDouble(props.getProperty("polling.interval.shrink.rate"));
    }

    public static String getWorkerLockDir() {
        return props.getProperty("worker.lock.dir");
    }

    public static String getWorkerLogDir() {
        return props.getProperty("worker.log.dir");
    }

    public static long getWorkerProgressTimeout() {
        return Long.parseLong(props.getProperty("worker.progress.timeout"));
    }

    public static long getWorkerStartupTimeout() {
        return Long.parseLong(props.getProperty("worker.startup.timeout"));
    }
}