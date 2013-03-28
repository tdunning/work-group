package com.mapr.workGroup;

import com.google.common.collect.Maps;

import java.io.File;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Watches files and/or directories for changes.
 */
public class Watcher {
    private Map<File, Watch> toWatch = Maps.newConcurrentMap();

    private ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor();
    private int minDelay;
    private int maxDelay;
    private double delay;

    private Runnable checker = new Runnable() {
        @Override
        public void run() {
            checkForChanges();
        }
    };

    public Watcher() {
        this(Constants.getDefaultMinPollingDelay(), Constants.getDefaultMaxPollingDelay());
    }

    public Watcher(int min, int max) {
        setDelay(min, max);
    }

    /**
     * Changes how often things are examined for changes.
     *
     * @param min Minimum interval between checks.
     * @param max Maximum interval between checks.
     */
    public void setDelay(int min, int max) {
        minDelay = min;
        maxDelay = max;
        delay = min;
        ex.schedule(checker, (long) delay, TimeUnit.MILLISECONDS);
    }

    /**
     * Adds a file to the watches.  Any previous watches on the same file are lost.
     *
     * @param f The file or directory to watch.
     * @param w The watcher to call when a changed is noted.
     */
    public void watch(File f, Watch w) {
        watch(f, w, -1);
    }

    /**
     * Adds a file to the watches.  Any previous watches on the same file are lost.
     *
     * @param f         The file or directory to watch.
     * @param w         The watcher to call when a changed is noted.
     * @param timeoutMs After this many milliseconds with no change, timeoutNotify will be called
     */
    public void watch(File f, Watch w, long timeoutMs) {
        w.lastChange.set(f.lastModified());
        w.start = System.currentTimeMillis();
        toWatch.put(f, w);
        w.timeout = timeoutMs;
    }

    /**
     * Adds a file to the watches and waits until the Watch signals that we can return.
     */
    public <T> T watchAndWait(File f, Watch<T> w, long timeoutMs) throws InterruptedException {
        watch(f, w, timeoutMs);
        w.finished.acquire();
        toWatch.remove(f);
        return w.result;
    }

    private void checkForChanges() {
        try {
            int changes = 0;
            for (File file : toWatch.keySet()) {
                Watch watch = toWatch.get(file);

                long t = watch.lastChange.get();
                long lastMod = file.exists() ? file.lastModified() : 0;
                while (lastMod > t) {
                    if (watch.lastChange.compareAndSet(t, lastMod)) {
                        changes++;
                        watch.changeNotify(this, file);
                        t = lastMod;
                    } else {
                        t = watch.lastChange.get();
                        lastMod = file.lastModified();
                    }
                }
                if (watch.timeout != -1 && file.lastModified() + watch.timeout < System.currentTimeMillis()) {
                    watch.timeoutNotify(this, file);
                }
            }
            // check more often if we see changes ... less often if we don't.
            // the asymmetry here causes the delay to sit mostly around the 25-th %-ile of
            // times between changes.
            if (changes > 0) {
                delay = Math.max(minDelay, Constants.getPollingIntervalShrinkRate() * delay);
            } else {
                delay = Math.min(maxDelay, Constants.getPollingIntervalGrowthRate() * delay);
            }
        } finally {
            ex.schedule(checker, (long) delay, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Stop watching a file.
     *
     * @param f   The file that we should stop watching.
     */
    public void remove(File f) {
        toWatch.remove(f);
    }

    public static abstract class Watch<T> {
        private AtomicLong lastChange = new AtomicLong(System.currentTimeMillis());
        private Semaphore finished = new Semaphore(0);

        private long start = 0;
        private long timeout = 0;

        private T result;

        public long getStart() {
            return start;
        }

        public long getAge() {
            return System.currentTimeMillis() - start;
        }

        public void finish(T result) {
            this.result = result;
            finished.release();
        }

        public abstract void changeNotify(Watcher watcher, File f);

        public abstract void timeoutNotify(Watcher watcher, File f);
    }
}
