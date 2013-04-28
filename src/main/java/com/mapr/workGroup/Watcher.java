/*
 * Copyright MapR Technologies, 2013
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mapr.workGroup;

import com.google.common.collect.Maps;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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
        w.lastSize.set(f.length());
        w.lastChange.set(f.lastModified());
        w.start = System.currentTimeMillis();
        w.timeout = timeoutMs;
        toWatch.put(f, w);
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

                // check for change in time or size,
                long t = watch.lastChange.get();
                long s = watch.lastSize.get();
                long lastMod = file.exists() ? file.lastModified() : 0;
                long lastSize = file.exists() ? file.length() : 0;

                // if we note a change, loop until we can correctly update the size and tme
                while (lastMod > t || lastSize > s) {
                    // watch out for some other thread updating this same structure
                    if (watch.lastChange.compareAndSet(t, lastMod) && watch.lastSize.compareAndSet(s, lastSize)) {
                        changes++;
                        watch.changeNotify(this, file);
                        t = lastMod;
                        s = lastSize;
                    } else {
                        // somebody else bumped in and changed things ... try again
                        t = watch.lastChange.get();
                        s = watch.lastSize.get();

                        lastMod = file.lastModified();
                        lastSize = file.exists() ? file.length() : 0;
                    }
                }
                // ok, any changes have been notified, check for timeout
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
     * @param f The file that we should stop watching.
     */
    public void remove(File f) {
        toWatch.remove(f);
    }

    public void close() {
        ex.shutdownNow();
    }

    public static abstract class Watch<T> {
        private AtomicLong lastChange = new AtomicLong(System.currentTimeMillis());
        private AtomicLong lastSize = new AtomicLong(0);
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
