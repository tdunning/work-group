package com.mapr.workGroup;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class Coordinator extends GroupThread {
    private Logger log = LoggerFactory.getLogger(Coordinator.class);
    private ClusterState.State.Builder state;

    /**
     * Creates a thread that tries to become a coordinator.  If the thread is able to become a coordinator,
     * it will coordinate all of the worker threads and eventually return when all work is completed or an
     * error occurs.  If the thread cannot become a coordinator because some other thread has done so, it
     * will complete immediately.
     *
     * @param baseDirectory The directory in which all status info is kept.
     * @return A future that can be used to wait for the coordinator to finish.
     */
    public static Future<Object> start(File baseDirectory) {
        return Executors.newSingleThreadScheduledExecutor().submit(new Coordinator(baseDirectory));
    }

    /**
     * Private constructor.  Use start() instead.
     *
     * @param baseDirectory The directory in which status files will be kept.
     */
    private Coordinator(File baseDirectory) {
        super(baseDirectory);
    }

    @Override
    public Object call() throws IOException, InterruptedException {
        if (coordinatorLock()) {
            // TODO the quorumConfiguration should come from somewhere ... this is a fake
            final List<AcceptableState> quorumConfiguration = Lists.newArrayList();

            final File lockDirectory = new File(getBaseDirectory(), Constants.getWorkerLockDir());

            final Set<String> workers = getWorkerQuorum(quorumConfiguration, lockDirectory);

            // anybody who arrives at this point should be told to go away
            turnAwayLateComers(lockDirectory, workers);

            // set up to watch the log files for all the workers
            // we do this before we put out any assignments
            final Set<String> liveWorkers = Collections.synchronizedSet(Sets.<String>newHashSet(workers));
            final AtomicInteger success = new AtomicInteger();
            final AtomicInteger failure = new AtomicInteger();
            final Semaphore pending = new Semaphore(-workers.size() + 1);

            monitorWorkerLogs(workers, liveWorkers, success, failure, pending);

            // write out the assignments for all the live workers
            for (String worker : workers) {
                // TODO write assignments
            }

            // wait for everybody to finish (or for somebody to register an error)
            waitForCompletion(workers, liveWorkers, success, failure, pending);

            // write completion message to coordinator lock file so that stragglers know to give up
            state.addNodesBuilder()
                    .setType(ClusterState.WorkerType.ALL_EXIT);
            writeLockState();

            setState(log, ThreadState.EXIT);
            return failure.get();
        } else {
            return 0;
        }
    }

    private boolean coordinatorLock() throws IOException {
        try {
            state = ClusterState.State.newBuilder();
            state.addNodesBuilder()
                    .setType(ClusterState.WorkerType.COORDINATOR)
                    .setId(getId().toStringUtf8())
                    .build();

            // if we are able to create this file, we have the job
            Files.write(new File(getBaseDirectory(), Constants.getCoordinatorLock()).toPath(), state.build().toByteArray(),
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

            // no exception means we won the race and can become coordinator
            setState(log, ThreadState.COORDINATOR);
            return true;
        } catch (FileAlreadyExistsException e) {
            return false;
        }
    }

    private void waitForCompletion(Set<String> workers, Set<String> liveWorkers,
                                   AtomicInteger success, AtomicInteger failure, Semaphore pending)
            throws InterruptedException {
        pending.acquire();
        log.info("Coordinator detected completion with {} failures and {} successes", failure.get(), success.get());
        int stillRunning = liveWorkers.size();
        if (stillRunning > 0) {
            log.info("{} workers still running", stillRunning);
        }

        if (failure.get() > 0) {
            setState(log, ThreadState.COORDINATOR_FAIL);
            for (String worker : workers) {
                if (liveWorkers.contains(worker)) {
                    state.addNodesBuilder()
                            .setId(worker)
                            .setType(ClusterState.WorkerType.WORKER_EXIT);
                }
            }
        } else {
            setState(log, ThreadState.COORDINATOR_SUCCESS);
        }
    }

    private void monitorWorkerLogs(final Set<String> workers, final Set<String> liveWorkers,
                                   final AtomicInteger success, final AtomicInteger failure, final Semaphore pending)
            throws FileNotFoundException {
        File logDirectory = new File(getBaseDirectory(), Constants.getWorkerLogDir());
        for (final String worker : workers) {
            final File logFile = new File(logDirectory, worker);
            getWatcher().watch(logFile, new Watcher.Watch() {
                FileInputStream input = new FileInputStream(logFile);

                @Override
                public void changeNotify(Watcher watcher, File f) {
                    log.info("Saw log update on {}", worker);
                    try {
                        ProgressNote.Update update = ProgressNote.Update.parseDelimitedFrom(input);
                        if (update.hasComplete()) {
                            boolean returnStatus = update.getComplete().getExitStatus() == 0;
                            liveWorkers.remove(f.getName());
                            if (returnStatus) {
                                success.incrementAndGet();
                                pending.release();
                            } else {
                                failure.incrementAndGet();
                                pending.release(workers.size());
                            }
                            watcher.remove(f);
                        } else {
                            liveWorkers.add(f.getName());
                        }
                    } catch (IOException e) {
                        log.error("Error parsing log file entry, possibly due to race condition");
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }

                }

                @Override
                public void timeoutNotify(Watcher watcher, File f) {
                    log.error("Worker {} timed out; aborting all operations", worker);
                    failure.incrementAndGet();
                    pending.release(workers.size());
                }
            }, Constants.getWorkerProgressTimeout());
        }
    }

    private void turnAwayLateComers(File lockDirectory, final Set<String> workers) {
        getWatcher().watch(lockDirectory, new Watcher.Watch() {
            @Override
            public void changeNotify(Watcher watcher, File f) {
                if (!workers.contains(f.getName())) {
                    state.addNodesBuilder()
                            .setId(f.getName())
                            .setType(ClusterState.WorkerType.DRONE);
                    writeLockState();
                } else {
                    log.error("Worker {} appeared late, but was already in workers list", f);
                }
            }

            @Override
            public void timeoutNotify(Watcher watcher, File f) {
                log.error("Can't happen");
                throw new UnsupportedOperationException("Can't happen");
            }
        });
    }

    private void writeLockState() {
        try {
            Files.write(new File(getBaseDirectory(), Constants.getCoordinatorLock()).toPath(), state.build().toByteArray(),
                    StandardOpenOption.WRITE);
        } catch (IOException e) {
            log.error("Error writing coordinator lock file", e);
            // TODO decide what kind of exception to throw.  We should probably abort everything at this point
        }
    }

    private Set<String> getWorkerQuorum(final List<AcceptableState> quorumConfiguration, final File lockDirectory) throws InterruptedException {
        // Any good quorum definition should end with an entry that requires 0 workers.  We should always
        // satisfy that.  If such an entry doesn't exist, however, we could wait forever.
        final Set<String> workers = getWatcher().watchAndWait(lockDirectory, new Watcher.Watch<Set<String>>() {
            Set<String> workers = Sets.newHashSet();

            @Override
            public void changeNotify(Watcher watcher, File f) {
                workers.addAll(Lists.newArrayList(lockDirectory.list()));
                if (acceptable(this.getStart(), quorumConfiguration, workers)) {
                    finish(workers);
                }
            }

            @Override
            public void timeoutNotify(Watcher watcher, File f) {
                log.error("Timed out during setup.  Shouldn't happen.");
                finish(null);
            }
        }, -1);

        if (workers == null || workers.size() == 0) {
            log.error("No workers available");
            return Collections.emptySet();
        }
        return workers;
    }

    private boolean acceptable(long t0, List<AcceptableState> quorumConfiguration, Set<String> workers) {
        long t = System.nanoTime() / 1000000 - t0;
        for (AcceptableState condition : quorumConfiguration) {
            if (t < condition.milliseconds && workers.size() >= condition.workers) {
                return true;
            }
        }
        return false;
    }

    private static class AcceptableState {
        int milliseconds;
        int workers;
    }

}
