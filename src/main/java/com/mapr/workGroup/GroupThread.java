package com.mapr.workGroup;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;

import java.io.File;
import java.security.SecureRandom;
import java.util.concurrent.Callable;

/**
 * Implements common functionality for work group threads such as the coordinator and worker.
 */
public abstract class GroupThread implements Callable<Object> {
    private final ByteString id;
    private final File baseDirectory;
    private ThreadState currentState = ThreadState.STARTING;

    private Watcher watcher = new Watcher();

    public GroupThread(File baseDirectory) {
        this.baseDirectory = baseDirectory;
        byte[] idBytes = new byte[8];
        new SecureRandom().nextBytes(idBytes);
        id = ByteString.copyFrom(idBytes);
    }

    protected void setState(Logger log, ThreadState state) {
        Preconditions.checkState(ThreadState.check(currentState, state));
        log.info("Transitioning from {} to {}", state);
        currentState = state;
    }

    public Watcher getWatcher() {
        return watcher;
    }

    public ByteString getId() {
        return id;
    }

    public ThreadState getState() {
        return currentState;
    }

    public File getBaseDirectory() {
        return baseDirectory;
    }

    public ThreadState getCurrentState() {
        return currentState;
    }
}
