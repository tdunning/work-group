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
