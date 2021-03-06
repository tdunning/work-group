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

import com.google.common.base.Charsets;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runs a sub-process in the background allowing one to check for completion
 * as well as canceling the process without blocking.
 */
public class ChildProcess {
    private Future<Integer> f;
    private final ProcessBuilder builder;
    private Process proc;
    private ByteArrayOutputStream stderr;

    public static ChildProcess builder(String... args) throws IOException {
        return new ChildProcess(args);
    }

    private ChildProcess(String... args) {
        builder = new ProcessBuilder(args);
    }

    public static ChildProcess builder(List<String> args) throws IOException {
        return new ChildProcess(args);
    }

    private ChildProcess(List<String> args) {
        builder = new ProcessBuilder(args);
    }

    public ChildProcess withStandardOut(File f) {
        builder.redirectOutput(f);
        return this;
    }

    public ChildProcess run() throws IOException {
        stderr = new ByteArrayOutputStream();
        proc = builder.start();
        f = Executors.newSingleThreadExecutor().submit(
                new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                        return proc.waitFor();
                    }
                }
        );
        return this;
    }

    public void kill() {
        proc.destroy();
    }

    public Integer waitFor(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return f.get(timeout, unit);
    }

    public String errorOutput() {
        return new String(stderr.toByteArray(), Charsets.UTF_8);
    }
}
