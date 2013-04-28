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
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

public class ChildProcessTest {
    @Test
    public void testStandardOutput() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        File f = File.createTempFile("file", "out");
        ChildProcess p = ChildProcess.builder("echo", "foobear")
                .withStandardOut(f)
                .run();
        assertEquals(Integer.valueOf(0), p.waitFor(100, TimeUnit.MILLISECONDS));

        assertEquals("foobear\n", Files.toString(f, Charsets.UTF_8));
    }

    @Test
    public void testKill() throws InterruptedException, ExecutionException, TimeoutException, IOException {
        ChildProcess p = ChildProcess.builder("sleep", "10").run();
        p.kill();
        Integer r = p.waitFor(100, TimeUnit.MILLISECONDS);
        assertNotNull(r);
        assertTrue(r != 0);
    }

    @Test
    public void testRun() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        ChildProcess p = ChildProcess.builder("false").run();
        assertEquals(Integer.valueOf(1), p.waitFor(100, TimeUnit.MILLISECONDS));

        p = ChildProcess.builder(Lists.newArrayList("false")).run();
        assertEquals(Integer.valueOf(1), p.waitFor(100, TimeUnit.MILLISECONDS));

        p = ChildProcess.builder("true").run();
        assertEquals(Integer.valueOf(0), p.waitFor(100, TimeUnit.MILLISECONDS));
    }
}
