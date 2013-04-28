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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

public enum ThreadState {
    STARTING, COORDINATOR, COORDINATOR_FAIL, COORDINATOR_SUCCESS, WORKER, WORKER_KILL, WORKER_FAIL, WORKER_SUCCESS, EXIT;

    private static final Map<ThreadState, Set<ThreadState>> legal = ImmutableMap.<ThreadState, Set<ThreadState>>builder()
            .put(STARTING, ImmutableSet.of(COORDINATOR, WORKER))
            .put(COORDINATOR, ImmutableSet.of(COORDINATOR_SUCCESS, COORDINATOR_FAIL))
            .put(WORKER, ImmutableSet.of(WORKER_KILL, WORKER_FAIL, WORKER_SUCCESS))
            .put(COORDINATOR_FAIL, ImmutableSet.of(EXIT))
            .put(COORDINATOR_SUCCESS, ImmutableSet.of(EXIT))
            .put(WORKER_FAIL, ImmutableSet.of(EXIT))
            .put(WORKER_KILL, ImmutableSet.of(EXIT))
            .put(WORKER_SUCCESS, ImmutableSet.of(EXIT))
            .build();

    public static boolean check(ThreadState from, ThreadState to) {
        return legal.get(from).contains(to);
    }
}
