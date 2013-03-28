package com.mapr.workGroup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
* Created with IntelliJ IDEA.
* User: tdunning
* Date: 3/26/13
* Time: 12:48 PM
* To change this template use File | Settings | File Templates.
*/
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
