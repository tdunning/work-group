package com.mapr.workGroup;

import com.google.protobuf.ByteString;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertEquals;

public class CoordinatorTest {


    // these next tests are just to make sure I understood how things work
    @Test
    public void verifySemaphoreSemantics() throws InterruptedException {
        final Semaphore s = new Semaphore(-5);
        final AtomicInteger count = new AtomicInteger();

        ScheduledFuture<?> f = Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                int k = count.incrementAndGet();
                s.release();
                System.out.printf("releasing %d\n", k);
            }
        }, 10, 10, TimeUnit.MILLISECONDS);

        System.out.printf("about to wait\n");
        s.acquire();
        f.cancel(true);

        System.out.printf("finished with %d\n", count.get());
    }

    @Test
    public void verifyReusableBuilder() throws IOException {
        ByteString id = ByteString.copyFromUtf8("foofar");
        ClusterState.State.Builder state = ClusterState.State.newBuilder();
        state.addNodesBuilder()
                .setType(ClusterState.WorkerType.COORDINATOR)
                .setId(id.toStringUtf8())
                .build();
        byte[] b1 = state.build().toByteArray();

        state.addNodesBuilder()
                .setType(ClusterState.WorkerType.WORKER)
                .setId("fumfum")
                .build();
        byte[] b2 = state.build().toByteArray();

        assertEquals(1, ClusterState.State.parseFrom(b1).getNodesCount());
        assertEquals(2, ClusterState.State.parseFrom(b2).getNodesCount());
    }
}
