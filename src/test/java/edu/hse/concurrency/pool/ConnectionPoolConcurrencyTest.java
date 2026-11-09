package edu.hse.concurrency.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.time.Duration;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class ConnectionPoolConcurrencyTest {
    private static final Duration WAIT = Duration.ofSeconds(3);

    @Test
    void returnWakesWaiterAndReusesConnection() throws Exception {
        ConnectionPool pool = new ConnectionPoolImpl(new PoolConfig(0, 1, WAIT, true), () -> () -> {});
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            PooledConnection first = pool.borrow(WAIT);
            RawConnection raw = first.raw();
            Future<PooledConnection> next = executor.submit(() -> pool.borrow(WAIT));
            awaitWaiting(pool, 1);
            first.close();
            PooledConnection second = next.get(2, TimeUnit.SECONDS);
            assertSame(raw, second.raw());
            first.close();
            assertSame(raw, second.raw());
            second.close();
        } finally { executor.shutdownNow(); pool.shutdown(Duration.ZERO); }
    }

    @Test
    void failedCreationReleasesSlotForWaitingThread() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        ConnectionPool pool = new ConnectionPoolImpl(new PoolConfig(0, 1, WAIT, true), () -> {
            if (calls.getAndIncrement() == 0) { entered.countDown(); release.await(); throw new Exception(); }
            return () -> {};
        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = executor.submit(() -> { assertThrows(ConnectionCreationException.class, () -> pool.borrow(WAIT)); });
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            Future<PooledConnection> second = executor.submit(() -> pool.borrow(WAIT));
            awaitWaiting(pool, 1);
            assertEquals(1, pool.stats().creating());
            release.countDown();
            first.get(2, TimeUnit.SECONDS);
            second.get(2, TimeUnit.SECONDS).close();
        } finally { release.countDown(); executor.shutdownNow(); pool.shutdown(Duration.ZERO); }
    }

    @Test
    void shutdownWakesWaiterAndInvalidatesLease() throws Exception {
        ConnectionPool pool = new ConnectionPoolImpl(new PoolConfig(0, 1, WAIT, true), () -> () -> {});
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            PooledConnection first = pool.borrow(WAIT);
            Future<?> waiting = executor.submit(() -> assertThrows(IllegalStateException.class, () -> pool.borrow(WAIT)));
            awaitWaiting(pool, 1);
            pool.shutdown(Duration.ZERO);
            waiting.get(2, TimeUnit.SECONDS);
            assertThrows(IllegalStateException.class, first::raw);
        } finally { executor.shutdownNow(); pool.shutdown(Duration.ZERO); }
    }

    @Test
    void resizeWakesWaiter() throws Exception {
        ConnectionPool pool = new ConnectionPoolImpl(new PoolConfig(0, 1, WAIT, true), () -> () -> {});
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            PooledConnection first = pool.borrow(WAIT);
            Future<PooledConnection> second = executor.submit(() -> pool.borrow(WAIT));
            awaitWaiting(pool, 1);
            pool.resize(2);
            PooledConnection lease = second.get(2, TimeUnit.SECONDS);
            assertEquals(2, pool.stats().borrowed());
            lease.close(); first.close();
        } finally { executor.shutdownNow(); pool.shutdown(Duration.ZERO); }
    }

    @Test
    void timeoutAndInterruptRemoveWaiters() throws Exception {
        ConnectionPool pool = new ConnectionPoolImpl(new PoolConfig(0, 1, WAIT, true), () -> () -> {});
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            PooledConnection lease = pool.borrow(WAIT);
            assertThrows(PoolTimeoutException.class, () -> pool.borrow(Duration.ofMillis(30)));
            Future<?> waiting = executor.submit(() -> assertThrows(InterruptedException.class, () -> pool.borrow(WAIT)));
            awaitWaiting(pool, 1);
            waiting.cancel(true);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (pool.stats().waiting() != 0 && System.nanoTime() < deadline) Thread.sleep(1);
            assertEquals(0, pool.stats().waiting());
            lease.close();
        } finally { executor.shutdownNow(); pool.shutdown(Duration.ZERO); }
    }

    @Test
    void fairQueueServesInOrder() throws Exception {
        ConnectionPool pool = new ConnectionPoolImpl(new PoolConfig(0, 1, WAIT, true), () -> () -> {});
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            PooledConnection first = pool.borrow(WAIT);
            Future<PooledConnection> a = executor.submit(() -> pool.borrow(WAIT));
            awaitWaiting(pool, 1);
            Future<PooledConnection> b = executor.submit(() -> pool.borrow(WAIT));
            awaitWaiting(pool, 2);
            first.close();
            PooledConnection second = a.get(2, TimeUnit.SECONDS);
            assertFalse(b.isDone());
            second.close();
            b.get(2, TimeUnit.SECONDS).close();
        } finally { executor.shutdownNow(); pool.shutdown(Duration.ZERO); }
    }

    @Test
    void concurrentUseNeverSharesPhysicalConnection() throws Exception {
        ConnectionPool pool = new ConnectionPoolImpl(new PoolConfig(0, 3, WAIT, false), () -> new RawConnection() { public void close() {} });
        ExecutorService executor = Executors.newFixedThreadPool(8);
        java.util.Set<RawConnection> inUse = ConcurrentHashMap.newKeySet();
        try {
            java.util.List<Future<?>> tasks = new java.util.ArrayList<>();
            for (int i = 0; i < 8; i++) tasks.add(executor.submit(() -> {
                for (int j = 0; j < 100; j++) {
                    try (PooledConnection lease = pool.borrow(WAIT)) {
                        RawConnection raw = lease.raw();
                        assertTrue(inUse.add(raw));
                        PoolStats stats = pool.stats();
                        assertEquals(stats.total(), stats.idle() + stats.borrowed() + stats.creating());
                        assertTrue(stats.total() <= 3);
                        assertTrue(inUse.remove(raw));
                    } catch (Exception e) { throw new RuntimeException(e); }
                }
            }));
            for (Future<?> task : tasks) task.get(8, TimeUnit.SECONDS);
        } finally { executor.shutdownNow(); pool.shutdown(Duration.ZERO); }
    }

    @Test
    void slowFactoryDoesNotBlockStatsOrReturningConnection() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        ConnectionPool pool = new ConnectionPoolImpl(new PoolConfig(0, 2, WAIT, true), () -> {
            if (calls.getAndIncrement() == 1) { entered.countDown(); release.await(); }
            return () -> {};
        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            PooledConnection first = pool.borrow(WAIT);
            Future<PooledConnection> creating = executor.submit(() -> pool.borrow(WAIT));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            Future<?> returning = executor.submit(() -> { first.close(); assertEquals(1, pool.stats().idle()); });
            returning.get(1, TimeUnit.SECONDS);
            release.countDown();
            creating.get(2, TimeUnit.SECONDS).close();
        } finally { release.countDown(); executor.shutdownNow(); pool.shutdown(Duration.ZERO); }
    }

    @Test
    void shutdownDuringCreationClosesResult() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
        ConnectionPool pool = new ConnectionPoolImpl(new PoolConfig(0, 1, WAIT, true), () -> {
            entered.countDown(); release.await(); return () -> closed.set(true);
        });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> creating = executor.submit(() -> assertThrows(IllegalStateException.class, () -> pool.borrow(WAIT)));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            pool.shutdown(Duration.ZERO);
            release.countDown();
            creating.get(2, TimeUnit.SECONDS);
            assertTrue(closed.get());
        } finally { release.countDown(); executor.shutdownNow(); pool.shutdown(Duration.ZERO); }
    }

    @Test
    void idleConnectionsExpireButMinIdleRemains() throws Exception {
        ConnectionPool pool = new ConnectionPoolImpl(new PoolConfig(1, 2, Duration.ofMillis(50), true), () -> () -> {});
        try {
            PooledConnection a = pool.borrow(WAIT), b = pool.borrow(WAIT);
            a.close(); b.close();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (pool.stats().idle() != 1 && System.nanoTime() < deadline) Thread.sleep(5);
            assertEquals(1, pool.stats().idle());
        } finally { pool.shutdown(Duration.ZERO); }
    }

    @Test
    void shutdownWaitsForCloseAlreadyInProgress() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        ConnectionPool pool = new ConnectionPoolImpl(new PoolConfig(0, 2, WAIT, true), () -> () -> {
            entered.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            PooledConnection a = pool.borrow(WAIT), b = pool.borrow(WAIT);
            pool.resize(1);
            Future<?> closing = executor.submit(a::close);
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            Future<?> stopping = executor.submit(() -> { try { pool.shutdown(Duration.ZERO); } catch (InterruptedException e) { throw new RuntimeException(e); } });
            assertThrows(TimeoutException.class, () -> stopping.get(100, TimeUnit.MILLISECONDS));
            release.countDown();
            closing.get(2, TimeUnit.SECONDS);
            stopping.get(2, TimeUnit.SECONDS);
            assertThrows(IllegalStateException.class, b::raw);
        } finally { release.countDown(); executor.shutdownNow(); pool.shutdown(Duration.ZERO); }
    }

    @Test
    void slowCloseDoesNotBlockPoolOperations() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        ConnectionPool pool = new ConnectionPoolImpl(new PoolConfig(0, 2, WAIT, true), () -> () -> {
            entered.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            PooledConnection a = pool.borrow(WAIT), b = pool.borrow(WAIT);
            pool.resize(1);
            Future<?> closing = executor.submit(a::close);
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            executor.submit(() -> { assertEquals(1, pool.stats().borrowed()); b.close(); }).get(1, TimeUnit.SECONDS);
            release.countDown();
            closing.get(2, TimeUnit.SECONDS);
        } finally { release.countDown(); executor.shutdownNow(); pool.shutdown(Duration.ZERO); }
    }

    private static void awaitWaiting(ConnectionPool pool, int count) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (pool.stats().waiting() != count && System.nanoTime() < deadline) Thread.sleep(1);
        assertEquals(count, pool.stats().waiting());
    }
}
