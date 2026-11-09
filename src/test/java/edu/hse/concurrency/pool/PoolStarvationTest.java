package edu.hse.concurrency.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class PoolStarvationTest {
    private static final int THREADS = 4;

    @Test
    void allThreadsWaitForSecondConnection() throws Exception {
        ConnectionPool pool = new ConnectionPoolImpl(new PoolConfig(0, THREADS, Duration.ofMinutes(2), true), () -> () -> {});
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        CountDownLatch firstTaken = new CountDownLatch(THREADS);
        CountDownLatch startSecond = new CountDownLatch(1);
        List<Future<?>> tasks = new ArrayList<>();
        try {
            for (int i = 0; i < THREADS; i++) tasks.add(executor.submit(() -> {
                try (PooledConnection first = pool.borrow(Duration.ofMinutes(1))) {
                    firstTaken.countDown();
                    startSecond.await();
                    try (PooledConnection second = pool.borrow(Duration.ofMinutes(1))) { }
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                catch (Exception e) { throw new RuntimeException(e); }
            }));
            assertTrue(firstTaken.await(3, TimeUnit.SECONDS));
            startSecond.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (pool.stats().waiting() != THREADS && System.nanoTime() < deadline) Thread.sleep(5);
            assertEquals(THREADS, pool.stats().waiting());
            assertEquals(THREADS, pool.stats().borrowed());
            for (Future<?> task : tasks) assertFalse(task.isDone());
            String jcmd = System.getProperty("java.home") + "/bin/jcmd";
            Process dump = new ProcessBuilder(jcmd, Long.toString(ProcessHandle.current().pid()), "Thread.print").redirectErrorStream(true).start();
            String output = new String(dump.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            assertTrue(dump.waitFor(3, TimeUnit.SECONDS));
            assertEquals(0, dump.exitValue(), output);
            assertTrue(output.contains("ConnectionPoolImpl.borrow"));
            System.out.println(output);
        } finally {
            for (Future<?> task : tasks) task.cancel(true);
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    void oneExtraConnectionAllowsAllThreadsToFinish() throws Exception {
        ConnectionPool pool = new ConnectionPoolImpl(new PoolConfig(0, THREADS + 1, Duration.ofMinutes(2), true), () -> () -> {});
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        CountDownLatch firstTaken = new CountDownLatch(THREADS);
        List<Future<?>> tasks = new ArrayList<>();
        try {
            for (int i = 0; i < THREADS; i++) tasks.add(executor.submit(() -> {
                try (PooledConnection first = pool.borrow(Duration.ofMinutes(1))) {
                    firstTaken.countDown();
                    assertTrue(firstTaken.await(3, TimeUnit.SECONDS));
                    try (PooledConnection second = pool.borrow(Duration.ofMinutes(1))) { }
                } catch (Exception e) { throw new RuntimeException(e); }
            }));
            for (Future<?> task : tasks) task.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            pool.shutdown(Duration.ZERO);
        }
    }
}
