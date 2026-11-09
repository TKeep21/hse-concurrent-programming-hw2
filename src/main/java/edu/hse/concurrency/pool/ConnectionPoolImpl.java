package edu.hse.concurrency.pool;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionPoolImpl implements ConnectionPool {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final ArrayDeque<Idle> idle = new ArrayDeque<>();
    private final Set<Lease> borrowed = new HashSet<>();
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();
    private final ConnectionFactory factory;
    private final Thread cleaner;
    private PoolConfig config;
    private int creating;
    private int closing;
    private boolean stopped;

    private record Idle(RawConnection raw, long since) {}
    private final class Waiter {
        final Condition condition = lock.newCondition();
    }
    private final class Lease implements PooledConnection {
        final RawConnection connection;
        boolean active = true;
        Lease(RawConnection connection) { this.connection = connection; }
        public RawConnection raw() {
            lock.lock();
            try {
                if (!active) throw new IllegalStateException("Соединение закрыто");
                return connection;
            } finally { lock.unlock(); }
        }
        public void close() {
            RawConnection close = null;
            lock.lock();
            try {
                if (!active) return;
                active = false;
                borrowed.remove(this);
                if (stopped || total() >= config.maxSize()) { close = connection; closing++; }
                else idle.addLast(new Idle(connection, System.nanoTime()));
                signalWaiters();
                changed.signalAll();
            } finally { lock.unlock(); }
            if (close != null) finishClose(close);
        }
    }

    public ConnectionPoolImpl(PoolConfig config, ConnectionFactory factory) {
        this.config = Objects.requireNonNull(config, "config");
        this.factory = Objects.requireNonNull(factory, "factory");
        cleaner = new Thread(this::clean, "connection-pool-cleaner");
        cleaner.setDaemon(true);
        cleaner.start();
    }

    private int total() { return idle.size() + borrowed.size() + creating; }
    private static long nanos(Duration duration) {
        try { return duration.toNanos(); }
        catch (ArithmeticException e) { return Long.MAX_VALUE; }
    }
    private static void closeRaw(RawConnection raw) {
        if (raw != null) {
            try { raw.close(); } catch (Exception ignored) { }
        }
    }
    private void signalWaiters() {
        if (!waiters.isEmpty() && (!idle.isEmpty() || total() < config.maxSize())) {
            waiters.peekFirst().condition.signal();
        }
    }
    private void finishClose(RawConnection raw) {
        try { closeRaw(raw); }
        finally {
            lock.lock();
            try { closing--; changed.signalAll(); }
            finally { lock.unlock(); }
        }
    }
    public PooledConnection borrow(Duration timeout) throws InterruptedException, PoolTimeoutException, ConnectionCreationException {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) throw new IllegalArgumentException("Отрицательный таймаут");
        long remaining = nanos(timeout);
        long start = System.nanoTime();
        Waiter waiter = null;
        lock.lockInterruptibly();
        try {
            for (;;) {
                if (stopped) throw new IllegalStateException("Пул остановлен");
                boolean eligible = waiter == null ? waiters.isEmpty() : waiters.peekFirst() == waiter;
                if (eligible && (!idle.isEmpty() || total() < config.maxSize())) {
                    if (waiter != null) waiters.remove(waiter);
                    if (!idle.isEmpty()) {
                        Lease lease = new Lease(idle.removeFirst().raw());
                        borrowed.add(lease);
                        signalWaiters();
                        return lease;
                    }
                    creating++;
                    signalWaiters();
                    break;
                }
                if (remaining <= 0) throw new PoolTimeoutException(timeout);
                if (waiter == null) {
                    waiter = new Waiter();
                    waiters.addLast(waiter);
                }
                waiter.condition.awaitNanos(remaining);
                long elapsed = System.nanoTime() - start;
                remaining = elapsed < 0 ? 0 : nanos(timeout) - elapsed;
            }
        } finally {
            if (waiter != null && waiters.remove(waiter)) signalWaiters();
            lock.unlock();
        }
        RawConnection raw;
        try { raw = factory.create(); }
        catch (Exception e) {
            lock.lock();
            try { creating--; signalWaiters(); changed.signalAll(); }
            finally { lock.unlock(); }
            throw new ConnectionCreationException(e);
        }
        lock.lock();
        try {
            creating--;
            if (!stopped) {
                Lease lease = new Lease(raw);
                borrowed.add(lease);
                signalWaiters();
                changed.signalAll();
                return lease;
            }
            signalWaiters();
            changed.signalAll();
        } finally { lock.unlock(); }
        closeRaw(raw);
        throw new IllegalStateException("Пул остановлен");
    }
    public void resize(int maxSize) {
        ArrayList<RawConnection> closing = new ArrayList<>();
        lock.lock();
        try {
            PoolConfig next = config.withMaxSize(maxSize);
            if (stopped) throw new IllegalStateException("Пул остановлен");
            config = next;
            while (total() > maxSize && !idle.isEmpty()) { closing.add(idle.removeLast().raw()); this.closing++; }
            signalWaiters();
            changed.signalAll();
        } finally { lock.unlock(); }
        closing.forEach(this::finishClose);
    }
    public PoolConfig config() {
        lock.lock();
        try { return config; } finally { lock.unlock(); }
    }
    public PoolStats stats() {
        lock.lock();
        try { return new PoolStats(total(), idle.size(), borrowed.size(), creating, waiters.size()); }
        finally { lock.unlock(); }
    }
    private void clean() {
        for (;;) {
            RawConnection close = null;
            lock.lock();
            try {
                while (!stopped && close == null) {
                    if (idle.size() > config.minIdle()) {
                        Idle first = idle.peekFirst();
                        long left = nanos(config.idleTimeout()) - (System.nanoTime() - first.since());
                        if (left <= 0) { close = idle.removeFirst().raw(); closing++; signalWaiters(); }
                        else changed.awaitNanos(left);
                    } else changed.await();
                }
                if (stopped) return;
            } catch (InterruptedException e) {
                if (stopped) return;
            } finally { lock.unlock(); }
            if (close != null) finishClose(close);
        }
    }
    public void shutdown(Duration grace) throws InterruptedException {
        Objects.requireNonNull(grace, "grace");
        if (grace.isNegative()) throw new IllegalArgumentException("Отрицательный grace");
        ArrayList<RawConnection> closing = new ArrayList<>();
        boolean interrupted = false;
        lock.lock();
        try {
            if (stopped) return;
            stopped = true;
            for (Waiter waiter : waiters) waiter.condition.signal();
            changed.signalAll();
            while (!idle.isEmpty()) closing.add(idle.removeFirst().raw());
        } finally { lock.unlock(); }
        closing.forEach(ConnectionPoolImpl::closeRaw);
        closing.clear();
        long remaining = nanos(grace);
        lock.lock();
        try {
            while (!borrowed.isEmpty() && remaining > 0) {
                try { remaining = changed.awaitNanos(remaining); }
                catch (InterruptedException e) { interrupted = true; break; }
            }
            for (Lease lease : borrowed) { lease.active = false; closing.add(lease.connection); }
            borrowed.clear();
        } finally { lock.unlock(); }
        closing.forEach(ConnectionPoolImpl::closeRaw);
        lock.lock();
        try {
            while (this.closing > 0) {
                try { changed.await(); } catch (InterruptedException e) { interrupted = true; }
            }
        } finally { lock.unlock(); }
        for (;;) {
            try { cleaner.join(); break; } catch (InterruptedException e) { interrupted = true; }
        }
        if (interrupted) throw new InterruptedException();
    }
}
