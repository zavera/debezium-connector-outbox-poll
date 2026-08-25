package io.debezium.connector.outboxpoll;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fixed-interval sweep. With no NOTIFY mechanism in the trigger-less mode,
 * this is the sole driver of detection -- there is no wake-up signal to fall
 * back from, so the poll interval directly bounds detection latency.
 */
public final class PollingScheduler implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PollingScheduler.class);

    private final ScheduledExecutorService executor;
    private final SweepEngine sweepEngine;
    private final long pollIntervalMs;

    public PollingScheduler(SweepEngine sweepEngine, long pollIntervalMs) {
        this.sweepEngine = sweepEngine;
        this.pollIntervalMs = pollIntervalMs;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "outbox-poll-sweep");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        executor.scheduleAtFixedRate(this::sweepSafely, 0, pollIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void sweepSafely() {
        try {
            sweepEngine.sweep();
        }
        catch (Exception e) {
            // a failed sweep must not cancel the scheduled task -- the next
            // sweep retries from the last committed baseline.
            LOG.warn("Sweep failed, will retry on next poll interval", e);
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
