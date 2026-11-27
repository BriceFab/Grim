package ac.grim.grimac.manager;

import ac.grim.grimac.manager.init.start.StartableInitable;
import ac.grim.grimac.manager.init.stop.StoppableInitable;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Remembers when the whole process last stalled (GC pause, suspended VM...). Packets that players
 * sent meanwhile are read late and all at once, so their age says nothing about the players' latency.
 * Uses its own timer, as server ticks may be slow or stopped without delaying netty.
 */
public class ServerStall implements StartableInitable, StoppableInitable {
    private static final long BEAT_MILLIS = 50;
    private static final long THRESHOLD = TimeUnit.MILLISECONDS.toNanos(250);

    private ScheduledExecutorService executor;
    private volatile long lastBeat = System.nanoTime();
    // Before any player's initial clock, which Timer sets 60 s back
    private volatile long endedAt = lastBeat - TimeUnit.MINUTES.toNanos(1);

    @Override
    public void start() {
        // The time between plugin load and enable is not a stall
        lastBeat = System.nanoTime();
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "grim-server-stall");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(() -> {
            long now = System.nanoTime();
            if (now - lastBeat >= THRESHOLD) endedAt = now;
            lastBeat = now;
        }, 0, BEAT_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if (executor != null) executor.shutdownNow();
    }

    /**
     * Whether the process has stalled since the given {@link System#nanoTime()}. Netty threads may
     * resume before the timer does.
     */
    public boolean hasStalledSince(long time) {
        return System.nanoTime() - lastBeat >= THRESHOLD || endedAt - time > 0;
    }
}
