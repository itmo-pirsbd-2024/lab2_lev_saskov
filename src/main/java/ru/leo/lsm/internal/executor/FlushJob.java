package ru.leo.lsm.internal.executor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import ru.leo.lsm.Entry;
import ru.leo.lsm.internal.SSTables;

public class FlushJob implements Runnable {
    // Poison pill is empty map
    public static final ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> POISON_PILL =
            new ConcurrentSkipListMap<>();
    private volatile ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> inFlushing;
    private final BlockingQueue<ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>>> flushQueue;
    private final SSTables storageSystem;

    public FlushJob(
        SSTables storageSystem,
                         BlockingQueue<ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>>> flushQueue) {
        this.storageSystem = storageSystem;
        this.flushQueue = flushQueue;
    }

    @Override
    public void run() {
        try {
            // take is blocking
            while (!isPoisonPill(inFlushing = flushQueue.take())) {
                storageSystem.save(inFlushing);
                inFlushing = null;
            }
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Fix with in flushing memory, to have access to it during flushing.
     *
     * @return memory that is flushing now or null, if all memory are flushed already
     */
    public ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> getInFlushing() {
        return inFlushing;
    }

    private static boolean isPoisonPill(ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> memTable) {
        return memTable.isEmpty();
    }
}
