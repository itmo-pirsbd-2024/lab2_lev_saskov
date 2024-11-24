package ru.leo.lsm.internal.executor;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import ru.leo.lsm.internal.SSTables;

public class CompactJob implements Runnable {
    public static final boolean POISON_PILL = false;
    private final SSTables storageSystem;
    private final BlockingQueue<Boolean> compactionQueue;

    public CompactJob(SSTables storageSystem, BlockingQueue<Boolean> compactionQueue) {
        this.storageSystem = storageSystem;
        this.compactionQueue = compactionQueue;
    }

    @Override
    public void run() {
        try {
            while (compactionQueue.take()) {
                if (!storageSystem.isCompacted()) {
                    storageSystem.compact();
                }
            }
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
