package ru.leo.lsm.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import ru.leo.lsm.Config;
import ru.leo.lsm.Dao;
import ru.leo.lsm.Entry;
import ru.leo.lsm.internal.executor.CompactJob;
import ru.leo.lsm.internal.executor.FlushJob;

public class LSMDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {
    private final ExecutorService executor = Executors.newFixedThreadPool(2);
    private final Object monitor = new Object[0];
    private final AtomicLong memTableByteSize = new AtomicLong();
    // Poison pill is empty map
    private final BlockingQueue<ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>>> flushQueue =
        new ArrayBlockingQueue<>(1);
    // True is signal to start compact, False is poison pill.
    private final BlockingQueue<Boolean> compactionQueue = new LinkedBlockingQueue<>();

    private final long flushThresholdBytes;
    private final FlushJob flushJob;

    private final SSTables ssTables;

    private final Future<?> flushFuture;
    private final Future<?> compactFuture;
    private volatile ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> memTable;
    private volatile boolean isClosed;

    public static LSMDao load(Config config) throws IOException {
        return new LSMDao(config, SSTables.load(config.basePath()));
    }

    private LSMDao(Config config, SSTables ssTables) {
        memTable = new ConcurrentSkipListMap<>();
        flushThresholdBytes = config.flushThresholdBytes();
        this.ssTables = ssTables;
        compactFuture = executor.submit(new CompactJob(ssTables, compactionQueue));
        flushJob = new FlushJob(ssTables, flushQueue);
        flushFuture = executor.submit(flushJob);
    }

    @Override
    public Entry<ByteBuffer> get(ByteBuffer key) throws IOException {
        checkClose();

        Entry<ByteBuffer> ans = memTable.get(key);
        if (ans != null) {
            return filterTombstone(ans);
        }

        var queueTable = flushQueue.peek();
        ans = queueTable == null ? null : queueTable.get(key);
        if (ans != null) {
            return filterTombstone(ans);
        }

        var flushingTable = flushJob.getInFlushing();
        ans = flushingTable == null ? null : flushingTable.get(key);
        if (ans != null) {
            return filterTombstone(ans);
        }

        if (ssTables != null) {
            ans = ssTables.findEntry(key);
        }
        return filterTombstone(ans);
    }

    private static Entry<ByteBuffer> filterTombstone(Entry<ByteBuffer> ans) {
        if (ans == null || ans.value() == null) {
            return null;
        }

        return ans;
    }

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) {
        checkClose();

        var memTableRange = getRange(memTable, from, to);
        if (ssTables == null && memTableRange != null) {
            return memTableRange.values().iterator();
        }

        return ssTables == null ? null : ssTables.getMergedEntrys(
            from, to,
            memTableRange,
            // Tables that are in flushing state now:
            getRange(flushQueue.peek(), from, to),
            getRange(flushJob.getInFlushing(), from, to)
        );
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        checkClose();

        int entrySize = SSTable.getPersEntryByteSize(entry);
        if (memTableByteSize.get() + entrySize > flushThresholdBytes) {
            synchronized (monitor) {
                if (memTableByteSize.addAndGet(entrySize) > flushThresholdBytes) {
                    // Add() throws exception if queue is full, so upsert will not be done.
                    flushQueue.add(memTable);
                    // Create new in memory memTable if queue of flushing not full (exception wasn't thrown)
                    memTable = new ConcurrentSkipListMap<>();
                    memTableByteSize.set(entrySize);
                }
            }
        } else {
            memTableByteSize.addAndGet(entrySize);
        }

        memTable.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        checkClose();

        if (!memTable.isEmpty()) {
            // Empty mem table is poison bill.
            try {
                flushQueue.put(memTable);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            memTable = new ConcurrentSkipListMap<>();
            memTableByteSize.set(0);
        }
    }

    @Override
    public void compact() {
        checkClose();
        compactionQueue.add(true);
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }

        isClosed = true;
        try {
            compactionQueue.put(CompactJob.POISON_PILL);
            flushQueue.put(FlushJob.POISON_PILL);
            flushFuture.get();
            compactFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
        }
        executor.shutdown();
        ssTables.save(memTable);
        ssTables.close();
    }

    private void checkClose() {
        if (isClosed) {
            throw new RuntimeException("In memory dao closed.");
        }
    }

    /**
     * Cuts mem table in given range.
     *
     * @return null if memTable is null, otherwise cut with given range memTable.
     */
    private static ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> getRange(
        ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> memTable,
        ByteBuffer from, ByteBuffer to
    ) {
        ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> cut;

        if ((from == null && to == null) || memTable == null) {
            cut = memTable;
        } else if (from == null) {
            cut = memTable.headMap(to);
        } else if (to == null) {
            cut = memTable.tailMap(from);
        } else {
            cut = memTable.subMap(from, to);
        }

        return cut;
    }
}
