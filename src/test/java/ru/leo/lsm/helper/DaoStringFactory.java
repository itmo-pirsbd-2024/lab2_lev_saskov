package ru.leo.lsm.helper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import ru.leo.lsm.Config;
import ru.leo.lsm.Dao;
import ru.leo.lsm.Entry;
import ru.leo.lsm.internal.LSMDao;

public class DaoStringFactory implements DaoFactory<ByteBuffer, Entry<ByteBuffer>> {
    @Override
    public Dao<ByteBuffer, Entry<ByteBuffer>> createDao(Config config) throws IOException {
        return LSMDao.load(config);
    }

    @Override
    public String toString(ByteBuffer data) {
        ByteBuffer indPosBuff = data.asReadOnlyBuffer();
        byte[] bytes = new byte[indPosBuff.capacity()];
        indPosBuff.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public ByteBuffer fromString(String data) {
        return data == null ? null : ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<ByteBuffer> fromBaseEntry(Entry<ByteBuffer> baseEntry) {
        return baseEntry;
    }
}
