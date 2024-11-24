package ru.leo.lsm;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import ru.leo.lsm.helper.DaoFactory;
import ru.leo.lsm.helper.DaoTest;

class CompactionTest extends BaseTest {
    @DaoTest
    void empty(Dao<String, Entry<String>> dao) throws IOException {
        // Compact
        dao.compact();
        dao.close();

        // Check the contents
        dao = DaoFactory.reopen(dao);
        assertSame(dao.all(), new int[0]);
    }

    @DaoTest
    void nothingToFlush(Dao<String, Entry<String>> dao) throws IOException {
        final Entry<String> entry = entryAt(42);
        dao.upsert(entry);

        // Compact and flush
        dao.compact();
        dao.close();

        // Check the contents
        dao = DaoFactory.reopen(dao);
        assertSame(dao.all(), entry);

        // Compact and flush
        dao.compact();
        dao.close();

        // Check the contents
        dao = DaoFactory.reopen(dao);
        assertSame(dao.all(), entry);
    }

    @DaoTest
    void overwrite(Dao<String, Entry<String>> dao) throws IOException {
        // Reference value
        int valueSize = 1024 * 1024;
        int keyCount = 10;
        int overwrites = 10;

        List<Entry<String>> entries = bigValues(keyCount, valueSize);

        // Overwrite keys several times each time closing DAO
        for (int round = 0; round < overwrites; round++) {
            for (Entry<String> entry : entries) {
                dao.upsert(entry);
            }
            dao.close();
            dao = DaoFactory.reopen(dao);
        }

        // Big size
        dao.close();
        dao = DaoFactory.reopen(dao);
        long bigSize = sizePersistentData(dao);

        // Compact
        dao.compact();
        dao.close();

        // Check the contents
        dao = DaoFactory.reopen(dao);
        assertSame(dao.all(), entries);

        // Check store size
        long smallSize = sizePersistentData(dao);

        // Heuristic
        assertTrue(smallSize * (overwrites - 1) < bigSize);
        assertTrue(smallSize * (overwrites + 1) > bigSize);
    }

    @DaoTest
    void compactAndAdd(Dao<String, Entry<String>> dao) throws IOException {
        List<Entry<String>> entries = entries(100);
        List<Entry<String>> firstHalf = entries.subList(0, 50);
        List<Entry<String>> lastHalf = entries.subList(50, 100);

        for (Entry<String> entry : firstHalf) {
            dao.upsert(entry);
        }
        dao.compact();
        dao.close();

        dao = DaoFactory.reopen(dao);
        for (Entry<String> entry : lastHalf) {
            dao.upsert(entry);
        }
        assertSame(dao.all(), entries);

        dao.flush();
        assertSame(dao.all(), entries);

        dao.close();
        dao = DaoFactory.reopen(dao);
        assertSame(dao.all(), entries);

        dao.compact();
        dao.close();
        dao = DaoFactory.reopen(dao);
        assertSame(dao.all(), entries);
    }
}
