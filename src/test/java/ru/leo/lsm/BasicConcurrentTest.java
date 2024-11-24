package ru.leo.lsm;

import java.util.List;
import ru.leo.lsm.helper.DaoTest;

public class BasicConcurrentTest extends BaseTest {

    @DaoTest
    void test_10_000(Dao<String, Entry<String>> dao) throws Exception {
        int count = 10_000;
        List<Entry<String>> entries = entries("k", "v", count);
        runInParallel(100, count, value -> dao.upsert(entries.get(value))).close();
        assertSame(dao.all(), entries);
    }

    @DaoTest
    void testConcurrentRW_2_500(Dao<String, Entry<String>> dao) throws Exception {
        int count = 2_500;
        List<Entry<String>> entries = entries("k", "v", count);
        runInParallel(100, count, value -> {
            dao.upsert(entries.get(value));
            assertContains(dao.all(), entries.get(value));
        }).close();

        assertSame(dao.all(), entries);
    }

    @DaoTest
    void testConcurrentRead_8_000(Dao<String, Entry<String>> dao) throws Exception {
        int count = 8_000;
        List<Entry<String>> entries = entries("k", "v", count);
        for (Entry<String> entry : entries) {
            dao.upsert(entry);
        }
        runInParallel(100, count, value -> assertContains(dao.all(), entries.get(value))).close();

        assertSame(dao.all(), entries);
    }

}
