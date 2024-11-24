package ru.leo.lsm.helper;

import java.io.IOException;
import ru.leo.lsm.Config;
import ru.leo.lsm.Dao;
import ru.leo.lsm.Entry;

public interface DaoFactory<D, E extends Entry<D>> {

    Dao<D, E> createDao(Config config) throws IOException;

    String toString(D data);

    D fromString(String data);

    E fromBaseEntry(Entry<D> baseEntry);

    static Config extractConfig(Dao<String, Entry<String>> dao) {
        return ((TestDao<?, ?>) dao).config;
    }

    static Dao<String, Entry<String>> reopen(Dao<String, Entry<String>> dao) throws IOException {
        return ((TestDao<?, ?>) dao).reopen();
    }

    default Dao<String, Entry<String>> createStringDao(Config config) throws IOException {
        return new TestDao<>(this, config);
    }
}