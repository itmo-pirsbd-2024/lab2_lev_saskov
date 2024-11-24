package ru.leo.lsm.helper;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import ru.leo.lsm.Config;
import ru.leo.lsm.Dao;
import ru.leo.lsm.Entry;

@Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ParameterizedTest
@ArgumentsSource(DaoTest.DaoList.class)
@Timeout(100)
public @interface DaoTest {
    class DaoList implements ArgumentsProvider {
        private static final long FLUSH_THRESHOLD = 100 * 1024 * 1024; // 100 mb

        private static final AtomicInteger ID = new AtomicInteger();
        private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create("dao");

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
            return Stream.of(Arguments.of(createDao(context)));
        }

        private Dao<String, Entry<String>> createDao(ExtensionContext context) throws IOException {
            Path tmp = Files.createTempDirectory("dao");

            DaoFactory<?, ?> f = new DaoStringFactory();
            Dao<String, Entry<String>> dao = f.createStringDao(new Config(tmp, FLUSH_THRESHOLD));
            ExtensionContext.Store.CloseableResource res = () -> {
                dao.close();
                if (!Files.exists(tmp)) {
                    return;
                }
                Files.walkFileTree(tmp, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                        Files.delete(path);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        if (exc != null) {
                            throw exc;
                        }

                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            };

            context.getStore(NAMESPACE).put(ID.incrementAndGet() + "", res);

            return dao;
        }
    }

}
