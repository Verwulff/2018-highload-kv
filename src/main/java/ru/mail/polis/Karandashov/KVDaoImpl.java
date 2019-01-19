package ru.mail.polis.Karandashov;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {
    DB db;
    HTreeMap<byte[], byte[]> map;

    public KVDaoImpl(File data) {
        db = DBMaker
                .fileDB(data.getAbsolutePath() + "//db")
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                .fileChannelEnable()
                .closeOnJvmShutdown()
                .make();
        map = db
                .hashMap("data")
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .createOrOpen();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        byte[] val = map.get(key);
        if (val == null) throw new NoSuchElementException();
        return val;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        map.put(key, value);
    }

    @Override
    public void remove(@NotNull byte[] key) {
        map.remove(key);
    }

    @Override
    public void close() {
        db.close();
    }
}