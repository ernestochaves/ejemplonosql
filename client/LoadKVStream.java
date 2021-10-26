package client;

import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.EntryStream;
import oracle.kv.KeyValue;
import oracle.kv.*;

public class LoadKVStream implements EntryStream<KeyValue> {
    private final String name;
    private final long index;
    private final long max;
    private final long min;
    private long id;
    private long count;
    private final AtomicLong keyExistsCount;

    LoadKVStream(String name, long index, long min, long max) {
        this.index = index;
        this.max = max;
        this.min = min;
        this.name = name;
        id = min;
        count = 0;
        keyExistsCount = new AtomicLong();
    }

    @Override
    public String name() {
        return name + "-" + index + ": " + min + "~" + max;
    }

    @Override
    public KeyValue getNext() {
        if (id++ == max) {
            return null;
        }
        Key key = Key.fromString("/list/" + id);
        Value value = Value.createValue((NumberToWords.convert(id)).getBytes());
        KeyValue kv = new KeyValue(key, value);
        count++;
        return kv;
    }

    @Override
    public void completed() {
        System.err.println(name() + " completed, loaded: " + count);
    }

    @Override
    public void keyExists(KeyValue entry) {
        keyExistsCount.incrementAndGet();
    }

    @Override
    public void catchException(RuntimeException exception, KeyValue entry) {
        System.err.println(name() + " catch exception: " + exception.getMessage() + ": " + entry.toString());
        throw exception;
    }

    public long getCount() {

        return count;
    }

    public long getKeyExistsCount() {
        return keyExistsCount.get();
    }
}
