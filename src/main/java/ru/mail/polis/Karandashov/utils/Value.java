package ru.mail.polis.Karandashov.utils;

import java.io.Serializable;
import java.util.Arrays;

public class Value implements Serializable {

    public byte[] data;
    private long timestamp;
    public static final byte[] EMPTY_DATA = new byte[0];
    public static final Value UNKNOWN = new Value(EMPTY_DATA, 0, stateCode.UNKNOWN);
    private stateCode state;

    public Value() {
        this.data = EMPTY_DATA;
        this.timestamp = System.currentTimeMillis();
        this.state = stateCode.DELETED;
    }

    public Value(byte[] value) {
        this.data = value;
        this.timestamp = System.currentTimeMillis();
        state = stateCode.PRESENT;
    }

    public Value(byte[] value, long timestamp, stateCode state) {
        this.data = value;
        this.timestamp = timestamp;
        this.state = state;
    }

    public stateCode getState() {
        return state;
    }

    public byte[] getData() {
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Value{" +
                "data=" + Arrays.toString(data) +
                ", timestamp=" + timestamp +
                ", state=" + state +
                '}';
    }

    public enum stateCode {
        PRESENT,
        DELETED,
        UNKNOWN
    }
}