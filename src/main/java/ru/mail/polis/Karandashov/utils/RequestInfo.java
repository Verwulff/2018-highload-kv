package ru.mail.polis.Karandashov.utils;

public class RequestInfo {

    private String id;
    private int ack;
    private int from;
    private boolean proxied;

    public RequestInfo(String id, String replicas, int topologyLength, String proxied) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException();
        }
        this.id = id;
        this.proxied = proxied != null;
        if (replicas == null) {
            ack = topologyLength / 2 + 1;
            from = topologyLength;
            return;
        }
        String parts[] = replicas.split("/");
        if (parts.length != 2) {
            throw new IllegalArgumentException();
        }
        ack = Integer.parseInt(parts[0]);
        from = Integer.parseInt(parts[1]);
        if (ack <= 0 || ack > from) {
            throw new IllegalArgumentException();
        }
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }

    public String getId() {
        return id;
    }

    public boolean isProxied() {
        return proxied;
    }
}
