package ru.mail.polis.Karandashov;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.Karandashov.utils.Value;
import ru.mail.polis.Karandashov.utils.ValueSerializer;

import java.io.IOException;
import java.util.*;

public class Handler {

    private final Logger logger = LoggerFactory.getLogger(Handler.class);

    private final KVDao dao;
    private final String me;
    private final ValueSerializer serializer;
    private final Map<String, HttpClient> nodes;

    public Handler(KVDao dao, String me, ValueSerializer serializer, Map<String, HttpClient> nodes) {
        this.dao = dao;
        this.me = me;
        this.serializer = serializer;
        this.nodes = nodes;
    }

    public Response proxyGet(String id, int ack, List<String> from) {
        List<Value> values = new ArrayList<>();
        for (String node : from) {
            if (node.equals(me)) {
                try {
                    byte[] res = dao.get(id.getBytes());
                    Value resValue = serializer.deserialize(res);
                    values.add(resValue);
                } catch (NoSuchElementException e) {
                    logger.warn("No such element", e);
                    values.add(Value.UNKNOWN);
                } catch (IOException e) {
                    logger.error("IO exception", e);
                }
            } else {
                try {
                    final Response response = nodes.get(node).get("/v0/entity?id=" + id, "Proxied: true");
                    switch (response.getStatus()) {
                        case 500:
                        case 404:
                            values.add(Value.UNKNOWN);
                        case 200:
                            byte[] data = response.getBody();
                            long timestamp = Long.parseLong(response.getHeader("Timestamp"));
                            String state = response.getHeader("State");
                            Value value = new Value(data, timestamp, Value.stateCode.valueOf(state));
                            values.add(value);
                        default:
                    }
                } catch (Exception e) {
                    logger.error("Error on request, no ack", e);
                }
            }
        }
        if (values.size() >= ack) {
            Value max = values.stream()
                    .max(Comparator.comparingLong(Value::getTimestamp))
                    .orElse(Value.UNKNOWN);
            switch (max.getState()) {
                case UNKNOWN:
                case DELETED:
                    return new Response(Response.NOT_FOUND, Response.EMPTY);
                case PRESENT:
                    return new Response(Response.OK, max.getData());
            }
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    public Response get(String id) {
        try {
            byte[] res = dao.get(id.getBytes());
            Value value = serializer.deserialize(res);
            Response response = new Response(Response.OK, value.getData());
            response.addHeader("Timestamp" + value.getTimestamp());
            response.addHeader("State" + value.getState());
            return response;
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    public void proxyUpsert(String id, int ack, List<String> from, Value value) throws Exception {
        int myAck = 0;

        byte[] serializedValue = serializer.serialize(value);
        for (String node : from) {
            if (node.equals(me)) {
                try {
                    dao.upsert(id.getBytes(), serializedValue);
                    myAck++;
                } catch (IOException e) {
                    logger.error("IOException with id=" + id, e);
                }
            } else {
                try {
                    final Response response = nodes.get(node).put("/v0/entity?id=" + id, serializedValue, "Proxied: true");
                    if (response.getStatus() != 500) {
                        myAck++;
                    }
                } catch (Exception e) {
                    logger.error("Bad answer, no ack from node " + node, e);
                }
            }
        }
        if (myAck < ack) {
            throw new Exception("Not enough acks");
        }
     }

    public Response upsert(String id, byte[] serializedValue) {
        try {
            dao.upsert(id.getBytes(), serializedValue);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return new Response(Response.CREATED, Response.EMPTY);
    }
}
