package ru.mail.polis.Karandashov;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import ru.mail.polis.Karandashov.utils.RequestInfo;
import ru.mail.polis.Karandashov.utils.Value;
import ru.mail.polis.Karandashov.utils.ValueSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KVServiceImpl extends HttpServer implements KVService {
    private final KVDao dao;
    private final String[] topology;
    private final Map<String, HttpClient> nodes;
    private final String me;
    private final ValueSerializer serializer;
    private final Handler handler;
    private final Logger logger = LoggerFactory.getLogger(KVServiceImpl.class);

    public KVServiceImpl(final int port, KVDao dao, Set<String> topology) throws IOException {
        super(getConfig(port));
        this.dao = dao;
        this.topology = topology.toArray(new String[0]);
        me = "http://localhost:" + port;
        nodes = topology.stream().collect(Collectors.toMap(
                o -> o,
                o -> new HttpClient(new ConnectionString(o))));
        serializer = ValueSerializer.getInstance();
        logger.info("Server has ben started");

        handler = new Handler(dao, me, serializer, nodes);
    }

    private static HttpServerConfig getConfig(int port) {
        HttpServerConfig serverConfig = new HttpServerConfig();
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        serverConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        return serverConfig;
    }

    @Path("/v0/status")
    public Response status(Request request) {
        logger.info(request.getURI());
        logger.debug(request.toString());
        return new Response(Response.OK, Response.EMPTY);
    }

    @Path("/v0/entity")
    public Response entity(
            Request request,
            @Param(value = "id") String id,
            @Param(value = "replicas") String replicas) {

        RequestInfo requestInfo;
        try {
            requestInfo = new RequestInfo(id, replicas, topology.length, request.getHeader("Proxied"));
        } catch (IllegalArgumentException e) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return requestInfo.isProxied() ?
                        handler.get(id) :
                        handler.proxyGet(id, requestInfo.getAck(), getNodes(id, requestInfo.getFrom()));
            case Request.METHOD_PUT:
                if (requestInfo.isProxied()) {
                    return handler.upsert(id, request.getBody());
                }
                try {
                    handler.proxyUpsert(id, requestInfo.getAck(), getNodes(id, requestInfo.getFrom()), new Value(request.getBody()));
                    return new Response(Response.CREATED, Response.EMPTY);
                } catch (Exception e) {
                    return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
                }
            case Request.METHOD_DELETE:
                if (requestInfo.isProxied()) {
                    return handler.upsert(id, request.getBody());
                }
                try {
                    handler.proxyUpsert(id, requestInfo.getAck(), getNodes(id, requestInfo.getFrom()), new Value());
                    return new Response(Response.ACCEPTED, Response.EMPTY);
                } catch (Exception e) {
                    return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
                }
        }

        return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        logger.info(request.getURI());
        logger.debug(request.toString());
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    private List<String> getNodes(String key, int length) {
        final List<String> clients = new ArrayList<>();
        int firstNodeId = (key.hashCode() & Integer.MAX_VALUE) % topology.length;
        clients.add(topology[firstNodeId]);
        for (int i = 1; i < length; i++) {
            clients.add(topology[(firstNodeId + i) % topology.length]);
        }
        return clients;
    }

}