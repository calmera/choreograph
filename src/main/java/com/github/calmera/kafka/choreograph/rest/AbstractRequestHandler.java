package com.github.calmera.kafka.choreograph.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import spark.Request;
import spark.Response;
import spark.Route;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractRequestHandler<P, R> implements RequestHandler<P, R>, Route {
    private final Class<P> payloadClass;

    public AbstractRequestHandler(Class<P> payloadClass) {
        this.payloadClass = payloadClass;
    }

    public static String dataToJson(Object data) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            StringWriter sw = new StringWriter();
            mapper.writeValue(sw, data);
            return sw.toString();
        } catch (IOException e) {
            throw new RuntimeException("IOException from a StringWriter?");
        }
    }

    public final R process(P value, Map<String, String> queryParams) {
        return processImpl(value, queryParams);
    }

    protected abstract R processImpl(P value, Map<String, String> queryParams);


    @Override
    public Object handle(Request request, Response response) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        P value = objectMapper.readValue(request.body(), payloadClass);
        Map<String, String> queryParams = new HashMap<>();
        try {
            R answer = process(value, queryParams);
            response.status(200);
            response.type("application/json");

            return answer;
        } catch (Exception e) {
            response.status(501);
            response.type("application/json");
            response.body(dataToJson(Map.of("error", e.getMessage())));
            return null;
        }
    }
}