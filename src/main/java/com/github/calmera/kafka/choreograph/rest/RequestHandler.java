package com.github.calmera.kafka.choreograph.rest;

import java.util.Map;

public interface RequestHandler<P, R> {
    R process(P value, Map<String, String> params);
}
