package com.github.calmera.kafka.choreograph.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class RoutineExecuted implements Event {
    private final String id;
    private final String label;
    private final String cronExpression;

    private final String topic;

    private final String key;

    private final String payload;

    private final Map<String, String> headers;

    @JsonCreator
    public RoutineExecuted(@JsonProperty("id") String id,
                           @JsonProperty("label") String label,
                           @JsonProperty("cronExpression") String cronExpression,
                           @JsonProperty("topic") String topic,
                           @JsonProperty("key") String key,
                           @JsonProperty("payload") String payload,
                           @JsonProperty("headers") Map<String, String> headers) {
        this.id = id;
        this.label = label;
        this.cronExpression = cronExpression;
        this.topic = topic;
        this.key = key;
        this.payload = payload;
        this.headers = headers;
    }

    public String getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public String getTopic() {
        return topic;
    }

    public String getPayload() {
        return payload;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public String getKey() {
        return key;
    }
}
