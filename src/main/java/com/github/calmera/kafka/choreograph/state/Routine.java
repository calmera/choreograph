package com.github.calmera.kafka.choreograph.state;

import com.cronutils.model.Cron;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.github.calmera.kafka.choreograph.utils.CronJsonDeserializer;
import com.github.calmera.kafka.choreograph.utils.CronJsonSerializer;

import java.util.Map;

public class Routine {
    private final String id;
    private String label;

    @JsonDeserialize(using = CronJsonDeserializer.class)
    @JsonSerialize(using = CronJsonSerializer.class)
    private Cron cron;

    private String topic;

    private String payload;

    private Map<String, String> headers;

    @JsonCreator
    public Routine(@JsonProperty("id") String id,
                   @JsonProperty("cron") Cron cron,
                   @JsonProperty("topic") String topic) {
        this.id = id;
        this.cron = cron;
        this.topic = topic;
    }

    public String getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Cron getCron() {
        return cron;
    }

    public void setCron(Cron cron) {
        this.cron = cron;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }
}
