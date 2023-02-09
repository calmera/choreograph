package com.github.calmera.kafka.choreograph.events;

import java.util.HashMap;
import java.util.Map;

public class CreateRoutineRequested implements Event {
    private String label;

    private String cronExpression;

    private String topic;

    private String key;

    private String payload;

    private Map<String, String> headers;

    public static Builder newBuilder() {
        return new Builder();
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

    public String getKey() {
        return key;
    }

    public String getPayload() {
        return payload;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public static class Builder {
        private String label;
        private String cronExpression;

        private String key;
        private String topic;
        private String payload;
        private Map<String, String> headers;

        public Builder withLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder withCronExpression(String cronExpression) {
            this.cronExpression = cronExpression;
            return this;
        }

        public Builder withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder withKey(String key) {
            this.key = key;
            return this;
        }

        public Builder withPayload(String payload) {
            this.payload = payload;
            return this;
        }

        public Builder withHeaders(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public Builder withHeader(String key, String value) {
            if (this.headers == null) {
                this.headers = new HashMap<>();
            }

            this.headers.put(key, value);
            return this;
        }

        public CreateRoutineRequested build() {
            CreateRoutineRequested createRoutineRequested = new CreateRoutineRequested();
            createRoutineRequested.label = this.label;
            createRoutineRequested.cronExpression = this.cronExpression;
            createRoutineRequested.topic = this.topic;
            createRoutineRequested.key = this.key;
            createRoutineRequested.payload = this.payload;
            createRoutineRequested.headers = this.headers;
            return createRoutineRequested;
        }
    }
}
