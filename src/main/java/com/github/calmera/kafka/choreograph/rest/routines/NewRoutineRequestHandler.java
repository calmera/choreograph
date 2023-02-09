package com.github.calmera.kafka.choreograph.rest.routines;

import com.github.calmera.kafka.choreograph.rest.AbstractRequestHandler;

import java.util.Map;

public class NewRoutineRequestHandler extends AbstractRequestHandler<NewRoutineRequestHandler.Request, NewRoutineRequestHandler.Response> {
    public NewRoutineRequestHandler() {
        super(Request.class);
    }

    @Override
    protected Response processImpl(Request value, Map<String, String> queryParams) {
        return null;
    }

    static class Request {
        private String label;

        private String cronExpression;

        private String topic;

        private String payload;

        private Map<String, String> headers;

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public String getCronExpression() {
            return cronExpression;
        }

        public void setCronExpression(String cronExpression) {
            this.cronExpression = cronExpression;
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

    static class Response {
        private final String id;

        public Response(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }
}
