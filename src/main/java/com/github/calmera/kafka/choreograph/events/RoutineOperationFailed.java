package com.github.calmera.kafka.choreograph.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RoutineOperationFailed implements Event {
    private final String id;
    private final String error;

    @JsonCreator
    public RoutineOperationFailed(@JsonProperty("id") String id,
                                  @JsonProperty("error") String error) {
        this.id = id;
        this.error = error;
    }

    public String getId() {
        return id;
    }

    public String getError() {
        return error;
    }
}
