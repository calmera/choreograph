package com.github.calmera.kafka.choreograph.events;

public class RemoveRoutineRequested implements Event {
    private final String id;

    public RemoveRoutineRequested(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
