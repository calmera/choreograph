package com.github.calmera.kafka.choreograph.processors;

import com.github.calmera.kafka.choreograph.events.Event;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

public abstract class AbstractEventProcessor<T extends Event> implements Processor<String, Event, String, Event> {
    private final Class<T> eventClass;

    public AbstractEventProcessor(Class<T> eventClass) {
        this.eventClass = eventClass;
    }

    @Override
    public void process(Record<String, Event> record) {
        if (eventClass.isInstance(record.value())) {
            processEvent(record.withValue(eventClass.cast(record.value())));
        }
    }

    public abstract void processEvent(Record<String, T> record);
}
