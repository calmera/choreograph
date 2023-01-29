package com.github.calmera.kafka.choreograph.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.calmera.kafka.choreograph.events.Event;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class JsonEventDeserializer implements Deserializer<Event> {
    public static final String EVENT_TYPE_HEADER = "event-type";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Map<byte[], Class<? extends Event>> cache = new HashMap<>();

    /**
     * Default constructor needed by Kafka
     */
    public JsonEventDeserializer() {
    }

    @Override
    public Event deserialize(String topic, Headers headers, byte[] data) {
        if (data == null)
            return null;

        if (headers == null)
            throw new SerializationException("Headers are required for deserializing events");

        Header eventTypeHeader = headers.lastHeader(EVENT_TYPE_HEADER);
        Class<? extends Event> eventClass = getEventClass(eventTypeHeader);

        Event result;
        try {
            result = objectMapper.readValue(data, eventClass);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return result;
    }

    @Override
    public Event deserialize(String topic, byte[] bytes) {
        throw new SerializationException("Headers are required for deserializing events");
    }

    private Class<? extends Event> getEventClass(Header eventTypeHeader) {
        if (eventTypeHeader == null)
            throw new SerializationException("Missing event type header");

        if (cache.containsKey(eventTypeHeader.value()))
            return cache.get(eventTypeHeader.value());

        return cache.computeIfAbsent(eventTypeHeader.value(), this::loadEventClass);
    }

    private Class<? extends Event> loadEventClass(byte[] eventTypeBytes) {
        String eventType = new String(eventTypeBytes, StandardCharsets.UTF_8);

        Class<?> cls;
        try {
            cls = Event.class.getClassLoader().loadClass(String.format("%s.%s", Event.class.getPackage().getName(), eventType));
        } catch (ClassNotFoundException e) {
            throw new SerializationException("Could not find event class " + eventType, e);
        }

        if (Event.class.isAssignableFrom(cls)) {
            return (Class<? extends Event>) cls;
        } else {
            throw new SerializationException("Class " + eventType + " is not an event");
        }
    }
}