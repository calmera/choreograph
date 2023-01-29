package com.github.calmera.kafka.choreograph.serdes;

import com.github.calmera.kafka.choreograph.events.Event;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Map;

public class CustomSerdes {

    public static <T> Serde<T> Pojo(Class<T> cls) {
        return new PojoSerde<>(cls);
    }

    static public final class PojoSerde<T> extends Serdes.WrapperSerde<T> {
        public PojoSerde(Class<T> cls) {
            super(new JsonSerializer<>(), new JsonPojoDeserializer<>());
            this.deserializer().configure(Map.of(JsonPojoDeserializer.TARGET_CLASS_CONF, cls), false);
        }
    }

    public static Serde<Event> Event() {
        return new EventSerde();
    }

    static public final class EventSerde extends Serdes.WrapperSerde<Event> {
        public EventSerde() {
            super(new JsonSerializer<>(), new JsonEventDeserializer());
        }
    }
}
