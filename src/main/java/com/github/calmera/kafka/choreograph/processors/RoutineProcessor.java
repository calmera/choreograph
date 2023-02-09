package com.github.calmera.kafka.choreograph.processors;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.github.calmera.kafka.choreograph.events.*;
import com.github.calmera.kafka.choreograph.serdes.JsonEventDeserializer;
import com.github.calmera.kafka.choreograph.serdes.JsonSerializer;
import com.github.calmera.kafka.choreograph.state.Routine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;

public class RoutineProcessor implements Processor<String, Event, String, Event> {
    private final CronParser CRON_PARSER = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

    private ProcessorContext<String, Event> context;

    private KeyValueStore<String, Routine> routineStore;

    private final Duration PUNCTUATION_INTERVAL = Duration.ofSeconds(1);

    private KafkaProducer<String, String> producer;


    @Override
    public void init(ProcessorContext<String, Event> context) {
        this.context = context;
        this.routineStore = context.getStateStore("routineStore");

        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, (ts) -> this.evaluateExecutions(context, ts));

        Map<String, Object> producerConfig = new HashMap<>(context.appConfigs());

        producerConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "routine-producer");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        producer = new KafkaProducer<>(producerConfig);
    }

    @Override
    public void process(Record<String, Event> record) {
        if (record.value() == null) {
            return;
        }

        if (record.value() instanceof CreateRoutineRequested) {
            processCreateEvent(record.withValue((CreateRoutineRequested) record.value()));
        }
    }

    public void processCreateEvent(Record<String, CreateRoutineRequested> record) {
        String id = record.key();

        // we will fail if a routine already exists with the given id
        if (routineStore.get(id) != null) {
            forward(record.key(), new RoutineOperationFailed(id, "Routine with id " + id + " already exists"));
            return;
        }

        CreateRoutineRequested createRoutineRequested = record.value();

        Cron cron;
        try {
            cron = CRON_PARSER.parse(createRoutineRequested.getCronExpression());
        } catch (IllegalArgumentException iae) {
            forward(record.key(), new RoutineOperationFailed(id, "Invalid cron expression: " + createRoutineRequested.getCronExpression()));
            return;
        }

        Routine routine = new Routine(id, cron, createRoutineRequested.getTopic());
        routine.setLabel(createRoutineRequested.getLabel());
        routine.setHeaders(createRoutineRequested.getHeaders());
        routine.setKey(createRoutineRequested.getKey());
        routine.setPayload(createRoutineRequested.getPayload());

        routineStore.put(id, routine);

        forward(record.key(), new RoutineCreated(routine.getId(), routine.getLabel(), routine.getCron().asString(), routine.getTopic(), routine.getKey(), routine.getPayload(), routine.getHeaders()));
    }

    private void evaluateExecutions(ProcessorContext<String, Event> context, long timestamp) {
        ZonedDateTime zdt = ZonedDateTime.now();

        try (KeyValueIterator<String, Routine> routines = routineStore.all()) {
            routines.forEachRemaining(routine -> {
                ExecutionTime executionTime = ExecutionTime.forCron(routine.value.getCron());
                Optional<Duration> duration = executionTime.timeFromLastExecution(zdt);

                if (duration.isEmpty() || duration.get().minus(PUNCTUATION_INTERVAL).isNegative()) {
                    List<Header> headers = new ArrayList<>();
                    if (routine.value.getHeaders() != null) {
                        routine.value.getHeaders().forEach((k, v) -> headers.add(new RecordHeader(k, v.getBytes())));
                    }

                    producer.send(new ProducerRecord<>(routine.value.getTopic(), null, timestamp, routine.value.getKey(), routine.value.getPayload(), new RecordHeaders(headers)));
                    forward(routine.value.getKey(), new RoutineExecuted(routine.value.getId(), routine.value.getLabel(), routine.value.getCron().asString(), routine.value.getTopic(), routine.value.getKey(), routine.value.getPayload(), routine.value.getHeaders()));
                }
            });
        }
    }

    private void forward(String key, Event value) {
        context.forward(new Record<>(key, value, context.currentSystemTimeMs(), new RecordHeaders(List.of(
                new RecordHeader(JsonEventDeserializer.EVENT_TYPE_HEADER, value.getClass().getSimpleName().getBytes()))
        )));
    }
}
