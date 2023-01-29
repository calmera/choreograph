package com.github.calmera.kafka.choreograph.processors;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import com.github.calmera.kafka.choreograph.events.CreateRoutineRequested;
import com.github.calmera.kafka.choreograph.events.Event;
import com.github.calmera.kafka.choreograph.events.RoutineCreated;
import com.github.calmera.kafka.choreograph.events.RoutineOperationFailed;
import com.github.calmera.kafka.choreograph.serdes.JsonEventDeserializer;
import com.github.calmera.kafka.choreograph.state.Routine;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

public class CreateRoutineProcessor extends AbstractEventProcessor<CreateRoutineRequested> {
    private final CronParser CRON_PARSER = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
    private ProcessorContext<String, Event> context;

    private KeyValueStore<String, Routine> routineStore;

    public CreateRoutineProcessor() {
        super(CreateRoutineRequested.class);
    }

    @Override
    public void init(ProcessorContext<String, Event> context) {
        super.init(context);
        this.context = context;
        this.routineStore = context.getStateStore("routineStore");
    }

    @Override
    public void processEvent(Record<String, CreateRoutineRequested> record) {
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
        routine.setPayload(createRoutineRequested.getPayload());

        routineStore.put(id, routine);

        forward(record.key(), new RoutineCreated(routine.getId(), routine.getLabel(), routine.getCron().asString(), routine.getTopic(), routine.getPayload(), routine.getHeaders()));
    }

    private void forward(String key, Event value) {
        context.forward(new Record<>(key, value, context.currentSystemTimeMs(), new RecordHeaders(List.of(
                new RecordHeader(JsonEventDeserializer.EVENT_TYPE_HEADER, value.getClass().getSimpleName().getBytes()))
        )));
    }
}
