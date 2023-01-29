package com.github.calmera.kafka.choreograph;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import com.github.calmera.kafka.choreograph.events.CreateRoutineRequested;
import com.github.calmera.kafka.choreograph.events.Event;
import com.github.calmera.kafka.choreograph.events.RoutineCreated;
import com.github.calmera.kafka.choreograph.events.RoutineOperationFailed;
import com.github.calmera.kafka.choreograph.serdes.CustomSerdes;
import com.github.calmera.kafka.choreograph.serdes.JsonEventDeserializer;
import com.github.calmera.kafka.choreograph.state.Routine;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ChoreographTest {
    private TopologyTestDriver testDriver;

    private TestInputTopic<String, Event> inputTopic;

    private TestOutputTopic<String, Event> outputTopic;

    private KeyValueStore<String, Routine> routineStore;

    private final CronParser parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

    @BeforeEach
    public void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "choreograph");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "target/kafka-state");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomSerdes.Event().getClass());

        Choreograph choreograph = new Choreograph(props, "choreograph-events");

        // setup test driver
        testDriver = new TopologyTestDriver(choreograph.buildTopology(), choreograph.getProperties());

        // setup test topics
        inputTopic = testDriver.createInputTopic(
                choreograph.getEventsTopic(),
                Serdes.String().serializer(),
                CustomSerdes.Event().serializer());

        outputTopic = testDriver.createOutputTopic(
                choreograph.getEventsTopic(),
                Serdes.String().deserializer(),
                CustomSerdes.Event().deserializer());

        // set up the store
        routineStore = testDriver.getKeyValueStore(Choreograph.ROUTINE_STORE);
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    void shouldRegisterARoutine() {
        // given
        CreateRoutineRequested createRequest = CreateRoutineRequested.newBuilder()
                .withLabel("test routine")
                .withCronExpression("0 * * L-3 * ? *")
                .withTopic("test-target-topic")
                .withPayload("{\"field\": \"value\"}")
                .withHeader("my-header", "my-value")
                .build();

        // when
        inputTopic.pipeInput(asTestRecord("my-routine-id", createRequest));

        // then
        // make sure a success event was published
        List<TestRecord<String, Event>> records = outputTopic.readRecordsToList();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).value()).isInstanceOf(RoutineCreated.class).satisfies(event -> {
            RoutineCreated routineCreated = (RoutineCreated) event;
            assertThat(routineCreated.getId()).isEqualTo("my-routine-id");
            assertThat(routineCreated.getLabel()).isEqualTo("test routine");
            assertThat(routineCreated.getCronExpression()).isEqualTo("0 * * L-3 * ? *");
            assertThat(routineCreated.getTopic()).isEqualTo("test-target-topic");
            assertThat(routineCreated.getPayload()).isEqualTo("{\"field\": \"value\"}");
            assertThat(routineCreated.getHeaders()).containsEntry("my-header", "my-value");
        });

        // make sure the routine was persisted
        Routine result = routineStore.get("my-routine-id");
        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo("my-routine-id");
        assertThat(result.getLabel()).isEqualTo("test routine");
        assertThat(result.getCron().asString()).isEqualTo("0 * * L-3 * ? *");
        assertThat(result.getTopic()).isEqualTo("test-target-topic");
        assertThat(result.getPayload()).isEqualTo("{\"field\": \"value\"}");
        assertThat(result.getHeaders()).containsEntry("my-header", "my-value");
    }

    @Test
    void shouldNotRegisterARoutineWithAWrongCronExpression() {
        // given
        CreateRoutineRequested createRequest = CreateRoutineRequested.newBuilder()
                .withLabel("test routine")
                .withCronExpression("bogus expression")
                .withTopic("test-target-topic")
                .withPayload("{\"field\": \"value\"}")
                .withHeader("my-header", "my-value")
                .build();

        // when
        inputTopic.pipeInput(asTestRecord("my-faulty-routine-id", createRequest));

        // then
        // make sure an error event was published
        List<TestRecord<String, Event>> records = outputTopic.readRecordsToList();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).value()).isInstanceOf(RoutineOperationFailed.class).satisfies(event -> {
            RoutineOperationFailed evt = (RoutineOperationFailed) event;
            assertThat(evt.getId()).isEqualTo("my-faulty-routine-id");
            assertThat(evt.getError()).isEqualTo("Invalid cron expression: bogus expression");
        });

        // make sure the routine was not persisted
        Routine result = routineStore.get("my-faulty-routine-id");
        assertThat(result).isNull();
    }

    TestRecord<String, Event> asTestRecord(String key, Event value) {
        TestRecord<String, Event> result = new TestRecord<>(key, value, Instant.now());

        result.headers().add(
                JsonEventDeserializer.EVENT_TYPE_HEADER,
                value.getClass().getSimpleName().getBytes(StandardCharsets.UTF_8)
        );

        return result;
    }
}