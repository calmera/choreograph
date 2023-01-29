/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.calmera.kafka.choreograph;

import com.github.calmera.kafka.choreograph.events.CreateRoutineRequested;
import com.github.calmera.kafka.choreograph.events.Event;
import com.github.calmera.kafka.choreograph.processors.CreateRoutineProcessor;
import com.github.calmera.kafka.choreograph.serdes.CustomSerdes;
import com.github.calmera.kafka.choreograph.state.Routine;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Choreograph {
    public static final Logger LOGGER = LoggerFactory.getLogger(Choreograph.class);
    public static final String ROUTINE_STORE = "routineStore";

    private final Properties properties;

    private final String eventsTopic;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "choreograph");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomSerdes.Event().getClass());

        Choreograph app = new Choreograph(props, "choreograph-events");

        try {
            app.start();
        } catch (Throwable e) {
            System.exit(1);
        }

        System.exit(0);
    }

    public Choreograph(Properties properties, String eventsTopic) {
        this.properties = properties;
        this.eventsTopic = eventsTopic;
    }

    protected Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ROUTINE_STORE),
                Serdes.String(),
                CustomSerdes.Pojo(Routine.class)
        ));

        KStream<String, Event> eventStream = builder.stream(this.eventsTopic, Consumed.with(Serdes.String(), CustomSerdes.Event()).withName("eventsSource"));

        eventStream.split()
                .branch((key, event) -> event instanceof CreateRoutineRequested, Branched.<String, Event>withConsumer(createEvents -> {
                    createEvents
                            .process(CreateRoutineProcessor::new, Named.as("createRoutineProcessor"), ROUTINE_STORE)
                            .to(this.eventsTopic, Produced.with(Serdes.String(), CustomSerdes.Event()).withName("createRoutineSink"));
                }).withName("createRoutineRequested"))
                .noDefaultBranch();


        return builder.build();
    }

    public void start() throws InterruptedException {
        final Topology topology = buildTopology();
        LOGGER.info(topology.describe().toString());

        final KafkaStreams streams = new KafkaStreams(topology, this.properties);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        streams.start();
        latch.await();
    }

    public String getEventsTopic() {
        return eventsTopic;
    }

    public Properties getProperties() {
        return properties;
    }
}
