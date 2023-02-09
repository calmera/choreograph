package com.github.calmera.kafka.choreograph.rest;

import org.apache.kafka.streams.KafkaStreams;

import java.util.List;

import static spark.Spark.stop;

public class Api {
    private final List<Resource> resources;

    public Api(List<Resource> resources) {
        this.resources = resources;
    }

    public void startListening(KafkaStreams kafkaStreams) {
        resources.forEach(Resource::registerRoutes);
    }

    public void stopListening() {
        stop();
    }

}
