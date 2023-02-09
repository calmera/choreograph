package com.github.calmera.kafka.choreograph.rest.routines;

import com.github.calmera.kafka.choreograph.rest.Resource;
import spark.Spark;

public class RoutinesResource implements Resource {
    public void registerRoutes() {
        Spark.path("/routines", () -> {
            Spark.get("/", (req, res) -> {
                return "[]";
            });

            Spark.post("/", (req, res) -> {
                return "";
            });
        });
    }
}
