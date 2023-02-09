package com.github.calmera.kafka.choreograph.rest.routines;

import com.github.calmera.kafka.choreograph.rest.Resource;
import spark.Spark;

public class RoutineResource implements Resource {
    public void registerRoutes() {
        Spark.path("/routines/:routineId", () -> {
            Spark.get("/", (req, res) -> {
                return "{}";
            });

            Spark.delete("/", (req, res) -> {
                return "";
            });
        });
    }
}
