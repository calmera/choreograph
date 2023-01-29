package com.github.calmera.kafka.choreograph.utils;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

public class CronJsonDeserializer extends JsonDeserializer<Cron> {
    private static final CronParser PARSER = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

    @Override
    public Cron deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        return PARSER.parse(jsonParser.getText());
    }
}
