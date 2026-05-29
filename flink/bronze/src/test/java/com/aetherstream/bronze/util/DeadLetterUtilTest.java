package com.aetherstream.bronze.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class DeadLetterUtilTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void carriesReasonAndRecord() throws Exception {
        JsonNode n = MAPPER.readTree(DeadLetterUtil.format("bad op", "raw-record"));
        assertEquals("bad op", n.get("reason").asText());
        assertEquals("raw-record", n.get("record").asText());
    }

    @Test
    void escapesQuotesAndNewlinesIntoValidJson() throws Exception {
        JsonNode n = MAPPER.readTree(
                DeadLetterUtil.format("line\nbreak", "has \"quotes\" and\ttab"));
        assertEquals("line\nbreak", n.get("reason").asText());
        assertEquals("has \"quotes\" and\ttab", n.get("record").asText());
    }

    @Test
    void nullRecordBecomesEmptyString() throws Exception {
        JsonNode n = MAPPER.readTree(DeadLetterUtil.format("null record", null));
        assertEquals("", n.get("record").asText());
    }
}
