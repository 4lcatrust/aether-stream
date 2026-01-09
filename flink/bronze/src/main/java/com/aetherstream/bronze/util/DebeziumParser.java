package com.aetherstream.bronze.util;

import com.aetherstream.bronze.model.MarketPriceBronze;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;

public class DebeziumParser {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static MarketPriceBronze parse(String json) throws Exception {
        JsonNode root = mapper.readTree(json);

        String op = root.get("op").asText();
        if (!op.equals("c") && !op.equals("u")) {
            return null;
        }

        JsonNode after = root.get("after");
        JsonNode source = root.get("source");

        MarketPriceBronze out = new MarketPriceBronze();
        out.assetId   = after.get("asset_id").asText();
        out.currency  = after.get("currency").asText();
        out.price     = after.get("price").asText();
        out.marketCap = after.get("market_cap").asText();
        out.volume24h = after.get("volume_24h").asText();

        out.eventTime = Instant.parse(after.get("event_time").asText());
        out.sourceTs  = Instant.parse(after.get("source_ts").asText());

        out.op  = op;
        out.lsn = source.get("lsn").asLong();

        return out;
    }
}
