package com.aetherstream.bronze.util;

import com.aetherstream.bronze.model.MarketPriceBronze;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DebeziumParser {
    private static final ObjectMapper mapper = new ObjectMapper();
    public static MarketPriceBronze parse(String json) throws Exception {
        JsonNode root = mapper.readTree(json);
        JsonNode opNode = root.get("op");
        if (opNode == null) return null;
        String op = opNode.asText();
        if (!op.equals("c") && !op.equals("u")) {
            return null;
        }
        JsonNode after = root.get("after");
        JsonNode source = root.get("source");
        if (after == null || after.isNull() || source == null || source.isNull()) {
            return null;
        }
        MarketPriceBronze out = new MarketPriceBronze();
        out.assetId   = textOrNull(after, "asset_id");
        out.currency  = textOrNull(after, "currency");
        out.price     = textOrNull(after, "price");
        out.marketCap = textOrNull(after, "market_cap");
        out.volume24h = textOrNull(after, "volume_24h");
        out.eventTimeMs = longOrNull(root, "ts_ms");
        out.sourceTsMs  = longOrNull(source, "ts_ms");
        out.op  = op;
        out.lsn = longOrNull(source, "lsn");
        return out;
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private static Long longOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) return null;
        if (v.isNumber()) return v.asLong();
        String s = v.asText();
        if (s == null || s.isEmpty()) return null;
        return Long.parseLong(s);
    }
}
