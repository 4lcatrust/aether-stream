package com.aetherstream.bronze.util;
import com.aetherstream.bronze.model.MarketPriceBronze;
import com.aetherstream.bronze.model.MarketCapBronze;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;

public class DebeziumParser {
    private static final ObjectMapper mapper = new ObjectMapper();
    // =========================================================
    // Market Prices
    // =========================================================
    public static MarketPriceBronze parseMarketPrice(String json) throws Exception {
        JsonNode root = mapper.readTree(json);
        String op = textOrNull(root, "op");
        if (op == null) return null;
        JsonNode source = root.get("source");
        JsonNode payload =
                op.equals("d")
                        ? root.get("before")
                        : root.get("after");
        if (payload == null || payload.isNull() || source == null || source.isNull()) {
            return null;
        }
        MarketPriceBronze out = new MarketPriceBronze();
        // ===== source columns =====
        out.assetId   = textOrNull(payload, "asset_id");
        out.symbol    = textOrNull(payload, "symbol");
        out.coinName  = textOrNull(payload, "coin_name");
        out.currency  = textOrNull(payload, "currency");
        out.price     = decimalOrNull(payload, "price");
        out.marketCap = decimalOrNull(payload, "market_cap");
        out.volume24h = decimalOrNull(payload, "volume_24h");
        out.coinImage = textOrNull(payload, "coin_image");
        out.eventTimeMs   = timestampToMillis(payload, "event_time");
        out.createdAtMs   = timestampToMillis(payload, "created_at");
        out.updatedAtMs   = timestampToMillis(payload, "updated_at");

        out.op        = op;
        out.lsn       = longOrNull(source, "lsn");
        out.sourceTsMs = longOrNull(source, "ts_ms");
        out.sourceDb     = textOrNull(source, "db");
        out.sourceSchema = textOrNull(source, "schema");
        out.sourceTable  = textOrNull(source, "table");
        return out;
    }

    // =========================================================
    // Market Caps
    // =========================================================
    public static MarketCapBronze parseMarketCap(String json) throws Exception {
        JsonNode root = mapper.readTree(json);
        String op = textOrNull(root, "op");
        if (op == null) return null;
        JsonNode source = root.get("source");
        JsonNode payload =
                op.equals("d")
                        ? root.get("before")
                        : root.get("after");
        if (payload == null || payload.isNull() || source == null || source.isNull()) {
            return null;
        }
        MarketCapBronze out = new MarketCapBronze();
        // ===== source columns =====
        out.assetId   = textOrNull(payload, "asset_id");
        out.symbol    = textOrNull(payload, "symbol");
        out.coinName  = textOrNull(payload, "coin_name");
        out.currency  = textOrNull(payload, "currency");
        out.marketCap         = decimalOrNull(payload, "market_cap");
        out.circulatingSupply = decimalOrNull(payload, "circulating_supply");
        out.totalSupply       = decimalOrNull(payload, "total_supply");
        out.maxSupply         = decimalOrNull(payload, "max_supply");
        out.eventTimeMs = timestampToMillis(payload, "event_time");
        out.createdAtMs = timestampToMillis(payload, "created_at");
        out.updatedAtMs = timestampToMillis(payload, "updated_at");

        out.op        = op;
        out.lsn       = longOrNull(source, "lsn");
        out.sourceTsMs = longOrNull(source, "ts_ms");
        out.sourceDb     = textOrNull(source, "db");
        out.sourceSchema = textOrNull(source, "schema");
        out.sourceTable  = textOrNull(source, "table");
        return out;
    }

    // =========================================================
    // Helpers
    // =========================================================
    private static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }
    private static Long longOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) return null;
        if (v.isNumber()) return v.asLong();
        String s = v.asText();
        return (s == null || s.isEmpty()) ? null : Long.parseLong(s);
    }
    private static BigDecimal decimalOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) return null;
        return new BigDecimal(v.asText());
    }
    private static Long timestampToMillis(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) return null;
        try {
            if (v.isTextual()) {
                return java.time.OffsetDateTime.parse(v.asText()).toInstant().toEpochMilli();
            }
            if (v.isObject() && v.has("value")) {
                return java.time.OffsetDateTime.parse(v.get("value").asText())
                        .toInstant()
                        .toEpochMilli();
            }
        } catch (Exception ignored) {}
        return null;
    }
}