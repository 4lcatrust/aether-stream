package com.aetherstream.bronze.util;
import com.aetherstream.bronze.model.MarketCapBronze;
import com.aetherstream.bronze.model.MarketPriceBronze;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import static com.aetherstream.bronze.util.AvroDecimalUtil.toBytes;
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
        JsonNode payload = op.equals("d") ? root.get("before") : root.get("after");
        if (payload == null || payload.isNull() || source == null || source.isNull()) {
            return null;
        }
        MarketPriceBronze out = new MarketPriceBronze();
        // ===== source columns =====
        out.setAssetId(textOrNull(payload, "asset_id"));
        out.setSymbol(textOrNull(payload, "symbol"));
        out.setCoinName(textOrNull(payload, "coin_name"));
        out.setCurrency(textOrNull(payload, "currency"));
        out.setPrice(toBytes(decimalOrNull(payload, "price")));
        out.setMarketCap(toBytes(decimalOrNull(payload, "market_cap")));
        out.setVolume24h(toBytes(decimalOrNull(payload, "volume_24h")));
        out.setCoinImage(textOrNull(payload, "coin_image"));
        out.setEventTimeMs(timestampToMillis(payload, "event_time"));
        out.setCreatedAtMs(timestampToMillis(payload, "created_at"));
        out.setUpdatedAtMs(timestampToMillis(payload, "updated_at"));
        // ===== CDC metadata =====
        out.setOp(op);
        out.setLsn(longOrNull(source, "lsn"));
        out.setSourceTsMs(longOrNull(source, "ts_ms"));
        out.setSourceDb(textOrNull(source, "db"));
        out.setSourceSchema(textOrNull(source, "schema"));
        out.setSourceTable(textOrNull(source, "table"));
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
        JsonNode payload = op.equals("d") ? root.get("before") : root.get("after");
        if (payload == null || payload.isNull() || source == null || source.isNull()) {
            return null;
        }
        MarketCapBronze out = new MarketCapBronze();
        // ===== source columns =====
        out.setAssetId(textOrNull(payload, "asset_id"));
        out.setSymbol(textOrNull(payload, "symbol"));
        out.setCoinName(textOrNull(payload, "coin_name"));
        out.setCurrency(textOrNull(payload, "currency"));
        out.setMarketCap(toBytes(decimalOrNull(payload, "market_cap")));
        out.setCirculatingSupply(toBytes(decimalOrNull(payload, "circulating_supply")));
        out.setTotalSupply(toBytes(decimalOrNull(payload, "total_supply")));
        out.setMaxSupply(toBytes(decimalOrNull(payload, "max_supply")));
        out.setEventTimeMs(timestampToMillis(payload, "event_time"));
        out.setCreatedAtMs(timestampToMillis(payload, "created_at"));
        out.setUpdatedAtMs(timestampToMillis(payload, "updated_at"));
        // ===== CDC metadata =====
        out.setOp(op);
        out.setLsn(longOrNull(source, "lsn"));
        out.setSourceTsMs(longOrNull(source, "ts_ms"));
        out.setSourceDb(textOrNull(source, "db"));
        out.setSourceSchema(textOrNull(source, "schema"));
        out.setSourceTable(textOrNull(source, "table"));
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
        String s = v.asText();
        if (s == null || s.isEmpty()) return null;
        try {
            return new BigDecimal(s);
        } catch (Exception e) {
            return null;
        }
    }
    private static Long timestampToMillis(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) return null;
        try {
            if (v.isTextual()) {
                return java.time.OffsetDateTime.parse(v.asText())
                        .toInstant()
                        .toEpochMilli();
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