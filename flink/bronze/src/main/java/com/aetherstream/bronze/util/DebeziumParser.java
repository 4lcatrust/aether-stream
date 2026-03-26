package com.aetherstream.bronze.util;
import com.aetherstream.bronze.model.MarketCapBronze;
import com.aetherstream.bronze.model.MarketPriceBronze;
import org.apache.avro.generic.GenericRecord;
import java.math.BigDecimal;
import static com.aetherstream.bronze.util.AvroDecimalUtil.toBytes;
public class DebeziumParser {
    // =========================================================
    // Market Prices
    // =========================================================
    public static MarketPriceBronze parseMarketPrice(GenericRecord record) throws Exception {
        if (record == null) return null;
        String op = textOrNull(record, "op");
        if (op == null) return null;
        GenericRecord source  = (GenericRecord) record.get("source");
        GenericRecord payload = op.equals("d")
                ? (GenericRecord) record.get("before")
                : (GenericRecord) record.get("after");
        if (payload == null || source == null) return null;
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
        out.setSourceTsMs(timestampToMillis(payload, "source_ts"));
        out.setCreatedAtMs(timestampToMillis(payload, "created_at"));
        // ===== CDC metadata =====
        out.setOp(op);
        out.setLsn(longOrNull(source, "lsn"));
        out.setSourceDb(textOrNull(source, "db"));
        out.setSourceSchema(textOrNull(source, "schema"));
        out.setSourceTable(textOrNull(source, "table"));
        return out;
    }
    // =========================================================
    // Market Caps
    // =========================================================
    public static MarketCapBronze parseMarketCap(GenericRecord record) throws Exception {
        if (record == null) return null;
        String op = textOrNull(record, "op");
        if (op == null) return null;
        GenericRecord source  = (GenericRecord) record.get("source");
        GenericRecord payload = op.equals("d")
                ? (GenericRecord) record.get("before")
                : (GenericRecord) record.get("after");
        if (payload == null || source == null) return null;
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
    // Helpers — GenericRecord
    // =========================================================
    private static String textOrNull(GenericRecord record, String field) {
        Object v = record.get(field);
        if (v == null) return null;
        String s = v.toString();
        return s.isEmpty() ? null : s;
    }
    private static Long longOrNull(GenericRecord record, String field) {
        Object v = record.get(field);
        if (v == null) return null;
        if (v instanceof Long)    return (Long) v;
        if (v instanceof Integer) return ((Integer) v).longValue();
        String s = v.toString();
        return s.isEmpty() ? null : Long.parseLong(s);
    }
    private static BigDecimal decimalOrNull(GenericRecord record, String field) {
        Object v = record.get(field);
        if (v == null) return null;
        String s = v.toString();
        if (s.isEmpty()) return null;
        try {
            return new BigDecimal(s);
        } catch (Exception e) {
            return null;
        }
    }
    private static Long timestampToMillis(GenericRecord record, String field) {
        Object v = record.get(field);
        if (v == null) return null;
        try {
            return java.time.OffsetDateTime.parse(v.toString())
                    .toInstant()
                    .toEpochMilli();
        } catch (Exception ignored) {}
        if (v instanceof Long) return (Long) v / 1000;
        return null;
    }
}