package com.aetherstream.bronze.util;
import com.aetherstream.bronze.model.MarketCapBronze;
import com.aetherstream.bronze.model.MarketPriceBronze;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
public final class ClickHouseJsonUtil {
    private static final LogicalTypes.Decimal DECIMAL =
            LogicalTypes.decimal(38, 18);
    private static final Schema DECIMAL_SCHEMA =
            DECIMAL.addToSchema(Schema.create(Schema.Type.BYTES));
    private static final Conversions.DecimalConversion DECIMAL_CONVERSION =
            new Conversions.DecimalConversion();
    private ClickHouseJsonUtil() {}
    // =========================================================
    // Market Prices
    // =========================================================
    public static String toJson(MarketPriceBronze r) {
        StringBuilder sb = new StringBuilder(512);
        sb.append("{");
        appendString(sb, "assetId", r.getAssetId());
        appendString(sb, "symbol", r.getSymbol());
        appendString(sb, "coinName", r.getCoinName());
        appendString(sb, "currency", r.getCurrency());
        appendDecimal(sb, "price", r.getPrice());
        appendDecimal(sb, "marketCap", r.getMarketCap());
        appendDecimal(sb, "volume24h", r.getVolume24h());
        appendString(sb, "coinImage", r.getCoinImage());
        appendLong(sb, "eventTimeMs", r.getEventTimeMs());
        appendLong(sb, "sourceTsMs", r.getSourceTsMs());
        appendLong(sb, "createdAtMs", r.getCreatedAtMs());
        appendLong(sb, "updatedAtMs", r.getUpdatedAtMs());
        appendString(sb, "op", r.getOp());
        appendLong(sb, "lsn", r.getLsn());
        appendString(sb, "sourceDb", r.getSourceDb());
        appendString(sb, "sourceSchema", r.getSourceSchema());
        appendString(sb, "sourceTable", r.getSourceTable());
        appendString(sb, "ingestion_date", r.getIngestionDate());
        appendString(sb, "ingestion_hour", r.getIngestionHour());
        removeTrailingComma(sb);
        sb.append("}");
        return sb.toString();
    }
    // =========================================================
    // Market Caps
    // =========================================================
    public static String toJson(MarketCapBronze r) {
        StringBuilder sb = new StringBuilder(512);
        sb.append("{");
        appendString(sb, "assetId", r.getAssetId());
        appendString(sb, "symbol", r.getSymbol());
        appendString(sb, "coinName", r.getCoinName());
        appendString(sb, "currency", r.getCurrency());
        appendDecimal(sb, "marketCap", r.getMarketCap());
        appendDecimal(sb, "circulatingSupply", r.getCirculatingSupply());
        appendDecimal(sb, "totalSupply", r.getTotalSupply());
        appendDecimal(sb, "maxSupply", r.getMaxSupply());
        appendLong(sb, "eventTimeMs", r.getEventTimeMs());
        appendLong(sb, "sourceTsMs", r.getSourceTsMs());
        appendLong(sb, "createdAtMs", r.getCreatedAtMs());
        appendLong(sb, "updatedAtMs", r.getUpdatedAtMs());
        appendString(sb, "op", r.getOp());
        appendLong(sb, "lsn", r.getLsn());
        appendString(sb, "sourceDb", r.getSourceDb());
        appendString(sb, "sourceSchema", r.getSourceSchema());
        appendString(sb, "sourceTable", r.getSourceTable());
        appendString(sb, "ingestion_date", r.getIngestionDate());
        appendString(sb, "ingestion_hour", r.getIngestionHour());
        removeTrailingComma(sb);
        sb.append("}");
        return sb.toString();
    }
    // =========================================================
    // Helpers
    // =========================================================
    private static void appendString(StringBuilder sb, String field, CharSequence value) {
        if (value == null) {
            sb.append("\"").append(field).append("\":null,");
        } else {
            sb.append("\"").append(field).append("\":\"")
              .append(escape(value.toString()))
              .append("\",");
        }
    }
    private static void appendLong(StringBuilder sb, String field, Long value) {
        if (value == null) {
            sb.append("\"").append(field).append("\":null,");
        } else {
            sb.append("\"").append(field).append("\":").append(value).append(",");
        }
    }
    private static void appendDecimal(StringBuilder sb, String field, ByteBuffer buffer) {
        if (buffer == null) {
            sb.append("\"").append(field).append("\":null,");
        } else {
            BigDecimal decimal = DECIMAL_CONVERSION.fromBytes(buffer, DECIMAL_SCHEMA, DECIMAL);
            sb.append("\"").append(field).append("\":")
              .append(decimal.toPlainString())
              .append(",");
        }
    }
    private static void removeTrailingComma(StringBuilder sb) {
        int last = sb.length() - 1;
        if (sb.charAt(last) == ',') {
            sb.deleteCharAt(last);
        }
    }
    private static String escape(String s) {
        return s.replace("\"", "\\\"");
    }
}