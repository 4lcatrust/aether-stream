package com.aetherstream.bronze.model;
/**
 * Lossless CDC Bronze model for public.market_caps.
 *
 * Guarantees:
 * - Mirrors ALL source columns
 * - Append-only CDC events
 * - BigDecimal for all numerics (precision-safe)
 * - No semantic transformation (Silver owns semantics)
 */
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

public class MarketCapBronze implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    // ===== source columns =====
    public String assetId;
    public String symbol;
    public String coinName;
    public String currency;
    public BigDecimal marketCap;
    public BigDecimal circulatingSupply;
    public BigDecimal totalSupply;
    public BigDecimal maxSupply;
    public Long eventTimeMs;
    public Long sourceTsMs;
    public Long createdAtMs;
    public Long updatedAtMs;
    // ===== CDC metadata =====
    public String op;     // c, u, d
    public Long lsn;
    // ===== source identity =====
    public String sourceDb;
    public String sourceSchema;
    public String sourceTable;
    // ===== ingestion partitions =====
    public String ingestionDate; // yyyy-MM-dd
    public String ingestionHour; // HH

    public MarketCapBronze() {}
    public static Long toMillis(Instant ts) {
        return ts == null ? null : ts.toEpochMilli();
    }
    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize MarketCapBronze", e);
        }
    }
}
