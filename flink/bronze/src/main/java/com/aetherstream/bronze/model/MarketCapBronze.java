package com.aetherstream.bronze.model;
import com.fasterxml.jackson.annotation.JsonProperty;
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
    @JsonProperty("ingestion_date")
    public String ingestionDate;

    @JsonProperty("ingestion_hour")
    public String ingestionHour;

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
