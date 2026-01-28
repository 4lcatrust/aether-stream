package com.aetherstream.bronze.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.Serializable;
import java.time.Instant;

public class MarketPriceBronze implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    public String assetId;
    public String currency;
    public String price;
    public String marketCap;
    public String volume24h;
    public Long eventTimeMs;
    public Long sourceTsMs;
    public String op;
    public Long lsn;
    // ===== partition columns =====
    public String ingestionDate; // yyyy-MM-dd
    public String ingestionHour; // HH
    public MarketPriceBronze() {}
    public static Long toMillis(Instant ts) {
        return ts == null ? null : ts.toEpochMilli();
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize MarketPriceBronze to JSON", e);
        }
    }

    @Override
    public String toString() {
        return String.format(
            "MarketPriceBronze(assetId=%s, currency=%s, price=%s, marketCap=%s, volume24h=%s, eventTimeMs=%s, sourceTsMs=%s, op=%s, lsn=%s, ingestionDate=%s, ingestionHour=%s)",
            assetId,
            currency,
            price,
            marketCap,
            volume24h,
            eventTimeMs,
            sourceTsMs,
            op,
            lsn,
            ingestionDate,
            ingestionHour
        );
    }
}
