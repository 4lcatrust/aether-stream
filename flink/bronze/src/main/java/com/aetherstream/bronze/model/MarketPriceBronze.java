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
    public Instant eventTime;
    public Instant sourceTs;
    public String op;
    public Long lsn;

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
            "MarketPriceBronze(assetId=%s, currency=%s, price=%s, marketCap=%s, volume24h=%s, eventTime=%s, sourceTs=%s, op=%s, lsn=%d)",
            assetId,
            currency,
            price,
            marketCap,
            volume24h,
            eventTime,
            sourceTs,
            op,
            lsn
        );
    }
}
