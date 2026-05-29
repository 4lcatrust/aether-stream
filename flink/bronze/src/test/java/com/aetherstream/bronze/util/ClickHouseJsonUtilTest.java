package com.aetherstream.bronze.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aetherstream.bronze.model.MarketPriceBronze;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class ClickHouseJsonUtilTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static MarketPriceBronze sample() {
        MarketPriceBronze r = new MarketPriceBronze();
        r.setAssetId("bitcoin");
        r.setSymbol("btc");
        r.setCoinName("Bitcoin");
        r.setCurrency("usd");
        r.setEventTimeMs(1000L);
        r.setOp("c");
        r.setLsn(42L);
        return r;
    }

    @Test
    void producesParseableJson() throws Exception {
        JsonNode n = MAPPER.readTree(ClickHouseJsonUtil.toJson(sample()));
        assertEquals("bitcoin", n.get("assetId").asText());
        assertEquals("usd", n.get("currency").asText());
        assertEquals(1000L, n.get("eventTimeMs").asLong());
        assertEquals(42L, n.get("lsn").asLong());
    }

    @Test
    void nullDecimalRendersAsJsonNull() throws Exception {
        MarketPriceBronze r = sample();
        r.setMarketCap(null);
        JsonNode n = MAPPER.readTree(ClickHouseJsonUtil.toJson(r));
        assertTrue(n.get("marketCap").isNull());
    }

    @Test
    void decimalRendersAsNumericValue() throws Exception {
        MarketPriceBronze r = sample();
        r.setPrice(AvroDecimalUtil.toBytes(new BigDecimal("123.45")));
        JsonNode n = MAPPER.readTree(ClickHouseJsonUtil.toJson(r));
        assertEquals(0, new BigDecimal("123.45").compareTo(n.get("price").decimalValue()));
    }

    @Test
    void escapesEmbeddedQuotes() throws Exception {
        MarketPriceBronze r = sample();
        r.setCoinName("a\"quoted\"name");
        JsonNode n = MAPPER.readTree(ClickHouseJsonUtil.toJson(r)); // must stay valid JSON
        assertEquals("a\"quoted\"name", n.get("coinName").asText());
    }
}
