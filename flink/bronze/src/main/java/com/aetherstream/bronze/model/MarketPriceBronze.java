package com.aetherstream.bronze.model;
import java.io.Serializable;
import java.time.Instant;
public class MarketPriceBronze implements Serializable {
    public String assetId;
    public String currency;
    public String price;
    public String marketCap;
    public String volume24h;
    public Instant eventTime;
    public Instant sourceTs;
    public String op;
    public Long lsn;
    @Override
    public String toString() {
        return String.format(
            "%s,%s,%s,%s,%s,%s,%s,%s,%d",
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
