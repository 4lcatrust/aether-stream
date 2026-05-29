package com.aetherstream.bronze.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

class AvroDecimalUtilTest {

    private static final LogicalTypes.Decimal DECIMAL = LogicalTypes.decimal(38, 18);
    private static final Schema SCHEMA = DECIMAL.addToSchema(Schema.create(Schema.Type.BYTES));
    private static final Conversions.DecimalConversion CONVERSION = new Conversions.DecimalConversion();

    private static BigDecimal decode(ByteBuffer buf) {
        return CONVERSION.fromBytes(buf, SCHEMA, DECIMAL);
    }

    @Test
    void nullInputReturnsNull() {
        assertNull(AvroDecimalUtil.toBytes(null));
    }

    @Test
    void roundTripsValue() {
        ByteBuffer buf = AvroDecimalUtil.toBytes(new BigDecimal("123.45"));
        assertEquals(0, new BigDecimal("123.45").compareTo(decode(buf)));
    }

    @Test
    void roundTripsHighPrecisionValue() {
        BigDecimal v = new BigDecimal("0.000000000000000001"); // 1e-18, the scale limit
        assertEquals(0, v.compareTo(decode(AvroDecimalUtil.toBytes(v))));
    }

    @Test
    void roundTripsNegativeValue() {
        BigDecimal v = new BigDecimal("-987654321.123456789");
        assertEquals(0, v.compareTo(decode(AvroDecimalUtil.toBytes(v))));
    }
}
