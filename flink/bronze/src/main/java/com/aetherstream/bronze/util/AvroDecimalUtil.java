package com.aetherstream.bronze.util;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
public final class AvroDecimalUtil {
    private static final LogicalTypes.Decimal DECIMAL =
            LogicalTypes.decimal(38, 18);
    private static final Schema SCHEMA =
            DECIMAL.addToSchema(Schema.create(Schema.Type.BYTES));
    private static final Conversions.DecimalConversion CONVERSION =
            new Conversions.DecimalConversion();
    private AvroDecimalUtil() {}
    public static ByteBuffer toBytes(BigDecimal value) {
        if (value == null) return null;
        return CONVERSION.toBytes(value, SCHEMA, DECIMAL);
    }
}