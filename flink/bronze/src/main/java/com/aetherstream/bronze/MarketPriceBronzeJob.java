package com.aetherstream.bronze;

import com.aetherstream.bronze.model.MarketPriceBronze;
import com.aetherstream.bronze.util.DebeziumParser;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import java.util.regex.Pattern;
import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.time.LocalDate;

public class MarketPriceBronzeJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        // ===== Checkpointing (EXACTLY ONCE)
        env.enableCheckpointing(60_000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointStorage(
            "s3a://bronze/_checkpoints/market_prices"
        );

        // ===== Kafka Source
        KafkaSource<String> source =
            KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("aether_pg.public.market_prices")
                .setGroupId("flink-bronze-market-prices")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                    new org.apache.flink.api.common.serialization.SimpleStringSchema()
                )
                .build();

        DataStream<Row> bronze =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-cdc")
                .map(DebeziumParser::parse)
                .filter(e -> e != null)
                .map(e -> Row.of(
                    e.assetId,
                    e.currency,
                    e.price == null ? null : new BigDecimal(e.price),
                    e.marketCap == null ? null : new BigDecimal(e.marketCap),
                    e.volume24h == null ? null : new BigDecimal(e.volume24h),
                    e.eventTime,
                    e.sourceTs,
                    e.op,
                    e.lsn,
                    LocalDate.now(ZoneOffset.UTC).toString()   // ingest_date
                ));

        DataStream<String> bronzeOut =
            bronze.map(Row::toString);

        bronzeOut.print();

        FileSink<String> sink =
            FileSink.<String>forRowFormat(
                new Path("s3a://bronze/market_prices"),
                new SimpleStringEncoder<>("UTF-8")
            )
            .build();

        bronzeOut.sinkTo(sink);

        env.execute("AetherStream Bronze – Market Prices");
    }
}
