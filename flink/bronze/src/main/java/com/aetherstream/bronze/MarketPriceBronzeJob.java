package com.aetherstream.bronze;

import com.aetherstream.bronze.model.MarketPriceBronze;
import com.aetherstream.bronze.util.DebeziumParser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.lang.String;

public class MarketPriceBronzeJob {
    private static final String TOPIC = "aether_pg.public.market_prices";
    private static final String GROUP_ID = "flink-bronze-market-prices";
    private static final ZoneId JAKARTA = ZoneId.of("Asia/Jakarta");
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ===== checkpointing (required for exactly-once + file finalization)
        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(15_000);
        env.getCheckpointConfig().setCheckpointTimeout(120_000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointStorage("s3a://bronze/_checkpoints/market_prices");

        // ===== kafka source
        KafkaSource<String> source =
            KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setProperty("partition.discovery.interval.ms", "-1")
                .setTopics(TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        // ===== transform Debezium event -> Bronze record (adds Jakarta partition columns)
        DataStream<MarketPriceBronze> bronze =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-cdc")
                .map(DebeziumParser::parse)
                .filter(e -> e != null)
                .map(out -> {
                    ZonedDateTime now = ZonedDateTime.now(JAKARTA);
                    out.ingestionDate = now.toLocalDate().toString();          // yyyy-MM-dd
                    out.ingestionHour = String.format("%02d", now.getHour());  // HH
                    return out;
                });

        // ===== debug print (optional)
        bronze.map(MarketPriceBronze::toJson).print();

        // ===== PARQUET sink (bulk format)
        StreamingFileSink<MarketPriceBronze> parquetSink =
            StreamingFileSink
                .forBulkFormat(
                    new Path("s3a://bronze/market_prices"),
                    ParquetAvroWriters.forReflectRecord(MarketPriceBronze.class)
                )
                .withBucketAssigner(
                    new DateTimeBucketAssigner<>("'ingestion_date='yyyy-MM-dd/'hour='HH", JAKARTA)
                )
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(
                    OutputFileConfig.builder()
                        .withPartPrefix("part")
                        .withPartSuffix(".parquet")
                        .build()
                )
                .build();
        bronze.addSink(parquetSink).name("minio-parquet-sink");
        env.execute("AetherStream Bronze: Market Prices");
    }
}
