package com.aetherstream.bronze;
import com.aetherstream.bronze.model.MarketCapBronze;
import com.aetherstream.bronze.util.DebeziumParser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
public class MarketCapBronzeJob {
    private static final String TOPIC = "aether_pg.public.market_caps";
    private static final String GROUP_ID = "flink-bronze-market-caps";
    private static final ZoneId JAKARTA = ZoneId.of("Asia/Jakarta");
    private static final BigDecimal UNKNOWN_MAX_SUPPLY_SENTINEL = new BigDecimal("-1");
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // ===== checkpointing
        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(15_000);
        env.getCheckpointConfig().setCheckpointTimeout(120_000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // ===== kafka CDC source
        final KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers("kafka:9092")
                        .setTopics(TOPIC)
                        .setGroupId(GROUP_ID)
                        .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();
        // ===== transform Debezium -> Bronze
        final DataStream<MarketCapBronze> bronze =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-cdc")
                        .map(DebeziumParser::parseMarketCap)
                        .filter(e -> e != null)
                        .map(out -> {
                            final ZonedDateTime now = ZonedDateTime.now(JAKARTA);
                            out.ingestionDate = now.toLocalDate().toString();
                            out.ingestionHour = String.format("%02d", now.getHour());
                            if (out.maxSupply == null) {
                                out.maxSupply = UNKNOWN_MAX_SUPPLY_SENTINEL;
                            }
                            return out;
                        })
                        .name("parse-debezium-and-enrich");
        // ===== debug
        bronze.map(MarketCapBronze::toJson).name("debug-json").print();
        // ===== Parquet sink
        final StreamingFileSink<MarketCapBronze> parquetSink =
                StreamingFileSink
                        .forBulkFormat(
                                new Path("s3a://bronze/market_caps"),
                                ParquetAvroWriters.forReflectRecord(MarketCapBronze.class)
                        )
                        .withBucketAssigner(
                                new DateTimeBucketAssigner<>(
                                        "'ingestion_date='yyyy-MM-dd/'hour='HH",
                                        JAKARTA
                                )
                        )
                        .withRollingPolicy(OnCheckpointRollingPolicy.build())
                        .withOutputFileConfig(
                                OutputFileConfig.builder()
                                        .withPartPrefix("part")
                                        .withPartSuffix(".parquet")
                                        .build()
                        )
                        .build();
        bronze.addSink(parquetSink).name("bronze-market-caps-parquet");
        env.execute("AetherStream Bronze: market_caps");
    }
}