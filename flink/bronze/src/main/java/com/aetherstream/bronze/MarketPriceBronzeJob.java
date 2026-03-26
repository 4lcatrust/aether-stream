package com.aetherstream.bronze;
import com.aetherstream.bronze.model.MarketPriceBronze;
import com.aetherstream.bronze.util.DebeziumParser;
import com.aetherstream.bronze.util.ClickHouseJsonUtil;
import com.aetherstream.bronze.sink.ClickHouseHttpSink;
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
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.avro.generic.GenericRecord;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
public class MarketPriceBronzeJob {
    private static final String TOPIC = "aether_pg.public.market_prices";
    private static final String GROUP_ID = "flink-bronze-market-prices";
    private static final String BRONZE_AVRO_TOPIC = "bronze.market_prices.avro";
    private static final String SCHEMA_REGISTRY_URL = "http://schema-registry:8081";
    private static final String SR_SUBJECT_MARKET_PRICES = BRONZE_AVRO_TOPIC + "-value";
    private static final ZoneId JAKARTA = ZoneId.of("Asia/Jakarta");
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // ===== checkpointing
        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(15_000);
        env.getCheckpointConfig().setCheckpointTimeout(120_000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // ===== kafka CDC source
        SchemaRegistryClient srClient = new CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL, 100);
        Schema readerSchema = new Schema.Parser().parse(
                srClient.getLatestSchemaMetadata(TOPIC + "-value").getSchema()
        );
        final KafkaSource<GenericRecord> source =
                KafkaSource.<GenericRecord>builder()
                        .setBootstrapServers("kafka:9092")
                        .setTopics(TOPIC)
                        .setGroupId(GROUP_ID)
                        .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .setValueOnlyDeserializer(
                                ConfluentRegistryAvroDeserializationSchema.forGeneric(
                                        readerSchema,
                                        SCHEMA_REGISTRY_URL
                                )
                        )
                        .build();
        // ===== transform Debezium -> Bronze
        final DataStream<MarketPriceBronze> bronze =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-cdc")
                        .map(DebeziumParser::parseMarketPrice)
                        .filter(e -> e != null)
                        .map(out -> {
                            final ZonedDateTime now = ZonedDateTime.now(JAKARTA);
                            out.setIngestionDate(now.toLocalDate().toString());
                            out.setIngestionHour(String.format("%02d", now.getHour()));
                            return out;
                        })
                        .name("parse-debezium-and-enrich");
        // ===== debug
        bronze.map(ClickHouseJsonUtil::toJson).name("debug-json").print();
        // ===== Kafka sink (Avro + Schema Registry)
        final KafkaSink<MarketPriceBronze> avroKafkaSink =
                KafkaSink.<MarketPriceBronze>builder()
                        .setBootstrapServers("kafka:9092")
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(BRONZE_AVRO_TOPIC)
                                        .setValueSerializationSchema(
                                                ConfluentRegistryAvroSerializationSchema.forSpecific(
                                                        MarketPriceBronze.class,
                                                        SR_SUBJECT_MARKET_PRICES,
                                                        SCHEMA_REGISTRY_URL
                                                )
                                        )
                                        .build()
                        )
                        .build();
        bronze.sinkTo(avroKafkaSink).name("bronze-market-prices-kafka-avro-sr");
        // ===== Parquet sink
        final StreamingFileSink<MarketPriceBronze> parquetSink =
                StreamingFileSink
                        .forBulkFormat(
                                new Path("s3a://bronze/market_prices"),
                                ParquetAvroWriters.forSpecificRecord(MarketPriceBronze.class)
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
        bronze.addSink(parquetSink).name("bronze-market-prices-parquet");
        // ===== ClickHouse sink
        bronze
                .map(ClickHouseJsonUtil::toJson)
                .addSink(new ClickHouseHttpSink(
                        "http://clickhouse:8123",
                        "INSERT INTO bronze.market_prices FORMAT JSONEachRow",
                        "aether",
                        "aether"
                ))
                .name("bronze-market-prices-clickhouse");
        env.execute("AetherStream Bronze: market_prices");
    }
}