package com.aetherstream.bronze;
import com.aetherstream.bronze.model.MarketCapBronze;
import com.aetherstream.bronze.util.DebeziumParser;
import com.aetherstream.bronze.util.ClickHouseJsonUtil;
import com.aetherstream.bronze.util.DeadLetterUtil;
import com.aetherstream.bronze.sink.ClickHouseHttpSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
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
public class MarketCapBronzeJob {
    private static final String TOPIC = "aether_pg.public.market_caps";
    private static final String GROUP_ID = "flink-bronze-market-caps";
    private static final String BRONZE_AVRO_TOPIC = "bronze.market_caps.avro";
    private static final String SCHEMA_REGISTRY_URL = "http://schema-registry:8081";
    private static final String SR_SUBJECT_MARKET_CAPS = BRONZE_AVRO_TOPIC + "-value";
    private static final String DLQ_TOPIC = "bronze.market_caps.dlq";
    private static final OutputTag<String> DLQ_TAG = new OutputTag<String>("dead-letter") {};
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
        // ===== transform Debezium -> Bronze (rejects routed to a side output)
        final SingleOutputStreamOperator<MarketCapBronze> parsed =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-cdc")
                        .process(new ProcessFunction<GenericRecord, MarketCapBronze>() {
                            @Override
                            public void processElement(GenericRecord record, Context ctx,
                                                       Collector<MarketCapBronze> out) {
                                try {
                                    MarketCapBronze parsed = DebeziumParser.parseMarketCap(record);
                                    if (parsed == null) {
                                        ctx.output(DLQ_TAG, DeadLetterUtil.format(
                                                "null or unsupported record", record));
                                    } else {
                                        out.collect(parsed);
                                    }
                                } catch (Exception e) {
                                    ctx.output(DLQ_TAG, DeadLetterUtil.format(e.toString(), record));
                                }
                            }
                        })
                        .returns(MarketCapBronze.class)
                        .name("parse-debezium");

        final DataStream<MarketCapBronze> bronze =
                parsed.map(out -> {
                            final ZonedDateTime now = ZonedDateTime.now(JAKARTA);
                            out.setIngestionDate(now.toLocalDate().toString());
                            out.setIngestionHour(String.format("%02d", now.getHour()));
                            return out;
                        })
                        .name("enrich");

        // ===== dead-letter sink (rejected records)
        final KafkaSink<String> dlqSink =
                KafkaSink.<String>builder()
                        .setBootstrapServers("kafka:9092")
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.<String>builder()
                                        .setTopic(DLQ_TOPIC)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build()
                        )
                        .build();
        parsed.getSideOutput(DLQ_TAG).sinkTo(dlqSink).name("dead-letter-kafka");
        // ===== Kafka sink (Avro + Schema Registry)
        final KafkaSink<MarketCapBronze> avroKafkaSink =
                KafkaSink.<MarketCapBronze>builder()
                        .setBootstrapServers("kafka:9092")
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(BRONZE_AVRO_TOPIC)
                                        .setValueSerializationSchema(
                                                ConfluentRegistryAvroSerializationSchema.forSpecific(
                                                        MarketCapBronze.class,
                                                        SR_SUBJECT_MARKET_CAPS,
                                                        SCHEMA_REGISTRY_URL
                                                )
                                        )
                                        .build()
                        )
                        .build();
        bronze.sinkTo(avroKafkaSink).name("bronze-market-caps-kafka-avro-sr");
        // ===== Parquet sink
        final StreamingFileSink<MarketCapBronze> parquetSink =
                StreamingFileSink
                        .forBulkFormat(
                                new Path("s3a://bronze/market_caps"),
                                ParquetAvroWriters.forSpecificRecord(MarketCapBronze.class)
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
        // ===== ClickHouse sink
        bronze
                .map(ClickHouseJsonUtil::toJson)
                .addSink(new ClickHouseHttpSink(
                        "http://clickhouse:8123",
                        "INSERT INTO bronze.market_caps FORMAT JSONEachRow",
                        System.getenv().getOrDefault("CLICKHOUSE_USER", "aether"),
                        System.getenv().getOrDefault("CLICKHOUSE_PASSWORD", "aether")
                ))
                .name("bronze-market-caps-clickhouse");
        env.execute("AetherStream Bronze: market_caps");
    }
}