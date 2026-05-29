package com.aetherstream.bronze.sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Buffers JSONEachRow lines and flushes them to ClickHouse as a single batched
 * INSERT on batch-size threshold, on checkpoint, and on close. Failed flushes
 * are retried with exponential backoff; if all retries are exhausted the
 * exception propagates so Flink restarts the job (uncommitted offsets are
 * replayed, giving at-least-once delivery).
 */
public class ClickHouseHttpSink extends RichSinkFunction<String> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseHttpSink.class);
    private static final int DEFAULT_BATCH_SIZE = 500;
    private static final int DEFAULT_MAX_RETRIES = 5;
    private static final long INITIAL_BACKOFF_MS = 500L;

    private final String baseUrl;
    private final String insertQuery;
    private final String username;
    private final String password;
    private final int batchSize;
    private final int maxRetries;

    private transient CloseableHttpClient client;
    private transient List<String> buffer;

    public ClickHouseHttpSink(String baseUrl, String insertQuery, String username, String password) {
        this(baseUrl, insertQuery, username, password, DEFAULT_BATCH_SIZE, DEFAULT_MAX_RETRIES);
    }

    public ClickHouseHttpSink(String baseUrl, String insertQuery, String username, String password,
                              int batchSize, int maxRetries) {
        this.baseUrl = baseUrl;
        this.insertQuery = insertQuery;
        this.username = username;
        this.password = password;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
    }

    @Override
    public void open(Configuration parameters) {
        final BasicCredentialsProvider creds = new BasicCredentialsProvider();
        creds.setCredentials(
                new AuthScope(null, -1),
                new UsernamePasswordCredentials(username, password.toCharArray())
        );
        this.client = HttpClients.custom()
                .setDefaultCredentialsProvider(creds)
                .build();
        this.buffer = new ArrayList<>(batchSize);
    }

    @Override
    public void invoke(String jsonEachRow, Context context) throws Exception {
        buffer.add(jsonEachRow);
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Drain the buffer before the checkpoint completes so no in-flight rows
        // need to be persisted in Flink state.
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // Buffer is flushed on every checkpoint, so there is nothing to restore.
    }

    private void flush() throws Exception {
        if (buffer.isEmpty()) {
            return;
        }
        final String payload = String.join("\n", buffer);
        final String url = baseUrl + "/?query=" + URLEncoder.encode(insertQuery, StandardCharsets.UTF_8);
        final HttpClientResponseHandler<Void> handler = response -> {
            final int code = response.getCode();
            if (code < 200 || code >= 300) {
                throw new RuntimeException("ClickHouse HTTP insert failed: status=" + code);
            }
            return null;
        };

        long backoff = INITIAL_BACKOFF_MS;
        for (int attempt = 1; ; attempt++) {
            try {
                final HttpPost post = new HttpPost(url);
                post.setEntity(new StringEntity(payload, ContentType.APPLICATION_JSON));
                client.execute(post, handler);
                buffer.clear();
                return;
            } catch (Exception e) {
                if (attempt >= maxRetries) {
                    LOG.error("ClickHouse flush failed after {} attempts ({} rows); failing the task",
                            attempt, buffer.size(), e);
                    throw e;
                }
                LOG.warn("ClickHouse flush attempt {} failed ({} rows); retrying in {}ms",
                        attempt, buffer.size(), backoff, e);
                Thread.sleep(backoff);
                backoff *= 2;
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            flush();
        } finally {
            if (client != null) client.close();
        }
        super.close();
    }
}
