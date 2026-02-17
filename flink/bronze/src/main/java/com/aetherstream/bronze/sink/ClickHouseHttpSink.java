package com.aetherstream.bronze.sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.StringEntity;
public class ClickHouseHttpSink extends RichSinkFunction<String> {
    private final String baseUrl;
    private final String insertQuery;
    private final String username;
    private final String password;
    private transient CloseableHttpClient client;
    public ClickHouseHttpSink(String baseUrl, String insertQuery, String username, String password) {
        this.baseUrl = baseUrl;
        this.insertQuery = insertQuery;
        this.username = username;
        this.password = password;
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
    }
    @Override
    public void invoke(String jsonEachRow, Context context) throws Exception {
        final String q = URLEncoder.encode(insertQuery, StandardCharsets.UTF_8);
        final String url = baseUrl + "/?query=" + q;
        final HttpPost post = new HttpPost(url);
        post.setEntity(new StringEntity(jsonEachRow, ContentType.APPLICATION_JSON));
        final HttpClientResponseHandler<Void> handler = response -> {
            final int code = response.getCode();
            if (code < 200 || code >= 300) {
                throw new RuntimeException("ClickHouse HTTP insert failed: status=" + code);
            }
            return null;
        };
        client.execute(post, handler);
    }
    @Override
    public void close() throws Exception {
        if (client != null) client.close();
        super.close();
    }
}