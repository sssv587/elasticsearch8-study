package com.fbd.api;

import co.elastic.clients.elasticsearch.*;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.*;
import org.apache.http.client.*;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.*;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.*;
import org.elasticsearch.client.*;

import javax.net.ssl.SSLContext;
import java.io.InputStream;
import java.nio.file.*;
import java.security.KeyStore;
import java.security.cert.*;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2024/1/25 - 0:33
 * @Description 构建ES客户端
 */
public class ESClient {
    public static void main(String[] args) {
        // 初始化ES客户端
    }

    private static void initESConnection() throws Exception {
        //获取客户端对象
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "O3x0hfu7i=ZbQvlktCnd"));

        Path caCertificatePath = Paths.get("certs/ca.crt");
        CertificateFactory factory = CertificateFactory.getInstance("X.509");

        Certificate trustedCa;

        try (InputStream is = Files.newInputStream(caCertificatePath)) {
            trustedCa = factory.generateCertificate(is);
        }

        KeyStore trustStore = KeyStore.getInstance("pkcs12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", trustedCa);
        SSLContextBuilder sslContextBuilder = SSLContexts.custom().loadTrustMaterial(trustStore, null);
        final SSLContext sslContext = sslContextBuilder.build();

        RestClientBuilder builder = RestClient.builder(
                        new HttpHost("linux1", 9200, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setSSLContext(sslContext)
                                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                .setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestClient restClient = builder.build();

        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // 同步客户端对象，需要关闭
        ElasticsearchClient client = new ElasticsearchClient(transport);

        // 异步客户端对象，需要回调方法得到结果
        ElasticsearchAsyncClient asyncClient = new ElasticsearchAsyncClient(transport);

        transport.close();
    }
}
