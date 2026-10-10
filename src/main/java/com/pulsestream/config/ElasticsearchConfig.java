package com.pulsestream.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures the Elasticsearch Java client.
 *
 * <p>We use the official Elasticsearch Java client (co.elastic.clients) rather than
 * Spring Data Elasticsearch. Reason: the official client gives us full control over
 * index mappings, queries, and aggregations without Spring Data's abstraction overhead.
 * For an analytics platform, we need to write complex aggregation queries that would
 * fight against Spring Data's repository pattern.
 *
 * <p>The client is configured via environment variables (ELASTICSEARCH_HOST/PORT/SCHEME)
 * so the same binary works in local Docker, CI, and production.
 */
@Configuration
public class ElasticsearchConfig {

    @Value("${pulsestream.elasticsearch.host:localhost}")
    private String host;

    @Value("${pulsestream.elasticsearch.port:9200}")
    private int port;

    @Value("${pulsestream.elasticsearch.scheme:http}")
    private String scheme;

    /**
     * Low-level REST client — manages HTTP connections to the Elasticsearch cluster.
     * Created as a bean so Spring manages its lifecycle (closes connections on shutdown).
     */
    @Bean
    public RestClient elasticsearchRestClient() {
        return RestClient.builder(new HttpHost(host, port, scheme)).build();
    }

    /**
     * High-level typed Elasticsearch client — used throughout the application for
     * index operations, document indexing, and search queries.
     */
    @Bean
    public ElasticsearchClient elasticsearchClient(RestClient restClient) {
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }
}
