package com.pulsestream.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Configures SpringDoc OpenAPI (Swagger UI).
 *
 * <p>Swagger UI is available at /swagger-ui.html in development.
 * The machine-readable spec is at /v3/api-docs.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI pulseStreamOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("PulseStream API")
                        .description("""
                                Real-Time User Activity & Analytics Platform.
                                
                                PulseStream receives user activity events, streams them through Apache Kafka,
                                aggregates sessions via Kafka Streams, and exposes analytics through REST APIs
                                backed by Elasticsearch.
                                
                                **Base path:** /api/v1
                                """)
                        .version("0.1.0")
                        .license(new License()
                                .name("MIT License (original source code only)")
                                .url("https://opensource.org/licenses/MIT"))
                        .contact(new Contact()
                                .name("PulseStream")
                                .url("https://github.com/your-username/pulsestream")))
                .servers(List.of(
                        new Server().url("http://localhost:8080").description("Local development")
                ));
    }
}
