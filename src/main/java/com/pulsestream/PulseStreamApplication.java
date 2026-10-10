package com.pulsestream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * PulseStream — Real-Time User Activity & Analytics Platform.
 *
 * <p>Entry point for the Spring Boot application. Uses a modular-monolith structure
 * with clear package boundaries. Each package boundary represents a potential
 * split point if independent scaling becomes necessary in the future.
 *
 * <p>See docs/ARCHITECTURE_DECISIONS.md for detailed reasoning behind every major
 * technology and design choice.
 */
@SpringBootApplication
public class PulseStreamApplication {

    public static void main(String[] args) {
        SpringApplication.run(PulseStreamApplication.class, args);
    }
}
