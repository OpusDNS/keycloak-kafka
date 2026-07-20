package com.github.discostur.keycloak.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.keycloak.Config.SystemPropertiesConfigProvider;

class KafkaProducerConfigTests {

	@BeforeEach
	void setUp() {
		// init() falls back to KAFKA_* env vars, which would leak into the exact-match
		// assertions below; skip instead of failing on preconfigured machines.
		assumeTrue(System.getenv().keySet().stream().noneMatch(k -> k.startsWith("KAFKA_")),
				"KAFKA_* environment variables present; skipping env-sensitive config tests");
	}

	@AfterEach
	void cleanUp() {
		System.clearProperty("keycloak.retry.backoff.ms");
		System.clearProperty("keycloak.max.block.ms");
		System.clearProperty("keycloak.ssl.endpoint.identification.algorithm");
	}

	@Test
	void shouldReturnMapWithConfigWhenPropertyExists() {
		System.setProperty("keycloak.retry.backoff.ms", "1000");
		System.setProperty("keycloak.max.block.ms", "5000");
		System.setProperty("keycloak.foo", "bar");

		Map<String, Object> config = KafkaProducerConfig.init(new SystemPropertiesConfigProvider().scope());
		Map<String, Object> expected = Map.of("retry.backoff.ms", "1000", "max.block.ms", "5000");

		assertEquals(expected, config);
	}

	@Test
	void shouldSetEmptyStringForDisabledSSLEndpointIdentification() {
		System.setProperty("keycloak.ssl.endpoint.identification.algorithm", "disabled");

		Map<String, Object> config = KafkaProducerConfig.init(new SystemPropertiesConfigProvider().scope());

		assertEquals("", config.get("ssl.endpoint.identification.algorithm"));
	}
}
