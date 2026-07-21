package com.opusdns.keycloak.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Field;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.Config.Scope;

class KafkaEventListenerProviderFactoryTests {

	private KafkaEventListenerProviderFactory factory;

	@BeforeEach
	void setUp() {
		// Factory.init() falls back to KAFKA_* env vars; a preconfigured environment would
		// change the outcomes below, so skip instead of failing spuriously.
		assumeTrue(System.getenv().keySet().stream().noneMatch(k -> k.startsWith("KAFKA_")),
				"KAFKA_* environment variables present; skipping env-sensitive factory tests");
		factory = new KafkaEventListenerProviderFactory();
	}

	@AfterEach
	void tearDown() {
		// init() creates a real KafkaProducer; close it so tests don't leak producer threads.
		if (factory != null) {
			factory.close();
		}
	}

	private static Scope scopeOf(Map<String, String> props) {
		return new Scope() {
			@Override public String get(String key) { return props.get(key); }
			@Override public String get(String key, String defaultValue) {
				String v = props.get(key);
				return v != null ? v : defaultValue;
			}
			@Override public Integer getInt(String key, Integer d) { return d; }
			@Override public Long getLong(String key, Long d) { return d; }
			@Override public Boolean getBoolean(String key, Boolean d) { return d; }
			@Override public String[] getArray(String key) { return null; }
			@Override public Scope scope(String... path) { return this; }
			@Override public java.util.Set<String> getPropertyNames() { return props.keySet(); }
			@Override public Scope root() { return this; }
		};
	}

	@Test
	void shouldThrowWhenTopicEventsIsNull() {
		Scope scope = scopeOf(Map.of("clientId", "kc", "bootstrapServers", "localhost:9092"));
		assertThrows(IllegalStateException.class, () -> factory.init(scope));
	}

	@Test
	void shouldThrowWhenClientIdIsNull() {
		Scope scope = scopeOf(Map.of("topicEvents", "keycloak-events", "bootstrapServers", "localhost:9092"));
		assertThrows(IllegalStateException.class, () -> factory.init(scope));
	}

	@Test
	void shouldThrowWhenBootstrapServersIsNull() {
		Scope scope = scopeOf(Map.of("topicEvents", "keycloak-events", "clientId", "kc"));
		assertThrows(IllegalStateException.class, () -> factory.init(scope));
	}

	@Test
	void shouldDefaultToRegisterEventWhenEventsNotConfigured() throws Exception {
		Scope scope = scopeOf(Map.of(
				"topicEvents", "keycloak-events",
				"clientId", "kc",
				"bootstrapServers", "localhost:9092"
		));
		factory.init(scope);

		Field eventsField = KafkaEventListenerProviderFactory.class.getDeclaredField("events");
		eventsField.setAccessible(true);
		assertArrayEquals(new String[] { "REGISTER" }, (String[]) eventsField.get(factory));
	}

	@Test
	void shouldSplitCommaSeparatedEventTypes() throws Exception {
		Scope scope = scopeOf(Map.of(
				"topicEvents", "keycloak-events",
				"clientId", "kc",
				"bootstrapServers", "localhost:9092",
				"events", "REGISTER,LOGIN,LOGOUT"
		));
		factory.init(scope);

		Field eventsField = KafkaEventListenerProviderFactory.class.getDeclaredField("events");
		eventsField.setAccessible(true);
		assertArrayEquals(new String[] { "REGISTER", "LOGIN", "LOGOUT" }, (String[]) eventsField.get(factory));
	}

	@Test
	void shouldTrimWhitespaceAndSkipEmptyEntriesInEventTypes() throws Exception {
		Scope scope = scopeOf(Map.of(
				"topicEvents", "keycloak-events",
				"clientId", "kc",
				"bootstrapServers", "localhost:9092",
				"events", "REGISTER, LOGIN ,,LOGOUT"
		));
		factory.init(scope);

		Field eventsField = KafkaEventListenerProviderFactory.class.getDeclaredField("events");
		eventsField.setAccessible(true);
		assertArrayEquals(new String[] { "REGISTER", "LOGIN", "LOGOUT" }, (String[]) eventsField.get(factory));
	}

	@Test
	void shouldReturnSameInstanceOnMultipleCreate() {
		Scope scope = scopeOf(Map.of(
				"topicEvents", "keycloak-events",
				"clientId", "kc",
				"bootstrapServers", "localhost:9092"
		));
		factory.init(scope);

		var first = factory.create(null);
		var second = factory.create(null);
		assertSame(first, second);
	}
}
