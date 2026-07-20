package com.github.discostur.keycloak.kafka;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class KafkaEventListenerProvider implements EventListenerProvider {

	private static final Logger LOG = Logger.getLogger(KafkaEventListenerProvider.class);

	// Keycloak model classes evolve between versions; an "empty bean" somewhere in the
	// event graph must not make serialization (and thus event delivery) fail.
	private static final ObjectMapper MAPPER = new ObjectMapper()
			.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

	private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);

	private final String topicEvents;

	private final Set<EventType> events;

	private final String topicAdminEvents;

	private final Producer<String, String> producer;

	public KafkaEventListenerProvider(String bootstrapServers, String clientId, String topicEvents, String[] events,
			String topicAdminEvents, Map<String, Object> kafkaProducerProperties, KafkaProducerFactory factory) {
		this.topicEvents = topicEvents;
		this.events = EnumSet.noneOf(EventType.class);
		this.topicAdminEvents = topicAdminEvents;

		for (String event : events) {
			try {
				EventType eventType = EventType.valueOf(event.toUpperCase());
				this.events.add(eventType);
			} catch (IllegalArgumentException e) {
				LOG.warnf("Ignoring unknown event type '%s'. Check the configured events / KAFKA_EVENTS list.", event);
			}
		}

		if (this.events.isEmpty()) {
			LOG.warn("No valid event types configured; no user events will be produced to Kafka.");
		}

		producer = factory.createProducer(clientId, bootstrapServers, kafkaProducerProperties);
	}

	private void produceEvent(String eventAsString, String topic) {
		LOG.debugf("Produce to topic: %s ...", topic);
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, eventAsString);
		try {
			producer.send(record, (metadata, exception) -> {
				if (exception != null) {
					LOG.errorf(exception, "Failed to send event to Kafka topic %s", topic);
				} else {
					LOG.debugf("Produced to topic: %s", metadata.topic());
				}
			});
		} catch (RuntimeException e) {
			// send() can also fail synchronously (serialization, exhausted buffer, metadata
			// timeout). Event delivery is best-effort and must never break the Keycloak flow.
			LOG.errorf(e, "Failed to produce event to Kafka topic %s", topic);
		}
	}

	@Override
	public void onEvent(Event event) {
		if (event != null && events.contains(event.getType())) {
			try {
				produceEvent(MAPPER.writeValueAsString(event), topicEvents);
			} catch (JsonProcessingException e) {
				LOG.error("Failed to serialize event to JSON", e);
			}
		}
	}

	@Override
	public void onEvent(AdminEvent event, boolean includeRepresentation) {
		if (event != null && topicAdminEvents != null) {
			try {
				produceEvent(MAPPER.writeValueAsString(event), topicAdminEvents);
			} catch (JsonProcessingException e) {
				LOG.error("Failed to serialize admin event to JSON", e);
			}
		}
	}

	@Override
	public void close() {
		// Keycloak calls close() at the end of every session, but this provider is a
		// shared singleton owning a shared producer. The producer is released via
		// shutdown(), invoked from the factory's close() at server shutdown.
	}

	void shutdown() {
		if (producer != null) {
			// Bounded close: an unreachable broker must not block Keycloak's shutdown.
			producer.close(SHUTDOWN_TIMEOUT);
		}
	}
}
