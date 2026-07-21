# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Keycloak event listener SPI module that produces Keycloak (admin) events to Kafka as JSON. Maintained fork of SnuK87/keycloak-kafka under the `com.opusdns.keycloak.kafka` namespace. The deliverable is a single fat jar dropped into Keycloak's `providers/` folder.

## Commands

```bash
# Build fat jar -> target/keycloak-kafka-<version>-jar-with-dependencies.jar
mvn clean package

# Unit tests only (*Tests, Surefire)
mvn test

# Single test class / method
mvn test -Dtest=KafkaEventListenerProviderTests
mvn test -Dtest=KafkaEventListenerProviderTests#<methodName>

# Unit + integration tests (*IT, Failsafe; needs Docker — the IT skips itself if no Docker daemon)
mvn verify

# Unit tests without local Maven (deps cached in named volume maven-cache)
docker compose --profile test run --rm test

# Opt-in static analysis (SpotBugs, not in default build)
mvn verify -Pquality

# Build against different Keycloak/Kafka versions
mvn clean package -Dkeycloak.version=19.0.3 -Dkafka.version=3.3.2
```

Releases: bump `<version>` in `pom.xml`, then `git tag vX.Y.Z && git push origin vX.Y.Z` — `.github/workflows/release.yml` builds and attaches the jar. Keep `keycloak.version` in `pom.xml` aligned with the Keycloak image in `Dockerfile`.

## Architecture

Small SPI plugin, ~5 main classes in `src/main/java/com/opusdns/keycloak/kafka/`:

- `KafkaEventListenerProviderFactory` — registered via `META-INF/services/org.keycloak.events.EventListenerProviderFactory` under id `kafka`. `init()` reads config once at startup (SPI params `--spi-events-listener-kafka-*` with `KAFKA_*` env vars as fallback via `config.get(name, System.getenv(...))`), fails fast with NPE on missing required config, and **eagerly** builds a single `KafkaEventListenerProvider` instance — `create()` just returns it. This eager-singleton pattern is deliberate (no lazy-init race); keep it.
- `KafkaEventListenerProvider` — holds the Kafka producer for the process lifetime. Serializes `Event`/`AdminEvent` to JSON with Jackson and sends **asynchronously** (fire-and-forget with a logging callback; `onEvent` must never block the Keycloak request thread). User events are filtered by configured `EventType` list (default `REGISTER`; unknown types logged and skipped); admin events go to `topicAdminEvents` only if that topic is set.
- `KafkaProducerConfig` — enum whitelist of Kafka producer properties passed through from SPI params/`KAFKA_*` env vars. To support a new producer setting, add an enum constant. Special case: `SSL_ENDPOINT_IDENTIFICATION_ALGORITHM` value `disabled` is translated to empty string (workaround for platforms that can't set empty env vars).
- `KafkaProducerFactory` (interface) / `KafkaStandardProducerFactory` — indirection so tests inject a `MockProducer` (`KafkaMockProducerFactory` in test sources).

Dependency scoping matters: `keycloak-*` and `jboss-logging` are `provided` (supplied by the Keycloak server, must not be bundled); `kafka-clients` is bundled in the fat jar (with `slf4j-api` excluded).

## Conventions

- Java sources use **tabs** for indentation (see `.editorconfig`).
- Bytecode target is Java 17 (`maven.compiler.release`) for broad Keycloak compatibility — don't use post-17 language features even when building on JDK 21.
- Use jboss-logging `Logger` with parameterized calls (`debugf`/`warnf`/`errorf`), not string concatenation.
- The public configuration contract is user-facing and must stay stable: `KAFKA_*` env vars and `--spi-events-listener-kafka-*` parameters (documented in README.md).
