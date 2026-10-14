/**
 * TCP Connector using Reactor Netty.
 * Copyright (C) 2026 the original author or authors.
 */

package by.ivanta.connector;

import static by.ivanta.connector.j8583.testutils.TestDataUtil.PAN;
import static by.ivanta.connector.j8583.testutils.TestDataUtil.createPurchaseRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import by.ivanta.connector.config.J8583TcpServerTestConfiguration;
import by.ivanta.connector.config.properties.J8583ConnectorProperties;
import by.ivanta.connector.j8583.J8583Client;
import by.ivanta.connector.j8583.constant.IsoField;
import by.ivanta.connector.j8583.server.J8583TcpServer;
import com.solab.iso8583.IsoMessage;
import com.solab.iso8583.MessageFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.tck.MeterRegistryAssert;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.backoff.BackOffExecution;
import reactor.netty.tcp.TcpServer;

@SpringBootTest
@DirtiesContext
@Import({J8583TcpServerTestConfiguration.class})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class J8583ConnectorApplicationAutoConnectTests {
    static final long INITIAL_INTERVAL_MS = 1_000;
    static final long MAX_INTERVAL_MS = 2_000;
    static final long MAX_ELAPSED_TIME_MS = 8_000;

    @Autowired TcpServer tcpServer;

    @Autowired MessageFactory<IsoMessage> messageFactory;

    @Autowired J8583Client client;

    @Autowired MeterRegistry meterRegistry;

    @Autowired J8583ConnectorProperties j8583ConnectorProperties;

    @Test
    @Order(1)
    void shouldPublishFinancialRequest() throws InterruptedException {
        client.publish(createPurchaseRequest(PAN, messageFactory)).subscribe();

        Thread.sleep(Duration.ofSeconds(j8583ConnectorProperties.retryCooldownSec()));
        var server = new J8583TcpServer(tcpServer, messageFactory);

        await().alias("server started").until(server::isStarted);
        await().alias("client connected").until(client::isConnected);

        var financialRequest = createPurchaseRequest(PAN, messageFactory);
        var expectedStan =
                financialRequest.getField(IsoField.SYSTEM_TRACE_AUDIT_NUMBER.getId()).toString();

        var actualResponse = client.publishAndBlock(financialRequest);

        assertThat(actualResponse).isNotNull();
        assertThat(actualResponse.getField(IsoField.SYSTEM_TRACE_AUDIT_NUMBER.getId()).toString())
                .isEqualTo(expectedStan);

        await().alias("promises are empty")
                .untilAsserted(
                        () ->
                                MeterRegistryAssert.assertThat(meterRegistry)
                                        .gauge(
                                                "cache.size",
                                                Tag.of("name", "j8583-connector-promise-sinks"))
                                        .hasValue(0.0));
        await().alias("prepared for sending")
                .untilAsserted(
                        () ->
                                MeterRegistryAssert.assertThat(meterRegistry)
                                        .meter(
                                                "cache.puts",
                                                Tag.of("name", "j8583-connector-promise-sinks"))
                                        .hasMeasurement(Statistic.COUNT, 2.0));
        await().alias("incoming responses")
                .untilAsserted(
                        () ->
                                MeterRegistryAssert.assertThat(meterRegistry)
                                        .meter(
                                                "cache.gets",
                                                Tag.of("name", "j8583-connector-promise-sinks"),
                                                Tag.of("result", "miss"))
                                        .hasMeasurement(Statistic.COUNT, 2.0));
        await().alias("processed responses")
                .untilAsserted(
                        () ->
                                MeterRegistryAssert.assertThat(meterRegistry)
                                        .meter(
                                                "cache.gets",
                                                Tag.of("name", "j8583-connector-promise-sinks"),
                                                Tag.of("result", "hit"))
                                        .hasMeasurement(Statistic.COUNT, 1.0));

        MeterRegistryAssert.assertThat(meterRegistry)
                .counter("j8583.connector.timeout.exceeded")
                .hasCount(1);
        MeterRegistryAssert.assertThat(meterRegistry)
                .counter("j8583.connector.orphaned.response")
                .hasCount(0);

        server.shutdown();
    }

    @Test
    @Order(2)
    void shouldPublishFinancialRequestWhenRestApiMethodCall() throws InterruptedException {
        var tasksCount = 5;
        var executor = Executors.newFixedThreadPool(tasksCount);
        var serverRef = new AtomicReference<J8583TcpServer>();

        for (var i = 0; i < tasksCount; i++) {
            switch (i) {
                case 4 -> {
                    var _ =
                            executor.submit(
                                    () -> {
                                        try {
                                            Thread.sleep(
                                                    Duration.ofSeconds(
                                                            j8583ConnectorProperties
                                                                    .retryCooldownSec()));
                                            serverRef.set(
                                                    new J8583TcpServer(tcpServer, messageFactory));
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                        }
                                    });
                }
                default -> {
                    var _ =
                            executor.submit(
                                    () -> {
                                        restApiMethodWithBusinessContext(PAN);
                                    });
                }
            }
        }

        await().alias("server started")
                .untilAsserted(() -> assertThat(serverRef.get()).isNotNull());
        await().alias("client connected").until(client::isConnected);

        await().alias("promises are empty")
                .untilAsserted(
                        () ->
                                MeterRegistryAssert.assertThat(meterRegistry)
                                        .gauge(
                                                "cache.size",
                                                Tag.of("name", "j8583-connector-promise-sinks"))
                                        .hasValue(0.0));
        await().alias("prepared for sending")
                .untilAsserted(
                        () ->
                                MeterRegistryAssert.assertThat(meterRegistry)
                                        .meter(
                                                "cache.puts",
                                                Tag.of("name", "j8583-connector-promise-sinks"))
                                        .hasMeasurement(Statistic.COUNT, 6.0));
        await().alias("incoming responses")
                .untilAsserted(
                        () ->
                                MeterRegistryAssert.assertThat(meterRegistry)
                                        .meter(
                                                "cache.gets",
                                                Tag.of("name", "j8583-connector-promise-sinks"),
                                                Tag.of("result", "miss"))
                                        .hasMeasurement(Statistic.COUNT, 6.0));
        await().alias("processed responses")
                .untilAsserted(
                        () ->
                                MeterRegistryAssert.assertThat(meterRegistry)
                                        .meter(
                                                "cache.gets",
                                                Tag.of("name", "j8583-connector-promise-sinks"),
                                                Tag.of("result", "hit"))
                                        .hasMeasurement(Statistic.COUNT, 5.0));

        MeterRegistryAssert.assertThat(meterRegistry)
                .counter("j8583.connector.timeout.exceeded")
                .hasCount(1);
        MeterRegistryAssert.assertThat(meterRegistry)
                .counter("j8583.connector.orphaned.response")
                .hasCount(0);

        serverRef.get().shutdown();
    }

    private IsoMessage restApiMethodWithBusinessContext(String pan) {
        var execution =
                new ExponentialBackOffExecution(
                        INITIAL_INTERVAL_MS, MAX_INTERVAL_MS, MAX_ELAPSED_TIME_MS);
        var connected = false;
        var waitInterval = 0L;
        do {
            connected = client.isConnected();
            waitInterval = execution.nextBackOff();
            if (!connected && waitInterval != BackOffExecution.STOP) {
                try {
                    Thread.sleep(waitInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } while (!connected && waitInterval != BackOffExecution.STOP);
        if (connected) {
            return client.publishAndBlock(createPurchaseRequest(pan, messageFactory));
        }
        throw new RuntimeException("Timeout has been reached");
    }

    static class ExponentialBackOffExecution implements BackOffExecution {
        private static final double DEFAULT_RANDOMIZATION_FACTOR = 0.5;

        private long currentInterval = -1;
        private long currentElapsedTime = 0;
        private final long initialInterval;
        private final long maxInterval;
        private final long maxElapsedTime;

        ExponentialBackOffExecution(long initialInterval, long maxInterval, long maxElapsedTime) {
            super();
            this.initialInterval = initialInterval;
            this.maxInterval = maxInterval;
            this.maxElapsedTime = maxElapsedTime;
        }

        @Override
        public long nextBackOff() {
            if (this.currentElapsedTime >= getMaxElapsedTime()) {
                return STOP;
            }
            final var nextInterval = computeNextInterval();
            this.currentElapsedTime += nextInterval;
            return nextInterval;
        }

        private long computeNextInterval() {
            final var maxInterval = getMaxInterval();
            if (this.currentInterval < 0) {
                this.currentInterval = getInitialInterval();
            } else {
                this.currentInterval = multiplyInterval(DEFAULT_RANDOMIZATION_FACTOR, maxInterval);
            }
            return this.currentInterval;
        }

        public long getInitialInterval() {
            return initialInterval;
        }

        public long getMaxInterval() {
            return maxInterval;
        }

        public long getMaxElapsedTime() {
            return maxElapsedTime;
        }

        @Override
        public String toString() {
            final var sb = new StringBuilder("ExponentialBackOff{");
            sb.append("currentInterval=")
                    .append(this.currentInterval < 0 ? "n/a" : this.currentInterval + "ms");
            sb.append('}');
            return sb.toString();
        }

        public static long multiplyInterval(double randomizationFactor, long maxInterval) {
            final var delta = randomizationFactor * maxInterval;
            final var min = maxInterval - delta;
            final var max = maxInterval + delta;
            final var randomizedValue =
                    min + (ThreadLocalRandom.current().nextDouble() * (max - min + 1));
            return (long) Math.max(100.0, randomizedValue);
        }
    }
}
