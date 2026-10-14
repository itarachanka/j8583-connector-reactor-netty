/**
 * TCP Connector using Reactor Netty.
 * Copyright (C) 2026 the original author or authors.
 */

package by.ivanta.connector;

import static by.ivanta.connector.j8583.testutils.TestDataUtil.PAN;
import static by.ivanta.connector.j8583.testutils.TestDataUtil.PAN_WITH_BROKEN_RESPONSE;
import static by.ivanta.connector.j8583.testutils.TestDataUtil.PAN_WITH_IDLE_TIMEOUT;
import static by.ivanta.connector.j8583.testutils.TestDataUtil.PAN_WITH_NOT_EXISTS_STAN;
import static by.ivanta.connector.j8583.testutils.TestDataUtil.PAN_WITH_TIMEOUT;
import static by.ivanta.connector.j8583.testutils.TestDataUtil.createBrokenPurchaseRequest;
import static by.ivanta.connector.j8583.testutils.TestDataUtil.createEchoTestRequest;
import static by.ivanta.connector.j8583.testutils.TestDataUtil.createPurchaseRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;

import by.ivanta.connector.config.J8583ConnectorTestConfiguration;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import reactor.test.StepVerifier;

@SpringBootTest
@DirtiesContext
@Import({J8583TcpServerTestConfiguration.class, J8583ConnectorTestConfiguration.class})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class J8583ConnectorApplicationTests {

    @Autowired J8583TcpServer server;

    @Autowired MessageFactory<IsoMessage> messageFactory;

    @Autowired J8583Client client;

    @Autowired MeterRegistry meterRegistry;

    @Autowired J8583ConnectorProperties j8583ConnectorProperties;

    @Test
    @Order(1)
    void shouldPublishFinancialRequest() {
        await().alias("server started").until(server::isStarted);
        await().alias("client connected").until(client::isConnected);

        StepVerifier.create(client.publish(createEchoTestRequest(messageFactory))).verifyComplete();

        client.publish(createPurchaseRequest(PAN, messageFactory)).subscribe();

        var exception =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                client.publishAndBlock(
                                        createPurchaseRequest(PAN_WITH_TIMEOUT, messageFactory)));

        client.publish(createPurchaseRequest(PAN_WITH_NOT_EXISTS_STAN, messageFactory)).subscribe();

        assertThat(exception).hasCauseExactlyInstanceOf(TimeoutException.class);

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
                                        .hasMeasurement(Statistic.COUNT, 3.0));
        await().alias("incoming responses")
                .untilAsserted(
                        () ->
                                MeterRegistryAssert.assertThat(meterRegistry)
                                        .meter(
                                                "cache.gets",
                                                Tag.of("name", "j8583-connector-promise-sinks"),
                                                Tag.of("result", "miss"))
                                        .hasMeasurement(Statistic.COUNT, 4.0));
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
                .hasCount(2);
        MeterRegistryAssert.assertThat(meterRegistry)
                .counter("j8583.connector.orphaned.response")
                .hasCount(1);
    }

    @Test
    @Order(2)
    void shouldPublishFinancialRequestWhenExceptionOccurred() {
        client.publish(createPurchaseRequest(PAN_WITH_BROKEN_RESPONSE, messageFactory)).subscribe();

        var brokenPurchase = createBrokenPurchaseRequest(PAN, messageFactory);
        client.publish(brokenPurchase).subscribe();
        client.publish(createPurchaseRequest(PAN, messageFactory)).subscribe();
        client.publish(createPurchaseRequest(PAN, messageFactory)).subscribe();

        var exception =
                assertThrows(RuntimeException.class, () -> client.publishAndBlock(brokenPurchase));
        var actualResponse = client.publishAndBlock(createPurchaseRequest(PAN, messageFactory));

        var encodeExPurchase = createPurchaseRequest(PAN, messageFactory);
        var localTimeField = IsoField.TIME_LOCAL_TRANSACTION;
        encodeExPurchase.setField(
                localTimeField.getId(),
                localTimeField.getIsoType().value(null, localTimeField.getLength()));
        client.publish(encodeExPurchase).subscribe();

        assertThat(exception).hasCauseExactlyInstanceOf(TimeoutException.class);
        assertThat(actualResponse).isNotNull();
        assertThat(actualResponse.getField(IsoField.SYSTEM_TRACE_AUDIT_NUMBER.getId()).toString())
                .isEqualTo("000011");

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
                                        .hasMeasurement(Statistic.COUNT, 9.0));
        await().alias("incoming responses")
                .untilAsserted(
                        () ->
                                MeterRegistryAssert.assertThat(meterRegistry)
                                        .meter(
                                                "cache.gets",
                                                Tag.of("name", "j8583-connector-promise-sinks"),
                                                Tag.of("result", "miss"))
                                        .hasMeasurement(Statistic.COUNT, 10.0));
        await().alias("processed responses")
                .untilAsserted(
                        () ->
                                MeterRegistryAssert.assertThat(meterRegistry)
                                        .meter(
                                                "cache.gets",
                                                Tag.of("name", "j8583-connector-promise-sinks"),
                                                Tag.of("result", "hit"))
                                        .hasMeasurement(Statistic.COUNT, 3.0));

        MeterRegistryAssert.assertThat(meterRegistry)
                .counter("j8583.connector.timeout.exceeded")
                .hasCount(8);
        MeterRegistryAssert.assertThat(meterRegistry)
                .counter("j8583.connector.orphaned.response")
                .hasCount(1);
    }

    @Test
    @Order(3)
    void shouldPublishFinancialRequestWhenIdleTimeoutOccurred() throws InterruptedException {
        Thread.sleep(
                Duration.ofSeconds(
                        j8583ConnectorProperties.writerIdleTimeoutSec()
                                + j8583ConnectorProperties.retryMaxBackoffSec()));

        client.publish(createPurchaseRequest(PAN_WITH_IDLE_TIMEOUT, messageFactory)).subscribe();

        Thread.sleep(
                Duration.ofSeconds(
                        j8583ConnectorProperties.readerIdleTimeoutSec()
                                + j8583ConnectorProperties.retryMaxBackoffSec()));
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
                                        .hasMeasurement(Statistic.COUNT, 11.0));
        await().alias("incoming responses")
                .untilAsserted(
                        () ->
                                MeterRegistryAssert.assertThat(meterRegistry)
                                        .meter(
                                                "cache.gets",
                                                Tag.of("name", "j8583-connector-promise-sinks"),
                                                Tag.of("result", "miss"))
                                        .hasMeasurement(Statistic.COUNT, 12.0));
        await().alias("processed responses")
                .untilAsserted(
                        () ->
                                MeterRegistryAssert.assertThat(meterRegistry)
                                        .meter(
                                                "cache.gets",
                                                Tag.of("name", "j8583-connector-promise-sinks"),
                                                Tag.of("result", "hit"))
                                        .hasMeasurement(Statistic.COUNT, 4.0));

        MeterRegistryAssert.assertThat(meterRegistry)
                .counter("j8583.connector.timeout.exceeded")
                .hasCount(9);
        MeterRegistryAssert.assertThat(meterRegistry)
                .counter("j8583.connector.orphaned.response")
                .hasCount(1);
    }

    @Test
    @Order(4)
    void shouldPublishFinancialRequestParallel() throws InterruptedException {
        var tasksCount = 200;
        var latch = new CountDownLatch(tasksCount);
        var executor = Executors.newFixedThreadPool(tasksCount);

        for (var i = 0; i < tasksCount; i++) {
            var _ =
                    executor.submit(
                            () -> {
                                try {
                                    var financialRequest =
                                            createPurchaseRequest(PAN, messageFactory);
                                    var expectedStan =
                                            financialRequest
                                                    .getField(
                                                            IsoField.SYSTEM_TRACE_AUDIT_NUMBER
                                                                    .getId())
                                                    .toString();

                                    var actualResponse = client.publishAndBlock(financialRequest);

                                    assertThat(actualResponse).isNotNull();
                                    assertThat(
                                                    actualResponse
                                                            .getField(
                                                                    IsoField
                                                                            .SYSTEM_TRACE_AUDIT_NUMBER
                                                                            .getId())
                                                            .toString())
                                            .isEqualTo(expectedStan);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                } finally {
                                    latch.countDown();
                                }
                            });
        }

        latch.await();

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
                                        .hasMeasurement(Statistic.COUNT, 211.0));
        await().alias("incoming responses")
                .untilAsserted(
                        () ->
                                MeterRegistryAssert.assertThat(meterRegistry)
                                        .meter(
                                                "cache.gets",
                                                Tag.of("name", "j8583-connector-promise-sinks"),
                                                Tag.of("result", "miss"))
                                        .hasMeasurement(Statistic.COUNT, 212.0));
        await().alias("processed responses")
                .untilAsserted(
                        () ->
                                MeterRegistryAssert.assertThat(meterRegistry)
                                        .meter(
                                                "cache.gets",
                                                Tag.of("name", "j8583-connector-promise-sinks"),
                                                Tag.of("result", "hit"))
                                        .hasMeasurement(Statistic.COUNT, 204.0));

        MeterRegistryAssert.assertThat(meterRegistry)
                .counter("j8583.connector.timeout.exceeded")
                .hasCount(9);
        MeterRegistryAssert.assertThat(meterRegistry)
                .counter("j8583.connector.orphaned.response")
                .hasCount(1);
    }
}
