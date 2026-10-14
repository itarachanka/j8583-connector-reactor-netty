/**
 * TCP Connector using Reactor Netty.
 * Copyright (C) 2026 the original author or authors.
 */

package by.ivanta.connector.j8583;

import by.ivanta.connector.config.properties.J8583ConnectorProperties;
import by.ivanta.connector.j8583.constant.IsoField;
import by.ivanta.connector.j8583.constant.MessageTypeIdentifier;
import by.ivanta.connector.j8583.constant.NetworkManagementInformationCode;
import com.solab.iso8583.IsoMessage;
import com.solab.iso8583.MessageFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.internal.annotation.SuppressFBWarnings;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.util.retry.Retry;

@Component
@Slf4j
public class J8583Client {
    private final LoopResources loopResources;
    private final TcpClient tcpClient;
    private final Sinks.Many<IsoMessage> outboundSink;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "Assign from a trusted source")
    private final MessageFactory<IsoMessage> messageFactory;

    private final J8583ConnectorProperties j8583ConnectorProperties;
    private final Cache promiseSinks;
    private final AtomicReference<Sinks.Empty<Void>> readinessSinkRef;
    private final AtomicReference<Connection> connectionRef;
    private final Disposable.Swap disposableSwap;
    private final Flux<IsoMessage> outboundSinkFlux;
    private final Counter timeoutExceededCounter;
    private final Counter orphanedResponseCounter;

    public J8583Client(
            LoopResources loopResources,
            TcpClient tcpClient,
            Sinks.Many<IsoMessage> outboundSink,
            MessageFactory<IsoMessage> messageFactory,
            J8583ConnectorProperties j8583ConnectorProperties,
            CacheManager cacheManager,
            MeterRegistry meterRegistry) {
        super();
        this.loopResources = loopResources;
        this.tcpClient = tcpClient;
        this.outboundSink = outboundSink;
        this.messageFactory = messageFactory;
        this.j8583ConnectorProperties = j8583ConnectorProperties;
        this.promiseSinks = cacheManager.getCache("j8583-connector-promise-sinks");
        this.readinessSinkRef = new AtomicReference<>(Sinks.empty());
        this.connectionRef = new AtomicReference<>();
        this.disposableSwap = Disposables.swap();
        this.outboundSinkFlux =
                this.outboundSink
                        .asFlux()
                        .publish()
                        .autoConnect(0)
                        .ofType(IsoMessage.class)
                        .delayUntil(_ -> Mono.defer(() -> readinessSinkRef.get().asMono()));
        this.timeoutExceededCounter =
                Counter.builder("j8583.connector.timeout.exceeded").register(meterRegistry);
        this.orphanedResponseCounter =
                Counter.builder("j8583.connector.orphaned.response").register(meterRegistry);
    }

    public Mono<IsoMessage> publish(IsoMessage request) {
        return switch (request.getType()) {
            case 0x0100, 0x0200, 0x0220, 0x0420 ->
                    getTraceNumber(request)
                            .map(stan -> createPromiseMono(request, stan))
                            .orElse(Mono.empty());
            default -> Mono.empty();
        };
    }

    public IsoMessage publishAndBlock(IsoMessage request) {
        return publish(request).block();
    }

    public boolean isConnected() {
        var connection = connectionRef.get();
        return connection != null && !connection.isDisposed();
    }

    @PostConstruct
    void postConstruct() {
        var client =
                tcpClient
                        .doOnChannelInit((_, _, _) -> doReadinessDown())
                        .doOnConnected(
                                conn -> {
                                    conn.addHandlerLast(
                                                    "idleStateHandler",
                                                    new IdleStateHandler(
                                                            j8583ConnectorProperties
                                                                    .readerIdleTimeoutSec(),
                                                            j8583ConnectorProperties
                                                                    .writerIdleTimeoutSec(),
                                                            0))
                                            .addHandlerLast(
                                                    "idleTimeoutHandler",
                                                    new ChannelDuplexHandler() {
                                                        @Override
                                                        @SuppressWarnings(
                                                                "FutureReturnValueIgnored")
                                                        public void userEventTriggered(
                                                                ChannelHandlerContext ctx,
                                                                Object evt)
                                                                throws Exception {
                                                            if (evt instanceof IdleStateEvent e) {
                                                                switch (e.state()) {
                                                                    case ALL_IDLE, READER_IDLE -> {
                                                                        doReadinessDown();
                                                                        ctx.close();
                                                                    }
                                                                    case WRITER_IDLE -> {
                                                                        doEchoTestRequest(conn);
                                                                    }
                                                                }
                                                            }
                                                            super.userEventTriggered(ctx, evt);
                                                        }
                                                    });
                                })
                        .handle(
                                (inbound, outbound) -> {
                                    var incoming =
                                            inbound.receiveObject()
                                                    .doOnError(
                                                            e ->
                                                                    log.error(
                                                                            "Failed to decode raw ISO8583 response",
                                                                            e))
                                                    .ofType(IsoMessage.class)
                                                    .doOnNext(this::doProcessResponse)
                                                    .then();

                                    var outcoming =
                                            outboundSinkFlux
                                                    .as(outbound::sendObject)
                                                    .then()
                                                    .doOnError(
                                                            e ->
                                                                    log.error(
                                                                            "Failed to encode raw ISO8583 request",
                                                                            e));

                                    return Mono.when(incoming, outcoming);
                                })
                        .doOnConnected(conn -> connectionRef.set(conn))
                        .doOnConnected(this::doSignOnRequest)
                        .doOnDisconnected(_ -> doReadinessDown());

        var connectionMono =
                Mono.defer(() -> client.connect())
                        .retryWhen(
                                Retry.backoff(
                                                Long.MAX_VALUE,
                                                Duration.ofSeconds(
                                                        j8583ConnectorProperties
                                                                .retryMinBackoffSec()))
                                        .maxBackoff(
                                                Duration.ofSeconds(
                                                        j8583ConnectorProperties
                                                                .retryMaxBackoffSec()))
                                        .onRetryExhaustedThrow(
                                                (_, retrySignal) -> retrySignal.failure())
                                        .doBeforeRetry(
                                                retrySignal ->
                                                        log.info(
                                                                "Connection failed or lost. Retrying (Attempt #{}) in [{}]",
                                                                retrySignal.totalRetries() + 1,
                                                                retrySignal
                                                                        .failure()
                                                                        .getMessage())));

        disposableSwap.update(
                connectionMono.subscribe(
                        conn -> conn.onDispose(this::postConstruct),
                        _ ->
                                Mono.delay(
                                                Duration.ofSeconds(
                                                        j8583ConnectorProperties
                                                                .retryCooldownSec()))
                                        .doAfterTerminate(this::postConstruct)
                                        .subscribe()));
    }

    @PreDestroy
    @SuppressWarnings({"PMD.AssignmentInOperand", "PMD.AvoidCatchingGenericException"})
    void preDestroy() {
        try {
            doReadinessDown();
            doSignOffRequest();
            Sinks.EmitResult result;
            while ((result = outboundSink.tryEmitComplete())
                    == Sinks.EmitResult.FAIL_NON_SERIALIZED) {
                // busy spin
            }
            result.orThrow();
        } catch (Exception e) {
            log.warn("Failed to shutdown j8583 client, message is [{}]", e.getMessage());
        } finally {
            doDisconnect();
            if (!disposableSwap.isDisposed()) {
                disposableSwap.dispose();
            }
            promiseSinks.invalidate();
            loopResources.disposeLater().block();
        }
    }

    private void doReadinessDown() {
        readinessSinkRef.set(Sinks.empty());
    }

    @SuppressWarnings("PMD.AssignmentInOperand")
    private void doPublish(IsoMessage request) {
        Sinks.EmitResult result;
        while ((result = outboundSink.tryEmitNext(request))
                == Sinks.EmitResult.FAIL_NON_SERIALIZED) {
            // busy spin
        }
        result.orThrow();
    }

    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private void doDisconnect() {
        try {
            if (isConnected()) {
                connectionRef
                        .get()
                        .disposeNow(
                                Duration.ofSeconds(
                                        j8583ConnectorProperties.disconnectTimeoutSec()));
            }
        } catch (Exception e) {
            log.warn("Failed to disconnect from server, message is [{}]", e.getMessage());
        }
    }

    private Mono<IsoMessage> createPromiseMono(IsoMessage request, String stan) {
        var sink = Sinks.<IsoMessage>one();
        promiseSinks.putIfAbsent(stan, sink);
        doPublish(request);
        return sink.asMono()
                .timeout(Duration.ofSeconds(j8583ConnectorProperties.timeoutSec()))
                .doOnError(
                        e -> e instanceof TimeoutException, _ -> timeoutExceededCounter.increment())
                .doOnError(_ -> log.error("Failed while processing request by [{}]", stan))
                .doAfterTerminate(() -> promiseSinks.evictIfPresent(stan));
    }

    private IsoMessage createSignOnRequest() {
        return createNetworkManagementRequest(
                NetworkManagementInformationCode.SIGN_ON_REQUEST.getCode());
    }

    private IsoMessage createSignOffRequest() {
        return createNetworkManagementRequest(
                NetworkManagementInformationCode.SIGN_OFF_REQUEST.getCode());
    }

    private IsoMessage createEchoTestRequest() {
        return createNetworkManagementRequest(
                NetworkManagementInformationCode.ECHO_TEST_MESSAGE.getCode());
    }

    private IsoMessage createNetworkManagementRequest(long code) {
        var message =
                messageFactory.newMessage(
                        MessageTypeIdentifier.NETWORK_MANAGEMENT_REQUEST.getHexCode());
        var infoCodeField = IsoField.NETWORK_MANAGEMENT_INFORMATION_CODE;
        message.setField(
                infoCodeField.getId(),
                infoCodeField.getIsoType().value(code, infoCodeField.getLength()));
        return message;
    }

    private void doSignOnRequest(Connection connection) {
        doSendRequest(connection, createSignOnRequest());
    }

    private void doSignOffRequest() {
        if (isConnected()) {
            doSendRequest(connectionRef.get(), createSignOffRequest());
        }
    }

    private void doEchoTestRequest(Connection connection) {
        doSendRequest(connection, createEchoTestRequest());
    }

    private void doSendRequest(Connection connection, IsoMessage request) {
        connection
                .outbound()
                .sendObject(request)
                .then()
                .timeout(Duration.ofSeconds(j8583ConnectorProperties.timeoutSec()))
                .doOnError(
                        error ->
                                log.error(
                                        "Failed while sending network management request, reason is [{}]",
                                        error.getMessage()))
                .subscribe();
    }

    private void doProcessResponse(IsoMessage response) {
        switch (response.getType()) {
            case 0x0810 -> {
                if (isSuccessSignOnResponse(response)) {
                    readinessSinkRef.get().tryEmitEmpty();
                }
            }
            default -> {
                getTraceNumber(response)
                        .ifPresent(
                                stan ->
                                        Optional.ofNullable(promiseSinks.get(stan, Sinks.One.class))
                                                .ifPresentOrElse(
                                                        promiseSink ->
                                                                tryEmitResponse(
                                                                        promiseSink, response),
                                                        () ->
                                                                incrementOrphanedResponseCounter(
                                                                        stan)));
            }
        }
    }

    private void incrementOrphanedResponseCounter(String stan) {
        log.warn("Orphaned response is [{}]", stan);
        orphanedResponseCounter.increment();
    }

    @SuppressWarnings("PMD.AssignmentInOperand")
    private void tryEmitResponse(Sinks.One<IsoMessage> sink, IsoMessage response) {
        Sinks.EmitResult result;
        while ((result = sink.tryEmitValue((IsoMessage) response))
                == Sinks.EmitResult.FAIL_NON_SERIALIZED) {
            // busy spin
        }
        result.orThrow();
    }

    private static Optional<String> getTraceNumber(IsoMessage message) {
        return message.objectValue(IsoField.SYSTEM_TRACE_AUDIT_NUMBER.getId())
                .map(Object::toString);
    }

    private static boolean isSuccessSignOnResponse(IsoMessage response) {
        return Objects.equals(
                NetworkManagementInformationCode.SIGN_ON_REQUEST.getCode(),
                response.getObjectValue(IsoField.NETWORK_MANAGEMENT_INFORMATION_CODE.getId()));
    }
}
