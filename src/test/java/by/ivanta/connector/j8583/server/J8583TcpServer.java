/**
 * TCP Connector using Reactor Netty.
 * Copyright (C) 2026 the original author or authors.
 */

package by.ivanta.connector.j8583.server;

import static by.ivanta.connector.j8583.testutils.TestDataUtil.AUTHORISATION_IDENTIFICATION_CODE;
import static by.ivanta.connector.j8583.testutils.TestDataUtil.OK_APPROVED_CODE;
import static by.ivanta.connector.j8583.testutils.TestDataUtil.PAN_APPROVED_CODES;
import static by.ivanta.connector.j8583.testutils.TestDataUtil.PAN_WITH_BROKEN_RESPONSE;
import static by.ivanta.connector.j8583.testutils.TestDataUtil.PAN_WITH_IDLE_TIMEOUT;
import static by.ivanta.connector.j8583.testutils.TestDataUtil.PAN_WITH_NOT_EXISTS_STAN;
import static by.ivanta.connector.j8583.testutils.TestDataUtil.PAN_WITH_TIMEOUT;

import by.ivanta.connector.j8583.constant.IsoField;
import by.ivanta.connector.j8583.constant.MessageTypeIdentifier;
import com.solab.iso8583.IsoMessage;
import com.solab.iso8583.MessageFactory;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;

@Slf4j
public class J8583TcpServer {
    private final AtomicReference<Sinks.Empty<Void>> readinessSinkRef;
    private final DisposableServer server;

    public J8583TcpServer(TcpServer tcpServer, MessageFactory<IsoMessage> messageFactory) {
        super();
        this.readinessSinkRef = new AtomicReference<>(Sinks.empty());
        this.server =
                tcpServer
                        .doOnConnection(_ -> readinessSinkRef.get().tryEmitEmpty())
                        .handle(
                                (inbound, outbound) ->
                                        inbound.receiveObject()
                                                .doOnError(
                                                        error ->
                                                                log.error(
                                                                        "Failed while parsing"
                                                                                + " request [{}]",
                                                                        error.getMessage()))
                                                .ofType(IsoMessage.class)
                                                .flatMap(
                                                        request ->
                                                                Mono.just(
                                                                        messageFactory
                                                                                .createResponse(
                                                                                        request)))
                                                .flatMap(this::mapToUserScenarioIfAny)
                                                .flatMap(
                                                        J8583TcpServer::mapToFinancialResponseIfAny)
                                                .delayUntil(
                                                        _ ->
                                                                Mono.defer(
                                                                        () ->
                                                                                readinessSinkRef
                                                                                        .get()
                                                                                        .asMono()))
                                                .flatMap(
                                                        response ->
                                                                outbound.sendObject(
                                                                        Mono.just(response)))
                                                .then())
                        .bindNow();
    }

    public boolean isStarted() {
        return server.channel().isActive();
    }

    @PreDestroy
    public void shutdown() {
        server.disposeNow();
    }

    private Mono<IsoMessage> mapToUserScenarioIfAny(IsoMessage response) {
        var type = response.getType();
        if (type == MessageTypeIdentifier.FINANCIAL_RESPONSE.getHexCode()) {
            var pan = response.getField(IsoField.PAN.getId()).toString();

            switch (pan) {
                case PAN_WITH_NOT_EXISTS_STAN -> {
                    var stanField = IsoField.SYSTEM_TRACE_AUDIT_NUMBER;
                    response.setField(
                            stanField.getId(),
                            stanField.getIsoType().value(0, stanField.getLength()));
                }
                case PAN_WITH_IDLE_TIMEOUT -> {
                    readinessSinkRef.set(Sinks.empty());
                }
                case PAN_WITH_BROKEN_RESPONSE -> {
                    var panField = IsoField.PAN;
                    response.setField(
                            panField.getId(),
                            panField.getIsoType()
                                    .value(
                                            PAN_WITH_BROKEN_RESPONSE,
                                            PAN_WITH_BROKEN_RESPONSE.length() + 1));
                }
            }
        }
        return Mono.just(response);
    }

    private static Mono<IsoMessage> mapToFinancialResponseIfAny(IsoMessage response) {
        var type = response.getType();
        if (type == MessageTypeIdentifier.FINANCIAL_RESPONSE.getHexCode()) {
            var pan = response.getField(IsoField.PAN.getId()).toString();

            if (PAN_WITH_TIMEOUT.equals(pan)) {
                return Mono.empty();
            }

            var authIdentRespField = IsoField.AUTHORISATION_IDENTIFICATION_RESPONSE;
            response.setField(
                    authIdentRespField.getId(),
                    authIdentRespField
                            .getIsoType()
                            .value(
                                    AUTHORISATION_IDENTIFICATION_CODE,
                                    authIdentRespField.getLength()));
            var responseCodeField = IsoField.RESPONSE_CODE;
            response.setField(
                    responseCodeField.getId(),
                    responseCodeField
                            .getIsoType()
                            .value(
                                    PAN_APPROVED_CODES.getOrDefault(pan, OK_APPROVED_CODE),
                                    responseCodeField.getLength()));
            response.removeFields(
                    IsoField.TIME_LOCAL_TRANSACTION.getId(),
                    IsoField.DATE_LOCAL_TRANSACTION.getId(),
                    IsoField.DATE_EXPIRATION.getId());
        }
        return Mono.just(response);
    }
}
