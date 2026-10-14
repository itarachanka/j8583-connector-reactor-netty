/**
 * TCP Connector using Reactor Netty.
 * Copyright (C) 2026 the original author or authors.
 */

package by.ivanta.connector.config.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "j8583.connector")
public record J8583ConnectorProperties(
        @DefaultValue("127.0.0.1") String host,
        @DefaultValue("12012") int port,
        @DefaultValue("4") int timeoutSec,
        @DefaultValue("1000") int connectTimeoutMs,
        @DefaultValue("10") int disconnectTimeoutSec,
        @DefaultValue("4") int retryCooldownSec,
        @DefaultValue("2") int retryMinBackoffSec,
        @DefaultValue("4") int retryMaxBackoffSec,
        @DefaultValue("14") int readerIdleTimeoutSec,
        @DefaultValue("8") int writerIdleTimeoutSec,
        @DefaultValue("8192") int maxFrameLength,
        @DefaultValue("0") int frameLengthFieldOffset,
        @DefaultValue("2") int lengthFieldLength,
        @DefaultValue("0") int frameLengthFieldAdjust) {}
