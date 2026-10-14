/**
 * TCP Connector using Reactor Netty.
 * Copyright (C) 2026 the original author or authors.
 */

package by.ivanta.connector.config;

import static reactor.netty.Metrics.REMOTE_ADDRESS;
import static reactor.netty.Metrics.TCP_CLIENT_PREFIX;

import io.micrometer.core.instrument.config.MeterFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class MetricsConfig {

    @Bean
    MeterFilter denyTcpClientMetricsFilter() {
        return MeterFilter.maximumAllowableTags(
                TCP_CLIENT_PREFIX, REMOTE_ADDRESS, 100, MeterFilter.deny());
    }
}
