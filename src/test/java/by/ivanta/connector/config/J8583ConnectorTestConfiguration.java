/**
 * TCP Connector using Reactor Netty.
 * Copyright (C) 2026 the original author or authors.
 */

package by.ivanta.connector.config;

import by.ivanta.connector.j8583.server.J8583TcpServer;
import by.ivanta.connector.support.DefaultTraceNumberGenerator;
import com.solab.iso8583.IsoMessage;
import com.solab.iso8583.MessageFactory;
import com.solab.iso8583.TraceNumberGenerator;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import reactor.netty.tcp.TcpServer;

@TestConfiguration
public class J8583ConnectorTestConfiguration {

    @Bean
    J8583TcpServer j8583TcpServer(TcpServer tcpServer, MessageFactory<IsoMessage> messageFactory) {
        return new J8583TcpServer(tcpServer, messageFactory);
    }

    @Bean
    @Primary
    TraceNumberGenerator traceNumberGenerator() {
        return new DefaultTraceNumberGenerator(0);
    }
}
