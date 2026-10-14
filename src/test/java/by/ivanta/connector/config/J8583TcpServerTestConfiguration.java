/**
 * TCP Connector using Reactor Netty.
 * Copyright (C) 2026 the original author or authors.
 */

package by.ivanta.connector.config;

import by.ivanta.connector.config.properties.J8583ConnectorProperties;
import by.ivanta.connector.j8583.codec.J8583Decoder;
import by.ivanta.connector.j8583.codec.J8583Encoder;
import com.solab.iso8583.IsoMessage;
import com.solab.iso8583.MessageFactory;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import reactor.netty.tcp.TcpServer;

@TestConfiguration
public class J8583TcpServerTestConfiguration {

    @Bean
    TcpServer tcpServer(
            J8583ConnectorProperties j8583ConnectorProperties,
            MessageFactory<IsoMessage> messageFactory,
            J8583Encoder j8583Encoder) {
        return TcpServer.create()
                .host(j8583ConnectorProperties.host())
                .port(j8583ConnectorProperties.port())
                .wiretap(true)
                .childOption(
                        ChannelOption.TCP_NODELAY,
                        Boolean.parseBoolean(System.getProperty("nfs.rpc.tcp.nodelay", "true")))
                .childOption(
                        ChannelOption.CONNECT_TIMEOUT_MILLIS,
                        j8583ConnectorProperties.connectTimeoutMs())
                .childOption(ChannelOption.AUTO_READ, false)
                .doOnConnection(
                        conn -> {
                            conn.addHandlerLast(
                                            "lengthFieldFrameDecoder",
                                            new LengthFieldBasedFrameDecoder(
                                                    j8583ConnectorProperties.maxFrameLength(),
                                                    j8583ConnectorProperties
                                                            .frameLengthFieldOffset(),
                                                    j8583ConnectorProperties.lengthFieldLength(),
                                                    j8583ConnectorProperties
                                                            .frameLengthFieldAdjust(),
                                                    j8583ConnectorProperties.lengthFieldLength()))
                                    .addHandlerLast(
                                            "j8583Decoder", new J8583Decoder(messageFactory))
                                    .addHandlerLast("j8583Encoder", j8583Encoder);
                        });
    }
}
