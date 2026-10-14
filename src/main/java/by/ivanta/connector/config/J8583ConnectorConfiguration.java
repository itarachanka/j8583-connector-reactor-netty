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
import com.solab.iso8583.TraceNumberGenerator;
import com.solab.iso8583.parse.ConfigParser;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Sinks;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

@Configuration
@EnableConfigurationProperties(J8583ConnectorProperties.class)
public class J8583ConnectorConfiguration {

    @Bean
    LoopResources loopResources() {
        return LoopResources.create(
                "j8583-connector-loop", 1, LoopResources.DEFAULT_IO_WORKER_COUNT, true);
    }

    @Bean
    MessageFactory<IsoMessage> messageFactory(TraceNumberGenerator traceGen) throws IOException {
        var messageFactory = ConfigParser.createDefault();
        messageFactory.setCharacterEncoding(StandardCharsets.US_ASCII.name());
        messageFactory.setUseBinaryBitmap(true);
        messageFactory.setUseBinaryMessages(true);
        messageFactory.setAssignDate(true);
        messageFactory.setTraceNumberGenerator(traceGen);
        return messageFactory;
    }

    @Bean
    TcpClient tcpClient(
            J8583ConnectorProperties j8583ConnectorProperties,
            LoopResources loopResources,
            MessageFactory<IsoMessage> messageFactory,
            J8583Encoder j8583Encoder) {
        return TcpClient.create()
                .host(j8583ConnectorProperties.host())
                .port(j8583ConnectorProperties.port())
                .metrics(true)
                .option(
                        ChannelOption.TCP_NODELAY,
                        Boolean.parseBoolean(System.getProperty("nfs.rpc.tcp.nodelay", "true")))
                .option(
                        ChannelOption.CONNECT_TIMEOUT_MILLIS,
                        j8583ConnectorProperties.connectTimeoutMs())
                .runOn(loopResources)
                .doOnConnected(
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

    @Bean
    Sinks.Many<IsoMessage> outboundSink() {
        return Sinks.many().unicast().onBackpressureBuffer();
    }
}
