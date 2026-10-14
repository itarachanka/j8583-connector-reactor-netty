/**
 * TCP Connector using Reactor Netty.
 * Copyright (C) 2026 the original author or authors.
 */

package by.ivanta.connector.j8583.codec;

import by.ivanta.connector.config.properties.J8583ConnectorProperties;
import com.solab.iso8583.IsoMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.springframework.stereotype.Component;

@Component
@ChannelHandler.Sharable
public class J8583Encoder extends MessageToByteEncoder<IsoMessage> {

    private final int lengthBytes;

    public J8583Encoder(J8583ConnectorProperties j8583ConnectorProperties) {
        super();
        this.lengthBytes = j8583ConnectorProperties.lengthFieldLength();
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, IsoMessage msg, ByteBuf out) throws Exception {
        var byteBuffer = msg.writeToBuffer(lengthBytes);
        out.writeBytes(byteBuffer);
    }
}
