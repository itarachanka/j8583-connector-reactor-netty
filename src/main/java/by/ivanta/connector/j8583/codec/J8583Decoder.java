/**
 * TCP Connector using Reactor Netty.
 * Copyright (C) 2026 the original author or authors.
 */

package by.ivanta.connector.j8583.codec;

import com.solab.iso8583.IsoMessage;
import com.solab.iso8583.MessageFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class J8583Decoder extends ByteToMessageDecoder {

    private final MessageFactory<IsoMessage> messageFactory;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
            throws Exception {
        if (!in.isReadable()) {
            return;
        }
        var bytes = new byte[in.readableBytes()];
        in.readBytes(bytes);
        var isoMessage = messageFactory.parseMessage(bytes, 0);
        out.add(isoMessage);
    }
}
