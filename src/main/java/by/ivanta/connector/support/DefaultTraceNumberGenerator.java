/**
 * TCP Connector using Reactor Netty.
 * Copyright (C) 2026 the original author or authors.
 */

package by.ivanta.connector.support;

import by.ivanta.connector.j8583.constant.IsoField;
import com.solab.iso8583.TraceNumberGenerator;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

@Component
public final class DefaultTraceNumberGenerator implements TraceNumberGenerator {
    private static final long MAX_SEQUENCE_VALUE =
            (long) Math.pow(10, IsoField.SYSTEM_TRACE_AUDIT_NUMBER.getLength());

    private final AtomicInteger sequence;

    public DefaultTraceNumberGenerator() {
        this(Math.max(Math.toIntExact(System.currentTimeMillis() % MAX_SEQUENCE_VALUE), 0));
    }

    public DefaultTraceNumberGenerator(int initialValue) {
        if (initialValue < 0 || initialValue >= MAX_SEQUENCE_VALUE) {
            throw new IllegalArgumentException(
                    "Initial value must be between 0 and " + MAX_SEQUENCE_VALUE);
        }
        sequence = new AtomicInteger(initialValue);
    }

    @Override
    public int nextTrace() {
        return sequence.accumulateAndGet(
                1, (prev, acc) -> prev + acc >= MAX_SEQUENCE_VALUE ? 1 : prev + acc);
    }

    @Override
    public int getLastTrace() {
        return sequence.get();
    }
}
