/**
 * TCP Connector using Reactor Netty.
 * Copyright (C) 2026 the original author or authors.
 */

package by.ivanta.connector.j8583.constant;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum MessageTypeIdentifier {
    AUTHORIZATION_REQUEST("0100", 0x0100),
    AUTHORIZATION_RESPONSE("0110", 0x0110),

    FINANCIAL_REQUEST("0200", 0x0200),
    FINANCIAL_RESPONSE("0210", 0x0210),

    ADVISE_REQUEST("0220", 0x0220),
    ADVISE_RESPONSE("0230", 0x0230),

    REVERSAL_REQUEST("0420", 0x0420),
    REVERSAL_RESPONSE("0430", 0x0430),

    NETWORK_MANAGEMENT_REQUEST("0800", 0x0800),
    NETWORK_MANAGEMENT_RESPONSE("0810", 0x0810);

    private final String code;
    private final int hexCode;
}
