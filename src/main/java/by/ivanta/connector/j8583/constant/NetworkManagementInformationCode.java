/**
 * TCP Connector using Reactor Netty.
 * Copyright (C) 2026 the original author or authors.
 */

package by.ivanta.connector.j8583.constant;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum NetworkManagementInformationCode {
    SIGN_ON_REQUEST(1),
    SIGN_OFF_REQUEST(2),
    ECHO_TEST_MESSAGE(301);

    private final long code;
}
