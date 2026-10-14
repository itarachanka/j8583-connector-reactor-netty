/**
 * TCP Connector using Reactor Netty.
 * Copyright (C) 2026 the original author or authors.
 */

package by.ivanta.connector.j8583.testutils;

import by.ivanta.connector.j8583.constant.IsoField;
import by.ivanta.connector.j8583.constant.MessageTypeIdentifier;
import by.ivanta.connector.j8583.constant.NetworkManagementInformationCode;
import com.solab.iso8583.IsoMessage;
import com.solab.iso8583.MessageFactory;
import java.util.Map;
import lombok.experimental.UtilityClass;

@UtilityClass
public class TestDataUtil {
    public static final String RECONCILE_ERROR_APPROVED_CODE = "95";
    public static final String NO_REASON_TO_DECLINE_APPROVED_CODE = "85";
    public static final String INCORRECT_CVV_APPROVED_CODE = "82";
    public static final String OK_APPROVED_CODE = "00";

    public static final String PAN = "5208000000000001";
    public static final String PAN_WITH_TIMEOUT = "5208000000000002";
    public static final String PAN_WITH_NOT_EXISTS_STAN = "5208000000000003";
    public static final String PAN_WITH_IDLE_TIMEOUT = "5208000000000004";
    public static final String PAN_WITH_BROKEN_RESPONSE = "5208000000000005";
    public static final String PAN_WITH_APPROVED_CODE_95 = "5208000000000006";
    public static final String PAN_WITH_APPROVED_CODE_85 = "5208000000000007";
    public static final String PAN_WITH_APPROVED_CODE_82 = "5208000000000008";

    public static final String AUTHORISATION_IDENTIFICATION_CODE = "000000";

    public static final Map<String, String> PAN_APPROVED_CODES =
            Map.of(
                    PAN_WITH_APPROVED_CODE_95,
                    RECONCILE_ERROR_APPROVED_CODE,
                    PAN_WITH_APPROVED_CODE_85,
                    NO_REASON_TO_DECLINE_APPROVED_CODE,
                    PAN_WITH_APPROVED_CODE_82,
                    INCORRECT_CVV_APPROVED_CODE);

    public static IsoMessage createPurchaseRequest(
            String pan, MessageFactory<IsoMessage> messageFactory) {
        var message =
                messageFactory.newMessage(MessageTypeIdentifier.FINANCIAL_REQUEST.getHexCode());
        var panField = IsoField.PAN;
        message.setField(panField.getId(), panField.getIsoType().value(pan, pan.length()));
        var processingCodeField = IsoField.PROCESSING_CODE;
        message.setField(
                processingCodeField.getId(),
                processingCodeField.getIsoType().value(0, processingCodeField.getLength()));
        var amountField = IsoField.AMOUNT_TRANSACTION;
        message.setField(
                amountField.getId(), amountField.getIsoType().value(1000, amountField.getLength()));
        var localTimeField = IsoField.TIME_LOCAL_TRANSACTION;
        message.setField(
                localTimeField.getId(),
                localTimeField.getIsoType().value(111111, localTimeField.getLength()));
        var localDateField = IsoField.DATE_LOCAL_TRANSACTION;
        message.setField(
                localDateField.getId(),
                localDateField.getIsoType().value(2222, localDateField.getLength()));
        var expDateField = IsoField.DATE_EXPIRATION;
        message.setField(
                expDateField.getId(),
                expDateField.getIsoType().value(3333, expDateField.getLength()));
        var rrnField = IsoField.RETRIEVAL_REFERENCE_NUMBER;
        message.setField(
                rrnField.getId(),
                rrnField.getIsoType().value("000000000000", rrnField.getLength()));
        var currencyCodeField = IsoField.CURRENCY_CODE_TRANSACTION;
        message.setField(
                currencyCodeField.getId(),
                currencyCodeField.getIsoType().value(933, currencyCodeField.getLength()));
        return message;
    }

    public static IsoMessage createBrokenPurchaseRequest(
            String pan, MessageFactory<IsoMessage> messageFactory) {
        var brokenPurchase = createPurchaseRequest(pan, messageFactory);
        var panField = IsoField.PAN;
        brokenPurchase.setField(
                panField.getId(), panField.getIsoType().value(pan, pan.length() + 1));
        return brokenPurchase;
    }

    public static IsoMessage createEchoTestRequest(MessageFactory<IsoMessage> messageFactory) {
        var message =
                messageFactory.newMessage(
                        MessageTypeIdentifier.NETWORK_MANAGEMENT_REQUEST.getHexCode());
        var infoCodeField = IsoField.NETWORK_MANAGEMENT_INFORMATION_CODE;
        message.setField(
                infoCodeField.getId(),
                infoCodeField
                        .getIsoType()
                        .value(
                                NetworkManagementInformationCode.ECHO_TEST_MESSAGE.getCode(),
                                infoCodeField.getLength()));
        return message;
    }
}
