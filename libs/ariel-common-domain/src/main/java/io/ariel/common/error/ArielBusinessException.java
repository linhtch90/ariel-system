package io.ariel.common.error;

import org.springframework.http.HttpStatus;

/**
 * Domain exception carrying HTTP status and RFC 9457 error code semantics.
 */
public class ArielBusinessException extends RuntimeException {

    private final HttpStatus status;
    private final String errorCode;

    public ArielBusinessException(HttpStatus status, String errorCode, String message) {
        super(message);
        this.status = status;
        this.errorCode = errorCode;
    }

    public HttpStatus getStatus() {
        return status;
    }

    public String getErrorCode() {
        return errorCode;
    }
}
