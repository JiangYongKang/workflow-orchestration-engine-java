package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core;

import com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model.RejectReason;

/**
 * 编排被拒绝或执行失败耗尽时抛出的异常，携带可区分的原因。
 */
public class OrchestrationException extends RuntimeException {

    private final RejectReason reason;

    public OrchestrationException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public OrchestrationException(RejectReason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
