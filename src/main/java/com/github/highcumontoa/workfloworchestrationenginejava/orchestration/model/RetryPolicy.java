package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.model;

/**
 * 失败重试策略。maxAttempts 为总尝试次数（含首次）；backoffMillis 为尝试间隔。
 */
public record RetryPolicy(int maxAttempts, long backoffMillis) {

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts 必须 >= 1（含首次尝试），实际: " + maxAttempts);
        }
        if (backoffMillis < 0) {
            throw new IllegalArgumentException("backoffMillis 不能为负数，实际: " + backoffMillis);
        }
    }

    public static RetryPolicy noRetry() {
        return new RetryPolicy(1, 0L);
    }

    public static RetryPolicy fixed(int maxAttempts, long backoffMillis) {
        return new RetryPolicy(maxAttempts, backoffMillis);
    }
}
