package com.github.highcumontoa.workfloworchestrationenginejava.orchestration.core;

/**
 * 重试策略：最大尝试次数（含首次执行）。例如 {@code maxAttempts=3}
 * 表示首次执行失败后最多再重试 2 次，总计尝试 3 次。
 */
public record RetryPolicy(int maxAttempts) {

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts 必须 >= 1，实际为: " + maxAttempts);
        }
    }

    public static RetryPolicy of(int maxAttempts) {
        return new RetryPolicy(maxAttempts);
    }
}
