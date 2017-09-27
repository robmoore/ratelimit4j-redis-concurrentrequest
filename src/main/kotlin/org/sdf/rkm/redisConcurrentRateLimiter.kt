package org.sdf.rkm

import es.moki.ratelimitj.core.limiter.concurrent.Baton
import es.moki.ratelimitj.core.limiter.concurrent.ConcurrentLimitRule
import es.moki.ratelimitj.core.limiter.concurrent.ConcurrentRequestLimiter
import es.moki.ratelimitj.core.limiter.request.RequestLimitRule
import es.moki.ratelimitj.redis.request.RedisRateLimiterFactory
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

class RedisConcurrentRateLimiter(factory: RedisRateLimiterFactory, rule: ConcurrentLimitRule): ConcurrentRequestLimiter {
    private val rules = setOf(RequestLimitRule.of(rule.timeoutMillis.toInt(), TimeUnit.MILLISECONDS, rule.concurrentLimit.toLong()))
    private val rateLimiter = factory.getInstance(rules)

    override fun acquire(key: String): Baton {
        return acquire(key, 1)
    }

    override fun acquire(key: String, weight: Int): Baton {
        return if (rateLimiter.overLimitWhenIncremented(key, weight)) {
            RedisBaton()
        } else {
            RedisBaton(true)
        }
    }
}

class RedisBaton(var acquired: Boolean = false): Baton {
    override fun hasAcquired(): Boolean {
        return acquired
    }

    override fun doAction(action: Runnable) {
        if (acquired) {
            try {
                action.run()
            } finally {
                acquired = false
            }
        }
    }

    override fun release() {
        // expires automatically
    }

    override fun <T : Any> get(action: Supplier<T>): Optional<T> {
        return if (acquired) {
            try {
                Optional.of(action.get())
            } finally {
                acquired = false
            }
        } else {
            Optional.empty()
        }
    }
}