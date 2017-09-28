package org.sdf.rkm

import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.result.Result
import com.lambdaworks.redis.RedisClient
import es.moki.ratelimitj.core.limiter.concurrent.Baton
import es.moki.ratelimitj.core.limiter.concurrent.ConcurrentLimitRule
import es.moki.ratelimitj.core.limiter.concurrent.ConcurrentRequestLimiter
import es.moki.ratelimitj.redis.request.RedisRateLimiterFactory
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.runBlocking
import net.jodah.failsafe.Failsafe
import net.jodah.failsafe.RetryPolicy
import net.jodah.failsafe.function.Predicate
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit

@SpringBootApplication
class DemoApplication {
    @Bean
    fun commandLineRunner(ctx: ApplicationContext, limiter: ConcurrentRequestLimiter): CommandLineRunner {
        return CommandLineRunner {
            val retryPolicy = RetryPolicy().retryIf<Baton>(Predicate { !it.hasAcquired() })
                    .withBackoff(1, 30, TimeUnit.SECONDS).withJitter(.25)
                    .withMaxRetries(10)
            runBlocking {
                val job = async(CommonPool) {
                    IntRange(1, 20).forEach {
                        val baton = Failsafe.with<Baton>(retryPolicy)
                                .onRetriesExceeded { _, _ -> System.err.println("Failed to connect. Max retries exceeded.") }
                                .get(Callable<Baton> { limiter.acquire("test") })

                        baton.doAction {
                            "https://now.httpbin.org/".httpGet().responseString { _, _, result ->
                                when (result) {
                                    is Result.Failure -> {
                                        System.err.println("$it: ${result.error.response.data}")
                                    }
                                    is Result.Success -> {
                                        println("$it: $result.value")
                                    }
                                }
                            }
                        }
                    }
                }
                job.join()
            }
        }
    }

    @Bean
    fun redisClient(): RedisClient {
        return RedisClient.create("redis://localhost")
    }

    @Bean
    fun concurrentLimitRule(): ConcurrentLimitRule {
        // int concurrentLimit, TimeUnit timeOutUnit, long timeOut
        return ConcurrentLimitRule.of(4, TimeUnit.SECONDS, 1)
    }

    @Bean
    fun concurrentRequestLimiter(redisClient: RedisClient, concurrentLimitRule: ConcurrentLimitRule): ConcurrentRequestLimiter {
        return RedisConcurrentRateLimiter(RedisRateLimiterFactory(redisClient), concurrentLimitRule)
    }
}

fun main(args: Array<String>) {
    SpringApplication.run(DemoApplication::class.java, *args)
}
