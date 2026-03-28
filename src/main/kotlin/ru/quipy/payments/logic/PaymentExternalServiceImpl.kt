package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.pow

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private val mapper = ObjectMapper().registerKotlinModule()
    }

    private val repeatTimes = 3
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val hedgedRetryCount = 0
    private val hedgedRequestDelayMs = 200L
    private val timeToDrop = 10000000L
    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(), Duration.ofSeconds(1)
    )
    private val semaphore = Semaphore(parallelRequests)
    private val paymentScope =
        CoroutineScope(Executors.newFixedThreadPool(100).asCoroutineDispatcher() + SupervisorJob())
    private val httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build()
    private val requestLatency =
        DistributionSummary.builder("request_latency").publishPercentiles(0.5, 0.7, 0.9, 0.99, 0.999, 0.9999)
            .register(meterRegistry)

    private var circuitBreakerConfig = CircuitBreakerConfig.custom()
        .failureRateThreshold(10f)
        .slidingWindowSize(200)
        .minimumNumberOfCalls(50)
        .waitDurationInOpenState(Duration.ofSeconds(5))
        .build()

    private val circuitBreaker = CircuitBreaker.of("payment-provider-$accountName", circuitBreakerConfig)


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.debug("[{}] Submitting payment request for payment {}", accountName, paymentId)
        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        paymentScope.launch {
            semaphore.withPermit {
                repeat(repeatTimes) { attempt ->
                    val success = sendHedgedRequest(transactionId, paymentId, amount, paymentStartedAt)
                    recordRetryAttempt(attempt + 1, success)

                    if (success) return@launch

                    if (attempt + 1 < repeatTimes) delay((1L * 2.0.pow((attempt + 1).toDouble())).toLong())
                }
            }
        }
    }

    private suspend fun sendHedgedRequest(
        transactionId: UUID, paymentId: UUID, amount: Int, paymentStartedAt: Long
    ): Boolean = coroutineScope {
        val totalRequests = 1 + hedgedRetryCount

        val requests = (0 until totalRequests).map { requestIndex ->
            async {
                if (requestIndex > 0) delay(hedgedRequestDelayMs * requestIndex)
                sendRequestWithRateLimit(transactionId, paymentId, amount, paymentStartedAt)
            }
        }

        val pending = requests.toMutableSet()
        while (pending.isNotEmpty()) {
            val (completedRequest, result) = select {
                pending.forEach { request ->
                    request.onAwait { request to it }
                }
            }

            pending.remove(completedRequest)
            if (result) {
                pending.forEach { it.cancel() }
                return@coroutineScope true
            }
        }

        false
    }

    private suspend fun sendRequestWithRateLimit(
        transactionId: UUID, paymentId: UUID, amount: Int, paymentStartedAt: Long
    ): Boolean {
        rateLimiter.tickBlocking()
        return sendRequest(transactionId, paymentId, amount, paymentStartedAt)
    }

    private suspend fun sendRequest(
        transactionId: UUID, paymentId: UUID, amount: Int, paymentStartedAt: Long
    ): Boolean {
        if (!circuitBreaker.tryAcquirePermission()) {
            logger.warn(
                "[{}] Circuit breaker is OPEN, skipping payment for txId: {}, payment: {}",
                accountName,
                transactionId,
                paymentId
            )
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "circuit breaker open")
            }
            return false
        }

        val start = now()

        return try {
            val uri = URI(
                "http://$paymentProviderHostPort/external/process?" + "serviceName=$serviceName&token=$token&accountName=$accountName" + "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
            )
            val request = HttpRequest.newBuilder().uri(uri).timeout(Duration.ofMillis(timeToDrop))
                .POST(HttpRequest.BodyPublishers.noBody()).build()

            val response = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()

            val duration = now() - start

            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.debug(
                    "[{}] [ERROR] Payment processed for txId: {}, payment: {}, result code: {}, reason: {}",
                    accountName,
                    transactionId,
                    paymentId,
                    response.statusCode(),
                    response.body(),
                    e
                )
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            logger.debug(
                "[{}] Payment processed for txId: {}, payment: {}, succeeded: {}, message: {}",
                accountName,
                transactionId,
                paymentId,
                body.result,
                body.message
            )

            if (body.result) {
                circuitBreaker.onSuccess(duration, TimeUnit.MILLISECONDS)
            } else {
                circuitBreaker.onError(
                    duration,
                    TimeUnit.MILLISECONDS,
                    RuntimeException(body.message ?: "payment provider returned false")
                )
            }

            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }

            body.result
        } catch (e: CancellationException) {
            circuitBreaker.releasePermission()
            throw e
        } catch (e: Exception) {
            val duration = now() - start
            circuitBreaker.onError(duration, TimeUnit.MILLISECONDS, e)

            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = e.message)
            }
            false
        } finally {
            val duration = now() - start
            requestLatency(duration.toDouble())
        }
    }

    private fun recordRetryAttempt(attempt: Int, success: Boolean) = meterRegistry.counter(
        "payment_retry_attempts", "attempt", attempt.toString(), "result", if (success) "success" else "failure"
    ).increment()

    fun requestLatency(duration: Double) = requestLatency.record(duration)

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName

    private fun now() = System.currentTimeMillis()
}
