package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
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

    private val timeToDrop = 2000L
    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1)
    )
    private val semaphore = Semaphore(parallelRequests)
    private val paymentScope =
        CoroutineScope(Executors.newFixedThreadPool(100).asCoroutineDispatcher() + SupervisorJob())
    private val httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.debug("[{}] Submitting payment request for payment {}", accountName, paymentId)
        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        paymentScope.launch {
            semaphore.withPermit {
                repeat(repeatTimes) { attempt ->
                    rateLimiter.tickBlocking()

                    val success = sendRequest(transactionId, paymentId, amount, paymentStartedAt)
                    recordRetryAttempt(attempt + 1, success)
                    if (success) return@repeat

                    delay((1L * 2.0.pow((attempt + 1).toDouble())).toLong())
                }
            }
        }
    }

    private suspend fun sendRequest(
        transactionId: UUID, paymentId: UUID, amount: Int, paymentStartedAt: Long
    ): Boolean {
        return try {
            val uri = URI(
                "http://$paymentProviderHostPort/external/process?" + "serviceName=$serviceName&token=$token&accountName=$accountName" + "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
            )
            val request = HttpRequest
                .newBuilder()
                .uri(uri)
                .timeout(Duration.ofMillis(timeToDrop))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build()

            val response = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()

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

            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }

            body.result
        } catch (e: Exception) {
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = e.message)
            }
            false
        } finally {
            val duration = now() - paymentStartedAt
            requestLatency(duration.toDouble())
        }
    }

    private fun recordRetryAttempt(attempt: Int, success: Boolean) = meterRegistry.counter(
        "payment_retry_attempts",
        "attempt",
        attempt.toString(),
        "result",
        if (success) "success" else "failure"
    ).increment()

    fun requestLatency(duration: Double) =
        DistributionSummary
            .builder("request_latency")
            .publishPercentiles(0.9, 0.99, 0.999, 0.9999)
            .register(meterRegistry)
            .record(duration)

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName

    private fun now() = System.currentTimeMillis()
}
