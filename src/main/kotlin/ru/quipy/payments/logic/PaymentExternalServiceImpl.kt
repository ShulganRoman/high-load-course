package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.delay
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val repeatTimes = 4
    private val timeToDrop = 3000L

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1),
    )

//    val semaphore: Semaphore = Semaphore(parallelRequests)

    private val client = OkHttpClient.Builder().callTimeout(timeToDrop, TimeUnit.MILLISECONDS).build()
    private var orderMap = HashMap<UUID, Long>()

    override fun performPaymentAsync(
        paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long
    ) {
//        CoroutineScope(Dispatchers.IO).launch {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        for (attempt in 1..repeatTimes) {
            val success = sendRequest(transactionId, paymentId, amount, paymentStartedAt)

            recordRetryAttempt(attempt, success)
            if (success) return

            val currentDeadline = orderMap.getOrDefault(paymentId, 0L)
            if (currentDeadline > 0) {
                Thread.sleep(currentDeadline)
            }
            orderMap[paymentId] = currentDeadline + requestAverageProcessingTime.toMillis()
        }
//        }
    }

    fun sendRequest(
        transactionId: UUID, paymentId: UUID, amount: Int, paymentStartedAt: Long
    ): Boolean {
        rateLimiter.tickBlocking()

        try {
            val request = Request.Builder().url(
                "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
            ).post(emptyBody).build()

            client.newCall(request).execute().use { response ->
                val raw = response.body?.string()

                val body = try {
                    mapper.readValue(raw, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error(
                        "[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $raw",
                        e
                    )
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn(
                    "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}"
                )

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }

                return body.result
            }

        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId")
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            val diff = now() - paymentStartedAt
            requestLatency(diff.toDouble())
        }

        return false
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName

    fun recordRetryAttempt(attempt: Int, success: Boolean) = meterRegistry
        .counter(
            "payment_retry_attempts",
            "attempt",
            attempt.toString(),
            "result", if (success) "success" else "failure"
        )
        .increment()

    fun requestLatency(duration: Double) = DistributionSummary
        .builder("request_latency")
        .publishPercentiles(0.9, 0.99, 0.999, 0.9999)
        .register(meterRegistry)
        .record(duration)
}

fun now() = System.currentTimeMillis()
