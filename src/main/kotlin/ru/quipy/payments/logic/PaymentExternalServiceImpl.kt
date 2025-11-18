package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val semaphore = Semaphore(parallelRequests)
    private val channel = Channel<suspend () -> Unit>(capacity = 500)
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val client = OkHttpClient.Builder()
        .connectTimeout(25, TimeUnit.SECONDS)
        .readTimeout(25, TimeUnit.SECONDS)
        .writeTimeout(25, TimeUnit.SECONDS)
        .build()
    private val rateLimiter =
        SlidingWindowRateLimiter(
            rateLimitPerSec.toLong(),
            Duration.ofSeconds(1),
        )

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        scope.launch {
            val transactionId = UUID.randomUUID()
            if (!rateLimiter.tick()) {
                logger.warn("[$accountName] rate limit overflow for txId: $transactionId")
                rateLimiter.tickBlocking();
            }
            logger.info("[$accountName] Submitting payment request for payment $paymentId")
            paymentESService.update(paymentId) {
                it.logSubmission(
                    success = true,
                    transactionId,
                    now(),
                    Duration.ofMillis(now() - paymentStartedAt)
                )
            }
            logger.info("[$accountName] Submit: $paymentId, time: ${Date(now())}, txId: $transactionId")
            job(paymentId, transactionId, amount)
        }
    }

    suspend fun job(paymentId: UUID, transactionId: UUID, amount: Int) {
        semaphore.withPermit {
            try {
                val request = Request.Builder()
                    .run {
                        url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                        post(emptyBody)
                    }
                    .build()

                client.newCall(request).execute().use { response ->
                    val rowBody = response.body?.string()

                    val body = try {
                        mapper.readValue(rowBody, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $rowBody")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }
                    logger.info("[$accountName] Payment ${requestAverageProcessingTime} processed for txId: $transactionId, payment: $paymentId, time: ${Date(now())}, succeeded: ${body.result}, message: ${body.message}")
                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(
                            body.result,
                            now(),
                            transactionId,
                            body.message
                        )
                    }
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(
                                false,
                                now(),
                                transactionId,
                                reason = "Request timeout."
                            )
                        }
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(
                                false,
                                now(),
                                transactionId,
                                reason = e.message
                            )
                        }
                    }
                }
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()
