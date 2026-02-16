package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        private const val POOL_SIZE = 10
        private const val MAX_BACKLOG = 5_000
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        POOL_SIZE,
        POOL_SIZE,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(MAX_BACKLOG),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler(Duration.ofSeconds(0))
    )

    @OptIn(ExperimentalAtomicApi::class)
    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        return try {
            paymentExecutor.submit {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId, orderId, amount
                    )
                }

                logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)
                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            }
            createdAt
        } catch (e: Exception) {
            when (e) {
                is RejectedExecutionException -> {
                    logger.error("Queue full or executor shutdown, payment rejected for $paymentId")
                    -1L
                }

                else -> {
                    throw e
                }
            }
        }
    }
}
