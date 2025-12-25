package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        private const val POOL_SIZE = 16
        private const val MAX_BACKLOG = 480
        private const val MAX_QUEUE_TO_EXPIRED = 266
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private class PrioritizedTask(
        val createdAt: Long,
        private val task: Runnable
    ) : Runnable {
        override fun run() = task.run()
    }

    private val paymentExecutor = ThreadPoolExecutor(
        POOL_SIZE,
        POOL_SIZE,
        0L,
        TimeUnit.MILLISECONDS,
        PriorityBlockingQueue(MAX_BACKLOG) { a, b ->
            val ta = a as PrioritizedTask
            val tb = b as PrioritizedTask
            ta.createdAt.compareTo(tb.createdAt)
        },
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    @OptIn(ExperimentalAtomicApi::class)
    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        if (paymentExecutor.queue.size > MAX_QUEUE_TO_EXPIRED) {
            return -1L
        }

        val runnable = Runnable {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }

            logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)
            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }

        val task = PrioritizedTask(createdAt, runnable)

        paymentExecutor.execute(task)

        return createdAt
    }
}
