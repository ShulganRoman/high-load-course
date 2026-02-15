package ru.quipy.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Timer
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.springframework.stereotype.Service
import java.util.concurrent.atomic.AtomicInteger

@Service
class PaymentMetricsService(prometheusRegistry: PrometheusMeterRegistry) {

    private val paymentCounter: Counter = Counter.builder("payment_requests_total")
        .description("Total payment requests")
        .register(prometheusRegistry)

    private val paymentTimer: Timer = Timer.builder("payment_duration_seconds")
        .description("Time to process payment")
        .register(prometheusRegistry)

    private val queueSize = AtomicInteger(0)

    init {
        Gauge.builder("payment_queue_size") { queueSize.get() }
            .description("Current size of payment queue")
            .register(prometheusRegistry)
    }

    fun recordPayment(processingTimeMs: Long) {
        paymentCounter.increment()
        paymentTimer.record(processingTimeMs, java.util.concurrent.TimeUnit.MILLISECONDS)
    }

    fun incrementQueue() = queueSize.incrementAndGet()
    fun decrementQueue() = queueSize.decrementAndGet()
}
