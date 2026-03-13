package ru.quipy.common.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class CallerBlockingRejectedExecutionHandler(
    private val maxWait: Duration = Duration.ofSeconds(1),
) : RejectedExecutionHandler {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(CallerBlockingRejectedExecutionHandler::class.java)
    }

    // Even if event is rejected we will still keep it, trying to put in queue so that not to lose it!
    override fun rejectedExecution(r: Runnable, executor: ThreadPoolExecutor) {
        if (executor.isShutdown) {
            throw RejectedExecutionException("Executor has been shut down")
        }

        try {
            val queue = executor.queue
            val offer = queue.offer(r, maxWait.toMillis(), TimeUnit.MILLISECONDS)
            if (!offer) {
                // Fallback to caller thread to apply backpressure instead of dropping requests.
                logger.debug("Queue is full after waiting {} ms, running task in caller thread", maxWait.toMillis())
                r.run()
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw RejectedExecutionException("Interrupted", e)
        }
    }
}
