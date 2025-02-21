package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Реализация ограничителя частоты запросов (Rate Limiter) на основе алгоритма скользящего окна (Sliding Window).
 *
 * Этот класс позволяет ограничивать количество запросов в заданном временном окне.
 * Хранит временные метки запросов и удаляет устаревшие записи по мере их поступления.
 *
 * @param rate Максимально допустимое количество запросов в пределах временного окна.
 * @param window Продолжительность временного окна, в течение которого учитываются запросы.
 */
class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    /**
     * CoroutineScope с выделенным потоком для управления асинхронной обработкой запросов.
     * Использует [Executors.newSingleThreadExecutor] для выполнения задач в отдельном потоке.
     */
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())


    /**
     * Общее количество запросов, зарегистрированных в окне.
     * Используется для отслеживания текущей нагрузки и проверки лимита.
     * Потокобезопасный счетчик, обновляемый атомарно.
     */
    private val sum = AtomicLong(0)

    /**
     * Очередь запросов с приоритетом по времени.
     * Хранит временные метки запросов для определения их актуальности в окне.
     * Ограничена 10 000 элементами для контроля памяти.
     */
    private val queue = PriorityBlockingQueue<Measure>(10_000)


    private val mutex = ReentrantLock()

    /**
     * Пытается выполнить операцию, проверяя лимит запросов в заданном временном окне.
     *
     * - Если текущее количество запросов (`sum`) превышает допустимый лимит (`rate`), возвращает `false`.
     * - В противном случае добавляет новую запись (`Measure`) с текущим временем в `queue`,
     *   увеличивает счетчик `sum` и возвращает `true`.
     *
     * @return `true`, если запрос был успешно обработан (не превышен лимит), иначе `false`.
     */
    override fun tick(): Boolean {
        if (sum.get() > rate) {
            return false
        } else {
            if (sum.get() <= rate) {
                queue.add(Measure(1, System.currentTimeMillis()))
                sum.incrementAndGet()
                return true
            } else return false
        }
    }


//    override fun tick(): Boolean {
//        if (sum.get() >= rate) {
//            return false
//        }
//        queue.add(Measure(1, System.currentTimeMillis()))
//        sum.incrementAndGet()
//        return true
//    }


    /**
     * Блокирующий вызов метода tick(), выполняющий попытки до успешного выполнения.
     * Если tick() возвращает false, поток засыпает на 10 мс и повторяет попытку.
     * Используется для ожидания доступного лимита в rate limiter.
     */
    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10)
        }
    }

    data class Measure(
        val value: Long,
        val timestamp: Long
    ) : Comparable<Measure> {
        override fun compareTo(other: Measure): Int {
            return timestamp.compareTo(other.timestamp)
        }
    }

    /**
     * Фоновая корутина, выполняющая удаление устаревших запросов из очереди.
     *
     * Эта задача запускается внутри `rateLimiterScope` и выполняется в бесконечном цикле.
     * - Извлекает первый элемент из `queue` (если он есть).
     * - Проверяет, выходит ли его временная метка за границы временного окна (`window`).
     * - Если элемент устарел, удаляет его из `queue` и уменьшает `sum`.
     * - В случае завершения корутины с ошибкой, логирует исключение.
     */
    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            val head = queue.peek()
            val winStart = System.currentTimeMillis() - window.toMillis()
            if (head == null || head.timestamp > winStart) {
                continue
            }
            sum.addAndGet(-1)
            queue.take()
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }
}