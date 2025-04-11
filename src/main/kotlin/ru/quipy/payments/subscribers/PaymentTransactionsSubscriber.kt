package ru.quipy.payments.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

/**
 * Сервис, подписывающийся на события обработки платежей и логирующий их.
 * Использует батчинг и автоматическую очистку старых логов.
 */
@Service
class PaymentTransactionsSubscriber {

    companion object {
        // Ограничение на размер логов
        private const val MAX_LOG_ENTRIES = 100_000

        // Интервал очистки логов (в минутах)
        private const val CLEANUP_INTERVAL_MINUTES = 60L
    }

    // Логгер для отладочной информации и предупреждений
    val logger: Logger = LoggerFactory.getLogger(PaymentTransactionsSubscriber::class.java)

    // Хранилище логов: ключ — UUID транзакции, значение — очередь логов по этой транзакции
    private val paymentLog: ConcurrentHashMap<UUID, ConcurrentLinkedQueue<PaymentLogRecord>> = ConcurrentHashMap()

    // Очередь для батчинга событий
    private val batchQueue = LinkedBlockingQueue<PaymentLogRecord>()

    // Флаг для управления фоновыми потоками
    private var isRunning = true

    // Менеджер подписок на агрегаты
    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    /**
     * Инициализация подписки и запуск фоновых потоков
     */
    @PostConstruct
    fun init() {
        startBatchProcessor()
        startCleanupScheduler()

        subscriptionsManager.createSubscriber(
            PaymentAggregate::class,
            "payments:payment-processings-subscriber", // Уникальный ID подписчика
            retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT) // Пропуск события после одной неудачной попытки
        ) {
            // Обработка события PaymentProcessedEvent
            `when`(PaymentProcessedEvent::class) { event ->
                val record = PaymentLogRecord(
                    event.processedAt,
                    status = if (event.success) PaymentStatus.SUCCESS else PaymentStatus.FAILED,
                    event.amount,
                    event.paymentId
                )
                // Добавляем запись в очередь для асинхронной батч-обработки
                batchQueue.put(record)
            }
        }
    }

    /**
     * Запуск фонового потока для батч-обработки событий
     */
    private fun startBatchProcessor() = Thread({
        val batch = mutableListOf<PaymentLogRecord>()
        while (isRunning) {
            try {
                val record = batchQueue.poll(100, TimeUnit.MILLISECONDS) ?: continue
                batch.add(record)

                if (batch.size >= 1000) {
                    processBatch(batch)
                    batch.clear()
                }
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                break
            }
        }

        // Обработка оставшихся записей после завершения
        if (batch.isNotEmpty()) processBatch(batch)
    }, "payment-log-batcher").start()

    /**
     * Сохранение батча логов в основное хранилище
     */
    private fun processBatch(batch: List<PaymentLogRecord>) {
        if (paymentLog.size >= MAX_LOG_ENTRIES) {
            paymentLog.clear()
            logger.warn("Payment log cleared due to size limit")
        }

        batch.forEach { record ->
            paymentLog
                .computeIfAbsent(record.transactionId) { ConcurrentLinkedQueue() }
                .add(record)
        }
    }

    /**
     * Запуск потока очистки старых логов
     */
    private fun startCleanupScheduler() = Thread({
        while (isRunning) {
            try {
                Thread.sleep(CLEANUP_INTERVAL_MINUTES * 60 * 1000)
                cleanupOldRecords()
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                break
            }
        }
    }, "payment-log-cleaner").start()

    /**
     * Удаление записей старше 1 часа
     */
    private fun cleanupOldRecords() {
        val cutoff = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)
        paymentLog.forEach { (id, records) ->
            records.removeIf { it.timestamp < cutoff }
            if (records.isEmpty()) paymentLog.remove(id)
        }
    }

    /**
     * Остановка фоновых потоков при завершении работы сервиса
     */
    @PreDestroy
    fun shutdown() {
        isRunning = false
    }

    /**
     * Класс лог-записи одной транзакции
     */
    class PaymentLogRecord(
        val timestamp: Long,           // Время события
        val status: PaymentStatus,     // Статус (успех / ошибка)
        val amount: Int,               // Сумма
        val transactionId: UUID,       // ID транзакции
    )

    /**
     * Перечисление возможных статусов платежа
     */
    enum class PaymentStatus {
        FAILED,
        SUCCESS
    }
}
