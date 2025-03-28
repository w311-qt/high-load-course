package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.HdrHistogram.Histogram
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

// Интерцептор для динамического изменения таймаутов в зависимости от текущих условий
class DynamicTimeoutInterceptor(private val getTimeout: () -> Long) : Interceptor {
    override fun intercept(chain: Interceptor.Chain): Response {
        val timeout = getTimeout().toInt()
        logger.warn("NEW TIMEOUT IS ${timeout}")

        // Установка таймаутов для всех типов соединений
        val newChain = chain
            .withConnectTimeout(timeout, TimeUnit.MILLISECONDS)
            .withReadTimeout(timeout, TimeUnit.MILLISECONDS)
            .withWriteTimeout(timeout, TimeUnit.MILLISECONDS)

        return newChain.proceed(chain.request())
    }
}

// Класс для работы с внешней платёжной системой, поддерживает асинхронную обработку платежей
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule() // Регистрация модуля для работы с Kotlin в Jackson
    }

    // Инициализация переменных из конфигурации
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val retryCount = 3

    private var currentTimeout90thPercentile: Duration = Duration.ofMillis(requestAverageProcessingTime.toMillis() * 2)
    private var highestTrackableValue = requestAverageProcessingTime.toMillis() * 2
    private val histogram = Histogram(requestAverageProcessingTime.toMillis(), highestTrackableValue, 2)

    private val client = OkHttpClient.Builder()
        .addInterceptor(DynamicTimeoutInterceptor { currentTimeout90thPercentile.toMillis() }) // Динамический интерцептор
        .build()

    // Лимитатор запросов для соблюдения ограничения по скорости
    private val rateLimiterBucket = TokenBucketRateLimiter(
        rateLimitPerSec,
        window = requestAverageProcessingTime.toMillis(),
        bucketMaxCapacity = rateLimitPerSec,
        timeUnit = TimeUnit.MILLISECONDS
    )

    private val semaphore = Semaphore(parallelRequests) // Семафор для ограничения параллельных запросов

    val baseDelay = 200L // Начальная задержка в миллисекундах
    val maxDelay = 1000L // Максимальная задержка 1 секунда

    private val threadPool: ThreadPoolExecutor = Executors.newFixedThreadPool(parallelRequests) as ThreadPoolExecutor

    // Асинхронная обработка платежа
    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Обновление статуса платежа в системе, независимо от результата
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val url = "http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&" +
                "transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=${currentTimeout90thPercentile}"

        val request = Request.Builder()
            .url(url)
            .post(emptyBody)
            .build()

        val startTime = System.currentTimeMillis()

        threadPool.submit {
            try {
                semaphore.acquire() // Захват семафора для ограничения параллельных запросов

                // Ожидание свободного места в лимитаторе скорости
                while (!rateLimiterBucket.tick()) {
                    Thread.sleep(10)
                }

                val attempt = AtomicInteger(1)
                var isSuccess = false

                // Попытки выполнения запроса с экспоненциальной задержкой в случае ошибок
                while (attempt.incrementAndGet() <= retryCount) {
                    if (now() + currentTimeout90thPercentile.toMillis() > deadline) {
                        logger.error("[$accountName] Deadline exceeded, payment $paymentId aborted.")
                        break
                    }

                    client.newCall(request).execute().use { response ->
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}, response code: ${response.code}")

                        // Обновление статуса оплаты в системе
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }

                        // Обработка различных кодов ответа
                        when (response.code) {
                            429 -> handleRateLimit(response, attempt, deadline) // Обработка 429 Too Many Requests
                            500, 502, 503, 504 -> handleServerError(response, attempt, deadline) // Серверные ошибки
                            400, 401, 403, 404, 405, 408 -> {
                                logger.error("[$accountName] Error ${response.code}. Aborted")
                                return@submit // Прекращаем ретраи для ошибок клиента
                            }
                        }

                        isSuccess = body.result
                    }

                    if (isSuccess) break

                    // Если все попытки не удались
                    if (attempt.get() >= retryCount) {
                        logger.error("[$accountName] Payment $paymentId failed after $retryCount tries.")
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Max retries exceeded.")
                        }
                    }
                }
            } catch (e: Exception) {
                handleError(e, paymentId, transactionId) // Обработка ошибок
            } finally {
                // Фиксация времени и обновление данных о времени ожидания
                val duration = System.currentTimeMillis() - startTime
                histogram.recordValue(duration)

                currentTimeout90thPercentile = Duration.ofMillis(
                    minOf(histogram.getValueAtPercentile(90.0), highestTrackableValue)
                )
                logger.info("[$accountName] Updated 90th percentile timeout: $currentTimeout90thPercentile ms")
                semaphore.release() // Освобождение семафора
            }
        }
    }

    // Обработка ошибки 429 (слишком много запросов)
    private fun handleRateLimit(response: Response, attempt: AtomicInteger, deadline: Long) {
        val retryAfterHeader = response.header("Retry-After")
        val retryDelay = retryAfterHeader?.toLongOrNull() ?: (100 * attempt.get()).toLong()

        val timeLeft = deadline - now()
        if (timeLeft > retryDelay) {
            attempt.decrementAndGet()
            logger.warn("[$accountName] 429 Too Many Requests. Retry request in $retryDelay ms.")
            Thread.sleep(retryDelay)
        } else {
            logger.warn("[$accountName] Not enough time to retry after 429. Abort")
        }
    }

    // Обработка серверных ошибок (500, 502, 503, 504)
    private fun handleServerError(response: Response, attempt: AtomicInteger, deadline: Long) {
        val retryDelay = minOf(baseDelay * (1 shl (attempt.get() - 1)), maxDelay)
        val timeLeft = deadline - now()

        if (timeLeft > retryDelay) {
            logger.warn("[$accountName] Error ${response.code}, retry in $retryDelay ms.")
            Thread.sleep(retryDelay)
        } else {
            logger.warn("[$accountName] Not enough time to retry after ${response.code}. Abort ")
        }
    }

    // Обработка исключений
    private fun handleError(e: Exception, paymentId: UUID, transactionId: UUID) {
        when (e) {
            is SocketTimeoutException -> {
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
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
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()
