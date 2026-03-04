package org.example;// Импорт классов для работы с Spark DataFrame API

// Импорт классов для работы с Spark DataFrame API
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// Импорт классов для работы со стримингом
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
// Импорт классов для работы с типами данных
import org.apache.spark.sql.types.DataTypes;
// Импорт классов для логирования
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Импорт классов для работы с многопоточностью и таймаутами

import java.util.concurrent.TimeoutException;
import org.apache.spark.api.java.function.MapPartitionsFunction;

// Импорт классов для работы с многопоточностью и таймаутами
import java.sql.Timestamp;
import java.util.ArrayList;

// Статический импорт функций Spark SQL
import static org.apache.spark.sql.functions.*;

// Главный класс приложения-генератора данных
public class DataGenerator {
    // Создаем логгер для этого класса
    private static final Logger logger = LoggerFactory.getLogger(DataGenerator.class);
    // Флаг для отслеживания состояния завершения работы (volatile для visibility между потоками)
    private static volatile boolean isShuttingDown = false;
    // Ссылка на streaming query для управления им из shutdown hook
    private static StreamingQuery query = null;

    // Главный метод приложения
    public static void main(String[] args) {
        // Добавляем обработчик для graceful shutdown (Ctrl+C)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Логируем получение сигнала завершения
            logger.info("Получен сигнал завершения работы...");
            // Устанавливаем флаг завершения работы
            isShuttingDown = true;
            // Проверяем, существует ли streaming query
            if (query != null) {
                try {
                    // Логируем начало остановки query
                    logger.info("Останавливаем streaming query...");
                    // Останавливаем streaming query
                    query.stop();
                    // Даем время для корректного завершения операций
                    Thread.sleep(5000);
                } catch (Exception e) {
                    // Логируем ошибку при остановке
                    logger.error("Ошибка при остановке query: {}", e.getMessage());
                }
            }
        }));

        // Объявляем переменную SparkSession для гарантированного закрытия
        SparkSession spark = null;
        try {
            // Создаем сессию Spark с помощью builder pattern
            spark = SparkSession.builder()
                    // Устанавливаем имя приложения
                    .appName("Data Generator")
                    // Указываем режим выполнения (local)
                    .config("spark.master", "local")
                    // Форсируем удаление временных checkpoint файлов
                    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                    // Создаем или получаем существующую сессию
                    .getOrCreate();

            // Логируем успешное создание сессии
            logger.info("Spark сессия успешно создана");

            // Создаем стриминг Dataset, который генерирует данные
            Dataset<Row> df = spark
                    // Читаем streaming данные
                    .readStream()
                    // Используем источник "rate" для генерации тестовых данных
                    .format("rate")
                    // Устанавливаем скорость генерации - 1 строка в секунду
                    .option("rowsPerSecond", 1)
                    // Загружаем данные
                    .load();

            // Преобразуем данные: добавляем ID датчика и случайное значение температуры
            Dataset<Row> sensorData = df
                    // Добавляем колонку sensorId - случайное число от 0 до 4
                    .withColumn("sensorId", rand(1).multiply(5).cast(DataTypes.IntegerType))
                    // Добавляем колонку temperature - случайное число от 20 до 70
                    .withColumn("temperature", rand(2).multiply(50).plus(20))
                    // Сохраняем колонку timestamp из исходных данных
                    .withColumn("timestamp", col("timestamp"))
                    // Выбираем только нужные колонки
                    .select("sensorId", "temperature", "timestamp");

            // Логируем схему данных
            logger.info("Схема генерируемых данных:");
            // Выводим схему DataFrame в консоль
            sensorData.printSchema();

            // Добавляем кастомный обработчик для логирования отправляемых данных
            Dataset<Row> loggedData = sensorData.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
                // Создаем список для накопления результатов
                ArrayList<Row> result = new ArrayList<>();
                // Итерируем по всем строкам в partition
                while (iterator.hasNext()) {
                    // Получаем следующую строку
                    Row row = iterator.next();
                    // Извлекаем значение sensorId (первая колонка)
                    int sensorId = row.getInt(0);
                    // Извлекаем значение temperature (вторая колонка)
                    double temperature = row.getDouble(1);
                    // Извлекаем значение timestamp (третья колонка)
                    Timestamp timestamp = row.getTimestamp(2);

                    // Логируем каждое отправляемое сообщение с форматированием
                    logger.info("Отправка данных: sensorId={}, temperature={:.2f}, timestamp={}",
                            sensorId, temperature, timestamp);

                    // Добавляем строку в результат
                    result.add(row);
                }
                // Возвращаем итератор по результатам
                return result.iterator();
            }, sensorData.encoder()); // Указываем encoder для сохранения схемы данных

            // Отправляем данные в Kafka с обработкой ошибок
            query = loggedData
                    // Подготавливаем данные для Kafka: преобразуем в key-value пары
                    .selectExpr("cast(sensorId as string) as key",
                            // Создаем value как строку с разделителями-запятыми
                            "cast(sensorId as string) || ',' || cast(temperature as string) || ',' || cast(timestamp as string) as value")
                    // Создаем streaming запись
                    .writeStream()
                    // Указываем формат sink - Kafka
                    .format("kafka")
                    // Указываем адреса Kafka брокеров
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    // Указываем топик для отправки данных
                    .option("topic", "sensor-data")
                    // Указываем location для checkpoint файлов (для восстановления состояния)
                    .option("checkpointLocation", "/tmp/generator-checkpoint")
                    // Добавляем обработку ошибок - количество повторных попыток
                    .option("kafka.retries", "3")
                    // Задержка между повторными попытками
                    .option("kafka.retry.backoff.ms", "1000")
                    // Таймаут для запросов к Kafka
                    .option("kafka.request.timeout.ms", "30000")
                    // Общий таймаут для доставки сообщения
                    .option("kafka.delivery.timeout.ms", "60000")
                    // Запускаем streaming query
                    .start();

            // Логируем успешный запуск streaming query
            logger.info("Streaming query запущен. ID: {}", query.id());
            // Информируем пользователя о способе остановки
            logger.info("Для остановки нажмите Ctrl+C");

            // Мониторинг состояния стриминга с обработкой исключений
            monitorStreamingQuery(query);

        } catch (Exception e) {
            // Логируем критические ошибки с stack trace
            logger.error("Критическая ошибка в приложении: {}", e.getMessage(), e);
            // Завершаем приложение с ошибкой
            System.exit(1);
        } finally {
            // Гарантированное закрытие ресурсов в блоке finally
            if (spark != null && !isShuttingDown) {
                // Логируем закрытие сессии
                logger.info("Закрываем Spark сессию...");
                // Закрываем Spark сессию
                spark.close();
            }
        }
    }

    /**
     * Мониторинг streaming query с обработкой исключений
     */
    private static void monitorStreamingQuery(StreamingQuery query) {
        try {
            // Ожидаем завершения с таймаутом для проверки состояния
            while (!query.awaitTermination(5000)) {
                // Проверяем, не запрошена ли остановка приложения
                if (isShuttingDown) {
                    // Логируем запрос на остановку
                    logger.info("Запрошена остановка приложения...");
                    // Выходим из цикла мониторинга
                    break;
                }

                // Логируем статус стриминга (debug уровень для частых сообщений)
                logger.debug("Статус стриминга: isActive={}, status={}",
                        query.isActive(), query.status());

                // Проверяем наличие ошибок в streaming query
                if (query.exception().isDefined()) {
                    // Получаем исключение
                    Exception exception = query.exception().get();
                    // Логируем ошибку
                    logger.error("Ошибка в streaming query: {}", exception.getMessage(), exception);

                    // В зависимости от типа ошибки можно предпринять разные действия
                    if (exception instanceof TimeoutException) {
                        // Логируем таймаут как предупреждение
                        logger.warn("Таймаут подключения к Kafka. Продолжаем попытки...");
                    } else if (exception instanceof org.apache.kafka.common.errors.TimeoutException) {
                        // Логируем таймаут Kafka как ошибку
                        logger.error("Таймаут Kafka. Проверьте доступность Kafka брокера.");
                    }
                }
            }

            // Логируем успешное завершение streaming query
            logger.info("Streaming query завершен корректно");

        } catch (StreamingQueryException e) {
            // Обрабатываем исключения streaming query
            if (!isShuttingDown) {
                // Логируем как ошибку, если это не graceful shutdown
                logger.error("Ошибка при ожидании завершения streaming query: {}", e.getMessage(), e);
            } else {
                // Логируем как информационное сообщение при graceful shutdown
                logger.info("Streaming query остановлен по запросу пользователя");
            }
        } catch (Exception e) {
            // Обрабатываем все остальные исключения
            logger.error("Неожиданная ошибка при мониторинге streaming query: {}", e.getMessage(), e);
        }
    }
}