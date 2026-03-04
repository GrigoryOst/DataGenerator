package org.example;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import java.util.ArrayList;
import java.util.List;
// Импорт классов для работы с Spark DataFrame API
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// Импорт классов для работы со стримингом
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
// Импорт классов для логирования
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Импорт классов для работы с многопоточностью
import java.util.concurrent.TimeoutException;

// Статический импорт функций Spark SQL
import static org.apache.spark.sql.functions.*;

// Класс преобразователя данных для чтения из Kafka и записи в HDFS
public class DataTransformer {
    // Создаем логгер для этого класса
    private static final Logger logger = LoggerFactory.getLogger(DataTransformer.class);
    // Флаг для отслеживания состояния завершения работы
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
                    // Устанавливаем имя приложения (отображается в Spark UI и логах)
                    .appName("Data Transformer")
                    // Указываем режим выполнения - локальный (без кластера)
                    .config("spark.master", "local")
                    // Форсируем удаление временных checkpoint файлов при перезапуске
                    // Это помогает избежать ошибок "checkpoint already exists"
                    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                    // === КОНФИГУРАЦИЯ WEBHDFS ===
                    // Указываем протокол и адрес для подключения к HDFS через WebHDFS
                    .config("spark.hadoop.fs.defaultFS", "webhdfs://localhost:9870")
                    // Заставляем клиент использовать hostname datanode вместо IP
                    // Решает проблемы с разрешением имен в Docker сети
                    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
                    // Устанавливаем фактор репликации данных (количество копий)
                    // 1 - минимальное значение для тестовой среды
                    .config("spark.hadoop.dfs.replication", "1")
                    // Отключаем проверку прав доступа для упрощения разработки
                    // В продакшене должно быть включено!
                    .config("spark.hadoop.dfs.permissions.enabled", "false")
                    // Указываем пользователя для аутентификации в WebHDFS
                    // "root" - пользователь с максимальными правами
                    .config("spark.hadoop.dfs.webhdfs.user", "root")
                    // Отключаем Kerberos аутентификацию (пустая строка)
                    // Kerberos не используется в нашей тестовой среде
                    .config("spark.hadoop.dfs.web.authentication.kerberos.principal", "")
                    // Отключаем OAuth2 аутентификацию (пустая строка)
                    // OAuth2 не используется в нашей конфигурации
                    .config("spark.hadoop.dfs.webhdfs.oauth2.access.token.provider", "")
                    // Создаем или получаем существующую Spark сессию
                    // Если сессия с такими настройками уже существует - переиспользуем ее
                    .getOrCreate();

            // Логируем успешное создание сессии
            logger.info("Spark сессия успешно создана");

            // Читаем поток данных из Kafka
            Dataset<Row> rawData = spark
                    // Читаем streaming данные
                    .readStream()
                    // Используем формат Kafka
                    .format("kafka")
                    // Указываем адреса Kafka брокеров
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    // Указываем топик для подписки
                    .option("subscribe", "sensor-data")
                    // Начинаем чтение с последних сообщений
                    .option("startingOffsets", "latest")
                    // Загружаем данные
                    .load();

            // Логируем успешное подключение к Kafka
            logger.info("Подключение к Kafka установлено");

            // Парсим строку из Kafka в отдельные колонки
            Dataset<Row> parsedData = rawData
                    // Разбиваем value по запятым на массив
                    .selectExpr("split(CAST(value AS STRING), ',') as data")
                    // Преобразуем элементы массива в отдельные колонки с правильными типами
                    .selectExpr(
                            "CAST(data[0] AS INT) as sensorId",
                            "CAST(data[1] AS DOUBLE) as temperature",
                            "CAST(data[2] AS TIMESTAMP) as timestamp"
                    );

            // Логируем схему распарсенных данных
            logger.info("Схема распарсенных данных:");
            parsedData.printSchema();

            // Добавляем кастомный обработчик для логирования обрабатываемых данных
            Dataset<Row> loggedData = parsedData.mapPartitions(
                    (MapPartitionsFunction<Row, Row>) iterator -> {
                        // Создаем список для накопления результатов
                        List<Row> result = new ArrayList<>();
                        // Итерируем по всем строкам в partition
                        while (iterator.hasNext()) {
                            // Получаем следующую строку
                            Row row = iterator.next();
                            // Извлекаем значение sensorId (первая колонка)
                            int sensorId = row.getInt(0);
                            // Извлекаем значение temperature (вторая колонка)
                            double temperature = row.getDouble(1);
                            // Извлекаем значение timestamp (третья колонка)
                            java.sql.Timestamp timestamp = row.getTimestamp(2);

                            // Логируем каждое обрабатываемое сообщение
                            logger.info("Обработка данных: sensorId={}, temperature={:.2f}, timestamp={}",
                                    sensorId, temperature, timestamp);

                            // Добавляем строку в результат
                            result.add(row);
                        }
                        // Возвращаем итератор по результатам
                        return result.iterator();
                    },
                    parsedData.encoder() // Указываем encoder для сохранения схемы данных
            );

            // Применяем преобразование: округляем температуру до одного знака
            Dataset<Row> transformedData = loggedData
                    // Добавляем новую колонку с округленной температурой
                    .withColumn("roundedTemperature", round(col("temperature"), 1))
                    // Выбираем все колонки включая новую
                    .select("sensorId", "temperature", "roundedTemperature", "timestamp");

            // Логируем применение преобразования
            logger.info("Преобразование данных применено - температура округлена до одного знака");

            // Записываем результат в HDFS в формате Parquet :cite[8]
            query = transformedData
                    .writeStream()
                    .format("json")
                    .option("path", "file:///tmp/sensor_data_output")
                    .option("checkpointLocation", "file:///tmp/transformer-checkpoint")
                    .start();

            // Логируем успешный запуск streaming query
            logger.info("Streaming query запущен. ID: {}", query.id());
            logger.info("Данные записываются в локальную файловую систему: file:///tmp/sensor_data_output");
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

                // Проверяем наличие ошибок в streaming query :cite[1]
                if (query.exception().isDefined()) {
                    // Получаем исключение
                    Exception exception = query.exception().get();
                    // Логируем ошибку
                    logger.error("Ошибка в streaming query: {}", exception.getMessage(), exception);

                    // В зависимости от типа ошибки можно предпринять разные действия
                    if (exception instanceof TimeoutException) {
                        // Логируем таймаут как предупреждение
                        logger.warn("Таймаут подключения к HDFS. Продолжаем попытки...");
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
