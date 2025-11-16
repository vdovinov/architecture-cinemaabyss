import os
import asyncio
import json
import logging
from typing import Dict, Any, List
from contextlib import asynccontextmanager # Добавляем импорт

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация Kafka
#KAFKA_BOOTSTRAP_SERVERS = "cinemaabyss-kafka:9092"
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").rstrip("/")
logger.info(f"KAFKA_BROKERS: {KAFKA_BROKERS}")

# Определяем список всех топиков, которые мы хотим читать
TOPICS_TO_SUBSCRIBE = ["movie-events", "user-events", "payment-events"]
#KAFKA_TOPIC = "movie-events"

# Глобальные переменные для продюсера
producer: AIOKafkaProducer | None = None

# Pydantic модель для события
class Event(BaseModel):
    message: str
    data: Dict[str, Any]

# Pydantic модель для данных фильма (из API-спецификации)
class MovieEvent(BaseModel):
    movie_id: int = Field(..., example=1, description="Идентификатор фильма")
    title: str = Field(..., example="Inception", description="Название фильма")
    action: str = Field(..., example="viewed", description="Действие с фильмом")
    user_id: int | None = Field(None, example=1, description="Идентификатор пользователя (опционально)")
    rating: float | None = Field(None, example=8.5, description="Рейтинг (опционально)")
    genres: List[str] | None = Field(None, example=["Sci-Fi", "Action"], description="Жанры фильма (опционально)")
    description: str | None = Field(None, example="A mind-bending thriller", description="Описание фильма (опционально)")

# Pydantic модель для данных пользователя (из API-спецификации)
class UserEvent(BaseModel):
    user_id: int = Field(..., example=1, description="Идентификатор пользователя")
    username: str | None = Field(None, example="john_doe", description="Имя пользователя (опционально)")
    email: str | None = Field(None, example="john.doe@example.com", description="Email пользователя (опционально)")
    action: str = Field(..., example="registered", description="Действие пользователя")
    timestamp: str = Field(..., example="2023-01-15T14:30:00Z", description="Время события")

# Pydantic модель для данных платежа (из API-спецификации)
class PaymentEvent(BaseModel):
    payment_id: int = Field(..., example=1, description="Идентификатор платежа")
    user_id: int = Field(..., example=1, description="Идентификатор пользователя")
    amount: float = Field(..., example=9.99, description="Сумма платежа")
    status: str = Field(..., example="completed", description="Статус платежа")
    timestamp: str = Field(..., example="2023-01-15T14:30:00Z", description="Время платежа")
    method_type: str | None = Field(None, example="credit_card", description="Тип метода оплаты (опционально)")

# Pydantic модель для ответа сервера (из API-спецификации)
class EventResponse(BaseModel):
    status: str = Field(..., example="success", description="Статус операции")
    partition: int = Field(..., example=0, description="Партиция Kafka")
    offset: int = Field(..., example=42, description="Смещение в партиции Kafka")
    event: Event # Включаем модель Event

# --- Функции Kafka ---

async def get_producer():
    """Возвращает асинхронного продюсера."""
    global producer
    if producer is None:
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKERS)
        await producer.start()
        logger.info("await producer.start() OK")
    return producer

async def consume_messages(topics_list: list[str]):
    """Асинхронный консьюмер, который читает сообщения из топика."""
    consumer = AIOKafkaConsumer(
        *topics_list,
        bootstrap_servers=KAFKA_BROKERS,
        group_id="event-consumer-group",
        auto_offset_reset="earliest"
    )
    logger.info(f"Запуск консьюмера для топиков {', '.join(topics_list)}")
    await consumer.start()
    try:
        async for msg in consumer:
            message_body = msg.value.decode("utf-8")
            logger.info(
                f"Получено сообщение: {message_body} "
                f"(тема: {msg.topic}, смещение: {msg.offset})"
            )
    except asyncio.CancelledError:
        logger.info("Консьюмер был отменён.")
    finally:
        await consumer.stop()
        logger.info("Консьюмер остановлен.")


# Вспомогательная функция для публикации в Kafka и обработки ответа
async def _publish_to_kafka(topic: str, message_text: str, data_model: BaseModel):
    try:
        producer = await get_producer()

        event_data = Event(
            message=message_text,
            data=data_model.model_dump(exclude_unset=True)
        )

        message_json = json.dumps(event_data.model_dump()).encode("utf-8")
        metadata = await producer.send_and_wait(topic, message_json)

        return EventResponse(
            status="success",
            partition=metadata.partition,
            offset=metadata.offset,
            event=event_data
        )

    except Exception as e:
        logger.error(f"Ошибка при публикации события: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to publish event"
        )

# --- Обработчик жизненного цикла (Lifespan) ---
@asynccontextmanager
async def lifespan(_app: FastAPI):
    """
    Обрабатывает события запуска и завершения приложения.
    """
    # Этот код выполняется при запуске приложения (Startup)
    asyncio.create_task(consume_messages(TOPICS_TO_SUBSCRIBE))
    logger.info("Микросервис 'events' запущен.")
    yield
    # Этот код выполняется при завершении приложения (Shutdown)
    global producer
    if producer is not None:
        await producer.stop()
        logger.info("Продюсер Kafka остановлен.")

# FastAPI приложение с обработчиком lifespan
app = FastAPI(title="Events Microservice", lifespan=lifespan)

# --- API-эндпоинты ---

@app.post("/events/publish")
async def publish_event(event: Event):
    """
    Публикует новое событие в топик Kafka.
    """
    return await _publish_to_kafka(
        "movie-events",
        message_text=event.message,
        data_model=event
    )

# Метод для проверки работоспособности
@app.get("/api/events/health",
    summary="Проверка работоспособности микросервиса событий",
    tags=["health"],
    operation_id="getEventsServiceHealth",
    response_model=Dict[str, bool] # Явно указываем тип ответа для схемы
)
async def get_health_status():
    """
    Возвращает статус работоспособности микросервиса событий.
    """
    logger.info("Проверка работоспособности микросервиса событий")
    return {"status": True}

# эндпоинт для фильмов
@app.post(
    "/api/events/movie",
    summary="Создание события фильма",
    description="Регистрирует новое событие, связанное с фильмом и публикует его в Kafka",
    tags=["events"],
    operation_id="createMovieEvent",
    response_model=EventResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        400: {"description": "Некорректный запрос"},
        500: {"description": "Внутренняя ошибка сервера"},
    }
)
async def create_movie_event(movie_event: MovieEvent):
    """
    Принимает данные о событии фильма, форматирует их и отправляет в Kafka.
    """
    return await _publish_to_kafka(
        topic="movie-events",
        message_text=f"Movie action: {movie_event.action} for movie {movie_event.movie_id}",
        data_model=movie_event
    )


# эндпоинт для пользователей
@app.post(
    "/api/events/user",
    summary="Создание события пользователя",
    description="Регистрирует новое событие, связанное с пользователем (регистрация, логин и т.д.)",
    tags=["events", "user"],
    operation_id="createUserEvent",
    response_model=EventResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_user_event(user_event: UserEvent):
    """
    Принимает данные о событии пользователя, форматирует их и отправляет в Kafka.
    """
    return await _publish_to_kafka(
        topic="user-events",
        message_text=f"User action: {user_event.action} for user {user_event.user_id}",
        data_model=user_event
    )


# эндпоинт для платежей
@app.post(
    "/api/events/payment",
    summary="Создание события платежа",
    description="Регистрирует новое событие, связанное с платежом (успех, отказ и т.д.)",
    tags=["events", "payment"],
    operation_id="createPaymentEvent",
    response_model=EventResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_payment_event(payment_event: PaymentEvent):
    """
    Принимает данные о событии платежа, форматирует их и отправляет в Kafka.
    """
    return await _publish_to_kafka(
        topic="payment-events",
        message_text=f"Payment status: {payment_event.status} for payment {payment_event.payment_id}",
        data_model=payment_event
    )

# рутовый эндпоинт
@app.get("/")
async def root():
    return {"status": "ok", "message": "Events microservice is running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8082)
