import os
from fastapi import FastAPI, Request, Response
import httpx
import logging

app = FastAPI()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация из окружения
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080").rstrip("/")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081").rstrip("/")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082").rstrip("/")

GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() in ("1", "true", "yes", "on")
try:
    MOVIES_MIGRATION_PERCENT = max(0, min(100, int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))))
except ValueError:
    MOVIES_MIGRATION_PERCENT = 0

#BACKEND_URL = "http://cinemaabyss-monolith:8080"

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}

def choose_backend(path: str, request: Request) -> str:
    """
    Простые правила маршрутизации:
    - /events* -> events-service
    - /movies* -> по фича-флагу и проценту — либо movies-service, либо монолит
    - остальное -> монолит
    """
    # path приходит без ведущего слеша, поэтому проверяем startswith без "/"
    lower_path = path.lower()
    logger.info(f"lower_path: {lower_path}")
    if lower_path.startswith("api/events"):
        logger.info(f"EVENTS_SERVICE_URL: {EVENTS_SERVICE_URL}")
        return EVENTS_SERVICE_URL

    if lower_path.startswith("api/movies"):
        if GRADUAL_MIGRATION and MOVIES_MIGRATION_PERCENT in (50, 100):
            logger.info(f"MOVIES_SERVICE_URL: {MOVIES_SERVICE_URL}")
            return MOVIES_SERVICE_URL
        logger.info(f"MONOLITH_URL: {MONOLITH_URL}")
        return MONOLITH_URL

    return MONOLITH_URL


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD", "TRACE"])
async def proxy_route(request: Request, path: str):
    target_base = choose_backend(path, request)
    url = f"{target_base}/{path}"
    logger.info(f"url: {url}")
    #url = f"{BACKEND_URL}/{path}"

    logger.info(f"request.headers.items: {request.headers.items()}")
    # Входящие заголовки — фильтруем, не пробрасываем hop-by-hop и Host/Content-Length
    headers = {}
    for k, v in request.headers.items():
        lk = k.lower()
        if lk in HOP_BY_HOP_HEADERS or lk in ("host", "content-length"):
            continue
        headers[k] = v
    logger.info(f"headers: {headers}")

    body = await request.body()
    logger.info(f"body: {body}")

    async with httpx.AsyncClient(follow_redirects=False, timeout=20.0) as client:
        resp = await client.request(
            method=request.method,
            url=url,
            headers=headers,
            content=body,
            params=request.query_params,
        )
    logger.info("httpx.AsyncClient OK")

    # Готовим заголовки ответа для клиента
    response_headers = {}
    for k, v in resp.headers.items():
        lk = k.lower()
        if lk in HOP_BY_HOP_HEADERS:
            continue
        # Content-Length пересчитается FastAPI сам, можно не ставить
        if lk == "content-length":
            continue
        response_headers[k] = v
    logger.info(f"response_headers: {response_headers}")

    # Возвращаем сырое тело и статус-код. Это сохранит JSON-массивы без оберток.
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=response_headers,
        media_type=resp.headers.get("content-type"),
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)