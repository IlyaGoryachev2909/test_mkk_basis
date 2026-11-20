# Асинхронный процессинг платежей

Сервис принимает платёж по HTTP, сохраняет его в PostgreSQL и возвращает `202 Accepted`. Обработчик FastStream получает событие из RabbitMQ, эмулирует платёжный шлюз и отправляет результат на webhook клиента.

## Запуск

Из корня проекта выполните команду в Docker Compose v2.

```sh
docker compose up --build -d --wait
```

API доступен по адресу `http://127.0.0.1:8000`. Compose запускает четыре сервиса — `postgres`, `rabbitmq`, `api` и `consumer`. API применяет миграции Alembic перед стартом. Данные PostgreSQL и RabbitMQ сохраняются в именованных томах при `docker compose down`.

Для локального запуска используется ключ `local-demo-key`. Чтобы задать свой ключ или порт, укажите `API_KEY` и `API_PORT` в файле `.env` рядом с `compose.yaml` перед запуском. Оба эндпоинта требуют заголовок `X-API-Key`.

## Создание и получение платежа

Пример для PowerShell использует webhook на компьютере с Docker Desktop. Подставьте адрес своего HTTP(S)-приёмника. Для проверки с готовым локальным приёмником можно сразу запустить интеграционные тесты ниже.

```powershell
$base = 'http://127.0.0.1:8000/api/v1/payments'
$headers = @{ 'X-API-Key' = 'local-demo-key' }
$createHeaders = $headers + @{ 'Idempotency-Key' = [guid]::NewGuid().ToString() }
$body = @{
    amount      = '1290.50'
    currency    = 'RUB'
    description = 'Order 42'
    metadata    = @{ order_id = 42 }
    webhook_url = 'http://host.docker.internal:9000/webhook'
} | ConvertTo-Json

$payment = Invoke-RestMethod -Method Post -Uri $base -Headers $createHeaders -ContentType 'application/json' -Body $body
$payment
Invoke-RestMethod -Uri "$base/$($payment.payment_id)" -Headers $headers
```

Первый `POST /api/v1/payments` возвращает `202` и тело следующего вида.

```json
{
  "payment_id": "f2e8b366-f937-47e2-8ee4-e336cc0dd5d1",
  "status": "pending",
  "created_at": "2026-11-18T08:00:00Z"
}
```

| Запрос | Результат |
| --- | --- |
| Повторный POST с тем же ключом и телом | `202`, тот же ID, дата создания и актуальный статус |
| Тот же ключ с изменённым телом | `409 Conflict` |
| GET по ID | `200`, сумма, валюта, описание, метаданные, статус, ключ, URL и даты |
| GET по неизвестному UUID | `404 Not Found` |
| Отсутствующий или неверный API-ключ | `401 Unauthorized` |
| POST без ключа идемпотентности или с недопустимыми данными | `422 Unprocessable Entity` |

Сумма положительна, допускает до двух знаков после запятой и хранится как `NUMERIC(18,2)`. Поддерживаются `RUB`, `USD` и `EUR`. Поле `metadata` принимает JSON-объект и по умолчанию равно `{}`. Даты возвращаются с часовым поясом. До завершения обработки `processed_at` равно `null`.

## Обработка и доставка

1. API создаёт платёж и запись `outbox` в одной транзакции. Уникальное ограничение на `idempotency_key` защищает от параллельных дублей. Для проверки совпадения тела запроса сохраняется SHA-256 нормализованных полей.
2. Фоновая задача в процессе `consumer` выбирает неопубликованную запись через `FOR UPDATE SKIP LOCKED` и отправляет постоянное сообщение с `payment_id` в очередь `payments.new`. Отметка `published_at` записывается после подтверждения RabbitMQ. При недоступном брокере API продолжает принимать платежи.
3. Единственный подписчик блокирует строку платежа. Для статуса `pending` шлюз эмулирует задержку 2–5 секунд и возвращает `succeeded` с вероятностью 90% или `failed` с вероятностью 10%. Обработчик фиксирует результат и время в PostgreSQL до отправки уведомления. Оба результата отправляются клиенту.
4. Обработчик делает HTTP POST на `webhook_url`. Ответ `2xx` подтверждает доставку. Сетевые ошибки, тайм-ауты и прочие HTTP-статусы запускают повтор. Всего предусмотрены три попытки, включая первую. Очереди `payments.retry.1s` и `payments.retry.2s` возвращают событие в `payments.new` через 1 и 2 секунды соответственно. Обработчик подтверждает исходное сообщение после подтверждения публикации повтора.
5. При третьей ошибке обработчик отклоняет сообщение, и RabbitMQ направляет его в `payments.dlq`. Маршрутизация идёт через direct exchange `payments`. Используются долговечные quorum-очереди и dead lettering с гарантией *at least once*. Статус платежа отражает результат шлюза даже при недоступности webhook.

Пример тела webhook для успешного платежа.

```json
{
  "payment_id": "f2e8b366-f937-47e2-8ee4-e336cc0dd5d1",
  "status": "succeeded",
  "amount": "1290.50",
  "currency": "RUB",
  "processed_at": "2026-11-18T08:00:04.218943+00:00"
}
```

Если процесс остановится после приёма уведомления клиентом, но до фиксации доставки в БД, webhook может прийти повторно. Получатель может устранить дубль по заголовку `Idempotency-Key`, равному `payment_id`. Результат шлюза и содержимое уведомления при повторе сохраняются.

## Проверка

```sh
docker compose exec -T api python -m unittest discover -s tests -v
docker compose exec -T api alembic check
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

Тесты используют работающий стек и поднимают собственный HTTP-приёмник в контейнере API. Они проверяют валидацию, авторизацию, восемь параллельных запросов с одним ключом, восстановление после ответа `503` и отправку в DLQ на третьей ошибке. Тестовые платежи и окончательно отклонённое сообщение сохраняются для просмотра.
