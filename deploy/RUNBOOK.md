# Runbook: изолированный инстанс memory-service

Контур «один заказчик = один инстанс»: `memory-service` + выделенный
`memory-db` (PostgreSQL 16 + Apache AGE + pgvector). Несколько баз знаний
одного заказчика разделяются namespace'ами внутри инстанса; для другого
заказчика поднимается отдельный такой же контур с отдельной БД.

Все секреты — только в `.env` (в git не попадает). В этом репозитории и
runbook'е нет и не должно быть реальных адресов, токенов и имён заказчиков.

## Требования

- Docker Engine 24+ с Compose v2; ~2 ГБ RAM для контура.
- Исходники этого репозитория на сервере (образы собираются на месте,
  готовый registry не требуется).
- Reverse proxy на хосте (nginx/traefik/…) — обязателен, см. ниже.

## Установка и запуск

```bash
cd deploy
cp .env.example .env        # заполнить: пароли БД, API-токен(ы), провайдеры
docker compose up -d --build
docker compose ps           # оба сервиса должны стать healthy
curl -fsS http://127.0.0.1:8077/healthz
```

Первый запуск создаёт том БД и инициализирует расширения (AGE, pgvector) и
граф. Схема памяти доинициализируется сервисом идемпотентно.

### Смоук-тест (обязателен после каждого деплоя)

```bash
BASE=http://127.0.0.1:8077 TOKEN=<CB_SERVER_API_KEY из .env> ./smoke-test.sh
```

Прогон полностью оффлайн при `CB_EMBEDDING_PROVIDER=fake` и
`CB_LLM_PROVIDER=echo`; проверяет живость, импорт в две KB, изоляцию
recall/source-view между KB и удаление с аудит-следом, после себя подчищает
тестовые записи. Для боевого качества поиска затем переключите провайдеры на
openai-совместимый endpoint и повторите смоук.

## Аутентификация потребителей: два способа

Заголовок один — `Authorization: Bearer <token>`; способа два, работают одновременно:

1. **Статические ключи** (`CB_SERVER_API_KEY`, `CB_SERVER_API_KEYS_PII`, реестр
   `CB_API_KEYS` с грантами по префиксу namespace — ADR-017). Как и прежде; ничего
   не меняется, если IAM не включать.
2. **Access token IAM платформы** (`CB_IAM_ENABLED=true`, ADR-018) — для контуров,
   подключённых к IAM. Bearer, не совпавший ни с одним статическим ключом и похожий
   на JWT, проверяется через общий `platform-auth-sdk`: подпись по JWKS
   (`CB_IAM_JWKS_URL`), точные `iss` (`CB_IAM_ISSUER`) и `aud`
   (`CB_IAM_AUDIENCE`, по умолчанию `memory-service`), срок с допуском
   `CB_IAM_LEEWAY_SECONDS`. Права выводятся из токена: scopes `memory:read` /
   `memory:write` / `memory:pii`, базы знаний — `tenant:<tenant_id>` и поддерево
   `tenant:<tenant_id>:*` (плюс claim `memory_namespaces`). Дефект токена → `401`
   (тот же ответ, что на неверный ключ), JWKS недоступен → `503` (fail closed,
   статические ключи при этом продолжают работать).

Что нужно на стороне IAM: audience `memory-service` с `allowedScopes`
`memory:read`, `memory:write`, `memory:pii`; потребители получают токен обменом
Platform Access Token на этот audience. `CB_IAM_JWKS_URL` указывайте на
**внутренний** адрес IAM (сеть контура), а не на внешний прокси: проверка подписи
не должна зависеть ни от внешнего TLS, ни от прокси. Локальный режим без
авторизации (все `CB_*_KEY*` пусты) при `CB_IAM_ENABLED=true` **не включается** —
запрос без токена получит `401`.

Сборка образа: контекст — **корень суперпроекта** (`context: ../../..`,
`dockerfile: services/memory-service/Dockerfile`), потому что `platform-auth-sdk`
лежит в `sdk/platform-auth-sdk` (раскладка TAI-ADR-0064); `Dockerfile.dockerignore`
ограничивает контекст двумя каталогами.

## Reverse proxy — обязательный контур доступа

Порт сервиса привязан к `127.0.0.1` хоста и **не должен публиковаться
наружу**. Весь внешний трафик — только через reverse proxy:

- `/api/brain/*`, `/healthz` — проксировать внешнему потребителю;
  авторизация здесь — Bearer-токен самого сервиса, проксируйте заголовок
  `Authorization` как есть.
- `/console` — **админ-контур без собственной аутентификации**. Проксировать
  только после аутентификации администратора средствами proxy (mTLS, SSO,
  IP-allowlist + basic auth — по политике контура) либо не проксировать вовсе
  (доступ только с хоста через SSH-туннель). Пока `CB_CONSOLE_ENABLED=false`,
  все маршруты `/console` отвечают 404 — включайте её только одновременно
  с настройкой proxy.
- Заголовки доверия, приходящие извне (`X-Forwarded-*`, кастомные auth-заголовки),
  proxy должен вырезать/переустанавливать сам — сервис не должен получать их
  от клиента напрямую.

Пример nginx (фрагмент; TLS и модуль auth — по стандартам контура):

```nginx
location /api/brain/ { proxy_pass http://127.0.0.1:8077; }
location /healthz    { proxy_pass http://127.0.0.1:8077; }

location /console {
    # аутентификация администратора обязательна, например:
    #   auth_basic "memory-console"; auth_basic_user_file /etc/nginx/console.htpasswd;
    allow 10.0.0.0/8;        # пример: только внутренняя сеть контура
    deny  all;
    proxy_set_header X-Forwarded-For "";   # не транслировать доверие извне
    proxy_pass http://127.0.0.1:8077;
}

location / { return 404; }   # всё остальное (в т.ч. /docs) наружу не отдаём
```

### Traefik с path-prefix

Для уже существующего Traefik используйте дополнительный
`docker-compose.traefik.yml`, а не меняйте базовый Compose-контур. Он добавляет
`memory-service` в сеть proxy и срезает path-prefix до передачи приложению;
PostgreSQL остаётся только во внутренней сети.

```bash
# .env (значения — пример, не секреты)
TRAEFIK_NETWORK=proxy_default
CB_PUBLIC_HOST=memory.example.com
CB_PUBLIC_PATH_PREFIX=/memory

docker compose -f docker-compose.yml -f docker-compose.traefik.yml up -d --build
```

Внешний base URL в примере — `https://memory.example.com/memory/`: путь
`/memory/api/brain/recall` становится `/api/brain/recall` внутри приложения.
`/memory/healthz` допускает проверку живости без токена; все прочие API-маршруты
сохраняют Bearer-аутентификацию. Не публикуйте `/console`, пока не настроена
отдельная аутентификация proxy и `CB_CONSOLE_ENABLED` не включён осознанно.

## Включение Memory Console

1. Настроить и проверить аутентификацию `/console` на proxy (см. выше).
2. В `.env`: `CB_CONSOLE_ENABLED=true`, `CB_CONSOLE_NAMESPACES=bank,mos-card`
   (CSV — список баз знаний этого заказчика; только они будут доступны в UI).
3. `docker compose up -d` (пересоздаст контейнер сервиса с новым окружением).
4. Проверка: изнутри хоста `curl -s http://127.0.0.1:8077/console | head` —
   HTML; снаружи без аутентификации proxy должен отдавать 401/403.

## Резервное копирование и восстановление

Вся память живёт в PostgreSQL (том `memory-db-data`); сервис — stateless.

```bash
# Бэкап (дамп всей БД; выполнять по расписанию + перед обновлениями)
cd deploy
docker compose exec -T memory-db \
  pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc > backup-$(date +%Y%m%d-%H%M).dump

# Восстановление на чистый том
docker compose down
docker volume rm memory-service_memory-db-data
docker compose up -d memory-db          # дождаться healthy: создастся пустая БД
docker compose exec -T memory-db \
  pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists < backup-XXXX.dump
docker compose up -d
BASE=http://127.0.0.1:8077 TOKEN=… ./smoke-test.sh
```

Дампы содержат данные заказчика (включая ПДн) — хранить в контуре заказчика
по его политике, не выносить наружу.

## Обновление версии

```bash
cd deploy
git pull                       # или доставка исходников принятым способом
docker compose build
docker compose up -d           # пересоздаст только изменившиеся контейнеры
./smoke-test.sh                # BASE/TOKEN как выше
```

Откат: `git checkout <прежний тег/коммит>` и те же две команды. Миграции схемы
идемпотентны и аддитивны, отдельных шагов не требуют. Начиная с Context Memory
Engine (ADR-016) при старте сервиса дополнительно создаются таблицы
`observations` и `context_traces` и, при наличии прав, расширение `pg_trgm` с
trigram-индексами (без прав на CREATE EXTENSION identifier-поиск деградирует до
последовательного ILIKE — корректно, медленнее; образ `memory-db` ставит
расширение сам). Существующие таблицы и граф не меняются.

### Переход на граф в таблицах (MEM-ADR-023)

Версия с графом в таблицах PostgreSQL (`graph_nodes`/`graph_edges`) не читает граф
Apache AGE: на контуре, где граф ещё в AGE, его переносит одноразовая команда. Делается
один раз, пока расширение AGE стоит; данные AGE она не меняет.

```bash
cd deploy
docker compose exec -T memory-db pg_dump -U "$POSTGRES_USER" -Fc "$POSTGRES_DB" > backup-before-graph-tables.dump
git pull && docker compose build memory-service
docker compose stop memory-service            # прежняя версия больше не пишет в AGE
docker compose run --rm memory-service cb migrate-graph-from-age --dry-run
docker compose run --rm memory-service cb migrate-graph-from-age; echo "exit=$?"
docker compose up -d memory-service
./smoke-test.sh
```

- `--dry-run` показывает, сколько узлов и рёбер в каждом namespace, дубли и то, что не
  перенесётся (ключ длиннее 2048 байт, запись без `natural_key`, ребро к такой вершине).
  Пропуски — решение владельца до переноса: команда их не обрезает и не чинит.
- Код выхода: `0` — таблицы = AGE − дубли по каждому namespace; `1` — расхождение или
  пропуски (список в отчёте); `2` — графа AGE нет (`CB_GRAPH_NAME`?), конфликт имён в
  схеме или ошибка БД. Повторный запуск доносит недостающее и ничего не дублирует —
  **но только пока сервис остановлен, до `up -d`**. После запуска сервис удаляет узлы
  (purge наблюдений, prune vault), а AGE их по-прежнему хранит: повтор вернул бы
  удалённое, в том числе вычищенные персональные данные. После `up -d` команду не
  запускать; проверка после запуска — только `--dry-run` (он ничего не пишет).
- «Каталог AGE указывает на OID другого кластера» — база восстанавливалась из дампа;
  переносу не мешает, `cypher()` на ней не работает (рецепт починки — в отчёте).
- Новая версия запущена до переноса и уже писала в пустые таблицы — сверка покажет
  лишние строки (`1`); перенос при этом идёт (`ON CONFLICT DO NOTHING`), решение о
  расхождении — по отчёту.
- Откат до удаления AGE — прежний образ сервиса: граф AGE не тронут. Расширение и
  таблицы меток AGE удаляет отдельный шаг следующего релиза; не через
  `ag_catalog.drop_graph(…, true)` — он удаляет схему графа целиком, вместе с
  `graph_nodes`/`graph_edges`.

Диагностика нового контура: `GET /api/memory/observations?status=failed` —
наблюдения, чья проекция не удалась (сырые записи не теряются);
`POST /api/memory/consolidate` — идемпотентный redrive;
`GET /api/memory/context/trace/{id}` — почему собран именно такой контекст.

## Диагностика

- `docker compose logs -f memory-service` — логи сервиса;
  `docker compose logs memory-db` — БД.
- `curl -fsS http://127.0.0.1:8077/healthz` — 503 значит БД недоступна.
- `GET /api/brain/trace/{trace_id}?namespace=…` — восстановить след операции
  (что искали/писали/удаляли; `X-Run-Id` запросов попадает сюда).
- Console → «Обзор» показывает состояние движка и последние операции KB.

## Чего не делать

- Не публиковать порт 8077 (или `MEMORY_SERVICE_PORT`) наружу и не убирать
  привязку к 127.0.0.1.
- Не включать `CB_CONSOLE_ENABLED` без аутентификации на proxy.
- Не выполнять деструктивных операций с БД (drop/truncate) без явного
  подтверждения владельца и свежего бэкапа.
- Не класть реальные токены/адреса в файлы репозитория — только в `.env`.
