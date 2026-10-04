"""Конфигурация Company Brain. Все секреты — из окружения/.env, никогда не в коде."""

from __future__ import annotations

import os
from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Настройки приложения. Читаются из переменных окружения с префиксом ``CB_`` и из .env."""

    model_config = SettingsConfigDict(
        env_prefix="CB_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- БД ---
    database_url: str = Field(default="")
    graph_name: str = Field(default="company_brain")
    chunks_table: str = Field(default="chunks")
    # JIT PostgreSQL для соединений сервиса (ADR-010). Выключен по умолчанию со времён AGE:
    # тот отдавал планировщику мусорные cost-оценки, и LLVM компилировал каждый графовый
    # запрос заново (483 мс из 578 мс). Нужно ли это на таблицах графа — решает замер
    # MEM-ADR-023 п.6. Включать только осознанно и под замер.
    db_jit: bool = Field(default=False)

    # --- Мультитенантность (ADR-054) ---
    # Namespace по умолчанию: backfill существующих данных и запросы без scope.
    default_namespace: str = Field(default="nexus")

    # --- Продукт-нейтральность: разрешённые значения enum `project` ---
    # Whitelist слагов проектов (синтетические узлы ``project:*``). Пусто = проектные
    # узлы не создаются (нейтральный движок без продуктовых литералов в коде). Продукт
    # задаёт свои значения через ``CB_PROJECT_VALUES`` (через запятую) или регистрацию
    # в ``ProductExtensionRegistry`` (M3).
    project_values: str = Field(default="")

    # --- Эмбеддинги ---
    embedding_provider: str = Field(default="openai")  # openai | fake
    embedding_base_url: str = Field(default="https://api.edenai.run/v2/openai/v1")
    embedding_api_key: str = Field(default="")
    embedding_model: str = Field(default="text-embedding-3-small")
    embedding_dim: int = Field(default=1536)
    # Таймаут вызова эмбеддингов, секунды. Эмбеддинг — первый вызов в retrieve(), до похода
    # в БД; без явного таймаута работает дефолт openai-SDK (600 с × 2 повтора).
    embedding_timeout: float = Field(default=25.0)

    # --- LLM ---
    llm_provider: str = Field(default="openai")  # openai | echo
    llm_base_url: str = Field(default="https://api.edenai.run/v2/openai/v1")
    llm_api_key: str = Field(default="")
    llm_model: str = Field(default="gpt-4o-mini")

    # --- Vault ---
    vault_path: str = Field(default="")

    # --- Кэш ingest (пропуск неизменённых заметок; пусто = выключен) ---
    cache_dir: str = Field(default="")

    # --- Извлечение свободных сущностей (люди/орги) из тел заметок через LLM ---
    extract_entities: bool = Field(default=False)

    # --- HTTP-сервис (memory API для агентов) ---
    server_host: str = Field(default="127.0.0.1")
    server_port: int = Field(default=8077)
    server_api_key: str = Field(default="")  # если задан — требуется Bearer-токен
    # Фазовый тайминг запросов (амендмент MEM-ADR-020): заголовок Server-Timing и строка
    # лога по фазам (auth, visibility, фазы движка). Выключено по умолчанию — длительности
    # фаз раскрывают внутреннее устройство сервиса.
    server_timing: bool = Field(default=False)
    # Реестр ключей с префиксными грантами по namespace (JSON-массив, ADR-017; формат —
    # server/auth.py). Опционален: legacy CB_SERVER_API_KEY / CB_SERVER_API_KEYS_PII
    # остаются full-access по всем базам знаний; гранты лишь сужают доступ новых ключей.
    api_keys: str = Field(default="")
    # --- IAM access tokens (ADR-018; суперпроект ADR-0013/0030) ---
    # Второй способ аутентификации рядом со статическими ключами: Bearer-JWT, выпущенный
    # IAM на audience этого сервиса, проверяется через platform-auth-sdk (server/iam.py).
    # Выключено по умолчанию — существующие инсталляции ничего не меняют.
    iam_enabled: bool = Field(default=False)
    iam_issuer: str = Field(default="")  # точный `iss` токена, например https://…/iam
    iam_jwks_url: str = Field(default="")  # JWKS IAM (внутренний адрес, не через прокси)
    iam_audience: str = Field(default="memory-service")  # точный `aud`; списки отвергаются
    iam_leeway_seconds: float = Field(default=5.0)  # допуск рассинхрона часов для exp/nbf
    # --- Маршруты ядра (амендмент MEM-ADR-020; TAI-ADR-0042 от 2026-09-23) ---
    # reconcile, packages и namespaces/{ns}/kinds вызывает только identity ядра.
    # Ограничение включено, если задан CB_CORE_IDENTITIES или CB_CORE_ONLY=true: тогда
    # эти маршруты доступны ключу/принципалу из списка (метка вызывающего: имя ключа
    # реестра CB_API_KEYS, ``base``/``pii-full`` legacy-ключей, ``iam:<type>:<id>``
    # IAM-токена) или вызывающему с service scope (IAM ``memory:service``, ключ реестра
    # с ``"service": true``); прочим — 403 даже с грантом на namespace.
    core_only: bool = Field(default=False)
    core_identities: str = Field(default="")  # метки через запятую
    query_synthesize_default: bool = Field(default=False)  # по умолчанию отдаём контекст без LLM
    # --- Видимость по principal через policy-service (MEM-ADR-019; TAI-ADR-0025/0031) ---
    # Включено — для IAM-токена человека/агента разрешённые namespaces и scopes
    # берутся из policy-service (list_objects memory.read); service account со scope
    # memory:on-behalf передаёт их в теле запроса. Статические ключи не ограничиваются.
    policy_enabled: bool = Field(default=False)
    policy_url: str = Field(default="http://localhost:8030")
    policy_audience: str = Field(default="policy-service")
    policy_timeout_seconds: float = Field(default=3.0)
    policy_cache_ttl_seconds: float = Field(default=5.0)
    # Service identity самого memory-service в IAM (client credentials) — для вызова policy.
    iam_base_url: str = Field(default="")
    iam_client_id: str = Field(default="")
    iam_client_secret: str = Field(default="")

    # --- Защита персональных данных (152-ФЗ), см. ADR-002 ---
    # Мастер-флаг: выключен по умолчанию — существующие инсталляции не меняют поведения.
    pii_protection: bool = Field(default=False)
    # Токены с полным допуском к ПДн (через запятую). Базовый server_api_key при
    # включённой защите получает маскированную выдачу.
    server_api_keys_pii: str = Field(default="")

    # --- Memory Console (административный веб-интерфейс) ---
    # Выключена по умолчанию: публичный Memory API работает без Console и без новых
    # обязательных переменных. Доступ к /console обязан ограничивать reverse proxy /
    # network policy на контуре клиента — своей аутентификации у Console нет.
    console_enabled: bool = Field(default=False)
    # CSV-allowlist баз знаний, доступных в Console (пусто -> только default_namespace).
    # UI предлагает ровно этот список и не принимает произвольный namespace.
    console_namespaces: str = Field(default="")

    # --- Публичная демо-витрина (/demo, ADR-009) ---
    # Публичная read-only витрина ценности «проверяемой памяти» (search + source-view)
    # поверх ОДНОЙ демо-KB. Выключена по умолчанию: публичный Memory API работает без
    # неё и без новых обязательных переменных. НЕ включать на контуре с реальными данными.
    demo_public_enabled: bool = Field(default=False)
    # Единственная база знаний, которую отдаёт витрина. Клиент namespace НЕ задаёт —
    # изоляция жёсткая: наружу видна только эта KB.
    demo_namespace: str = Field(default="demo")
    # Лимит запросов к /demo/api/* с одного IP в минуту (публичный вход). 0 -> без лимита.
    demo_rate_limit: int = Field(default=30)
    # Сколько фрагментов показывает «Search Check» (top-k).
    demo_search_k: int = Field(default=5)
    # Бренд/домен витрины — в одном месте, нейтрально по умолчанию (нейминг не развязан).
    # Продукт задаёт своё через CB_DEMO_BRAND_*/CB_DEMO_DOMAIN, не трогая разметку.
    demo_brand_name: str = Field(default="Verifiable Memory")
    demo_brand_tagline: str = Field(default="Answers your team can trace back to the source.")
    # Ссылка заявки на пилот (mailto: или https://…). Пусто -> CTA ведёт на mailto демо-домена.
    demo_cta_url: str = Field(default="")
    # Канонический домен витрины (для og/ссылок). Пусто -> относительные пути.
    demo_domain: str = Field(default="")

    # --- Реранкинг кандидатов перед выдачей top-k (ADR-005) ---
    # Переоценивает релевантность пары (запрос, кандидат) семантически и даёт единый
    # порядок; даёт настоящий 0..1 rerank_score. Выключен по умолчанию → поведение
    # ровно текущее (RRF без изменений). Провайдер по умолчанию — LLM поверх openai-клиента.
    rerank_enabled: bool = Field(default=False)  # глобально для retrieve/query/recall
    rerank_provider: str = Field(default="llm")  # llm (через существующий openai-клиент)
    rerank_pool: int = Field(
        default=20
    )  # размер пула кандидатов до реранка (берём top-N, реранкуем)
    rerank_model: str = Field(default="")  # пусто -> llm_model
    # Витрина /demo реранкует СВОЙ поиск независимо от глобального флага — чтобы дать
    # демонстративную уверенность, но не нагружать LLM realtime-путь /api/brain (recall).
    demo_rerank: bool = Field(default=True)
    # Порог уверенности витрины: карточки с rerank_score ниже не показываются (не зашумляем
    # выдачу нерелевантным). Действует только когда реранк отработал; 0 -> не фильтровать.
    demo_min_confidence: float = Field(default=0.3)

    # --- Наблюдаемость прогонов (run-аудит в граф-трейс) ---
    run_audit_enabled: bool = Field(default=True)  # эмиссия lifecycle-событий прогона

    # --- Context Memory Engine (ADR-016): observations + Context Compiler ---
    observations_table: str = Field(default="observations")
    context_traces_table: str = Field(default="context_traces")
    # Эмбеддить content наблюдений при ingest (вектор-канал для сырых observations).
    # Выключено по умолчанию: lexical/recent-каналы находят их без эмбеддингов,
    # а ingest-ACK не зависит от внешнего провайдера.
    observations_embed: bool = Field(default=False)
    # Максимум наблюдений в одном batch-запросе (защита от гигантских транзакций).
    observations_max_batch: int = Field(default=500)
    # Бюджет ContextPack по умолчанию (токены) и оценка размера (символов на токен).
    # 3.0 — консервативно для кириллицы (у cl100k-подобных токенизаторов русский
    # текст плотнее ~2.5-3 симв/токен): оценка завышает размер и бюджет модели
    # не переполняется. Для чисто английского контента можно поднять до 4.0.
    context_default_max_tokens: int = Field(default=8000)
    context_chars_per_token: float = Field(default=3.0)
    # Жёсткие лимиты graph expansion в Context Compiler (ADR-016 §24).
    context_max_depth: int = Field(default=2)
    context_max_nodes: int = Field(default=60)
    context_max_edges: int = Field(default=120)

    # --- Доменные пакеты видов и сверка снимков источников (MEM-ADR-020; TAI-ADR-0042) ---
    # Реестр пакетов (виды/связи как данные), настройки видов namespace (строгий режим)
    # и журнал снимков источников для reconcile (+ таблица ``<snapshots_table>_items``).
    domain_packs_table: str = Field(default="domain_packs")
    namespace_settings_table: str = Field(default="namespace_settings")
    snapshots_table: str = Field(default="source_snapshots")
    # Индекс эмбеддингов сущностей видов с ``searchable`` (поиск по смыслу по сущностям,
    # MEM-ADR-020 амендмент 2026-09-28): открытые версии, размерность — CB_EMBEDDING_DIM.
    entity_embeddings_table: str = Field(default="entity_embeddings")
    # Максимум сущностей+фактов в одном снимке reconcile (защита от гигантских транзакций).
    reconcile_max_items: int = Field(default=20000)
    # Максимум ключей в каждом списке ``changes`` ответа reconcile (opened/changed/closed).
    reconcile_changes_limit: int = Field(default=1000)

    @model_validator(mode="after")
    def _fill_database_url(self) -> Settings:
        """Если CB_DATABASE_URL не задан — собрать из POSTGRES_* (как в docker-compose)."""
        if not self.database_url:
            user = os.getenv("POSTGRES_USER", "brain")
            password = os.getenv("POSTGRES_PASSWORD", "brain")
            host = os.getenv("POSTGRES_HOST", "localhost")
            port = os.getenv("POSTGRES_PORT", "5432")
            db = os.getenv("POSTGRES_DB", "company_brain")
            self.database_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        return self

    def project_value_set(self) -> set[str]:
        """Распарсить ``CB_PROJECT_VALUES`` (через запятую) в множество слагов проектов."""
        return {value.strip() for value in self.project_values.split(",") if value.strip()}

    def console_namespace_list(self) -> list[str]:
        """Allowlist баз знаний Console (CSV, порядок сохраняется); пусто -> default."""
        items = [ns.strip() for ns in self.console_namespaces.split(",") if ns.strip()]
        return list(dict.fromkeys(items)) or [self.default_namespace]

    def model_copy_with(self, **overrides) -> Settings:
        """Удобная копия с переопределениями (используется в тестах для изоляции графа/таблицы)."""
        data = self.model_dump()
        data.update(overrides)
        return Settings(**data)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Кэшированный синглтон настроек."""
    return Settings()
