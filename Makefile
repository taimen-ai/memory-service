# Цели, которые зовут CI (.github/workflows/ci.yml) и проверки исполнителя
# (checks в .agents/runner.yaml): одна команда на проверку, копий команд нет.
.PHONY: install lint fmt test check runner-config

# Extra mcp входит в среду: без него `uv run` пересобрал бы окружение без MCP,
# а тесты MCP-сервера скипнулись бы молча.
UV_RUN := uv run --frozen --extra mcp

install:
	uv sync --frozen --extra mcp

lint:
	$(UV_RUN) ruff check .
	$(UV_RUN) ruff format --check .

fmt:
	$(UV_RUN) ruff check . --fix
	$(UV_RUN) ruff format .

# Один прогон тестов на рабочую копию: второй `make test` сразу выходит, а не
# встаёт в очередь (или во взаимную блокировку) за первым в той же тестовой базе.
# Блокировку держит fd 9, его наследует pytest; снимается, как бы прогон ни кончился.
# Где flock(1) нет (macOS), прогон идёт без блокировки с предупреждением.
# Интеграционные тесты идут при CB_TEST_DATABASE_URL, без неё — скипаются.
# Дополнительные аргументы pytest: make test PYTEST_ARGS="tests/test_x.py -x".
TEST_LOCK := .pytest.lock

test:
	@exec 9>$(TEST_LOCK); \
	if ! command -v flock >/dev/null; then \
		echo "flock не найден (util-linux): запуск без блокировки $(TEST_LOCK)" >&2; \
	elif ! flock -n 9; then \
		echo "тесты уже идут: $(TEST_LOCK) занят другим make test в этой рабочей копии" >&2; \
		exit 75; \
	fi; \
	$(UV_RUN) pytest -q $(or $(PYTEST_ARGS),tests)

check: lint test

# Проверка .agents/runner.yaml разборщиком формата из копии control-plane
# (по умолчанию — соседний каталог): молчит и выходит с 0, если файл верен.
CONTROL_PLANE ?= ../control-plane

runner-config:
	uv run --no-project --with pyyaml --with jsonschema \
		python $(CONTROL_PLANE)/src/control_plane_agent/runner_config.py .agents/runner.yaml
