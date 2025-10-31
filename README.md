# README.md

## 📘 DBF Sync Refactor

**DBF Sync Refactor** — это утилита синхронизации и загрузки данных из DBF-файлов в базу данных (SQLite или PostgreSQL). Данные обрабатываются как *временные ряды (time series)*: поля `DATE` и `TIME` объединяются в одно поле `TS`.

### 🚀 Возможности
- Поддержка чтения DBF-файлов со структурой из спецификации.
- Автоматическое извлечение идентификатора счётчика (`COUNTER_ID`) из имени файла.
- Пакетная вставка данных (batch insert) для высокой производительности.
- Поддержка SQLite и PostgreSQL.
- Ведение состояния обработанных файлов (`state.json`).
- Возможность dry-run (без записи в БД).
- Создание структуры БД при первом запуске (`--init`).
- Поддержка ротации логов.

---

## ⚙️ Установка

### 1️⃣ Установка зависимостей
```bash
pip install pyyaml dbfread psycopg2-binary tqdm
```

### 2️⃣ Конфигурация
Создайте файл `config.yaml` в корне проекта:

```yaml
source:
  folder: ./dbf            # путь к папке с DBF файлами
  pattern: ????xxxx.dbf     # шаблон поиска файлов, xxxx заменяется на год

db:
  type: sqlite             # или 'postgres'
  sqlite_path: ./db.sqlite  # путь к sqlite базе
  # параметры PostgreSQL (если используется)
  # host: 127.0.0.1
  # port: 5432
  # dbname: reports
  # user: postgres
  # password: secret

state_file: ./state.json   # файл состояния обработанных файлов
log_file: ./dbf_sync.log   # файл логов
batch_size: 1000           # размер пакета вставки
```

---

## 🧩 Использование

### Инициализация структуры базы данных
```bash
python dbf_sync_refactor.py --init --config config.yaml
```

### Синхронизация данных
```bash
python dbf_sync_refactor.py --config config.yaml
```

### Dry-run (без записи в БД)
```bash
python dbf_sync_refactor.py --config config.yaml --dry-run
```

---

## 🧱 Структура таблицы `reports`

| Поле | Тип | Описание |
|------|-----|----------|
| id | INTEGER / SERIAL | Первичный ключ |
| counter_id | INTEGER | Идентификатор счётчика |
| ts | TIMESTAMP / TEXT | Метка времени (объединение даты и времени) |
| count | INTEGER | Значение счётчика |
| kod | INTEGER | Код события |
| key | INTEGER | Ключевой параметр |
| avr, dna, pit, ktime | BOOLEAN / INTEGER | Булевы флаги |
| avrtime, pittime | INTEGER | Время соответствующих состояний |
| fs | INTEGER | Код флагов состояния |
| shift | INTEGER | Номер смены |
| shift_end | BOOLEAN | Завершение смены |
| sensor1_alarm, sensor2_alarm | BOOLEAN | Аварии датчиков |
| time_sync | BOOLEAN | Флаг синхронизации времени |
| read_attempt | BOOLEAN | Попытка чтения |

---

## 📄 Логи и состояние
- Все события и ошибки записываются в файл `dbf_sync.log` (ротация включена).
- Список обработанных файлов хранится в `state.json`.

---

## 🧪 Примеры

```bash
# Пример запуска для SQLite
env PYTHONUTF8=1 python dbf_sync_refactor.py --config config.yaml

# Пример запуска для PostgreSQL
python dbf_sync_refactor.py --config config_pg.yaml
```

---

## 🧰 Лицензия
MIT License