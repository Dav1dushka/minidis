# minidis

🇷🇺 [Русская версия ниже](#ru)

**A small Redis-compatible server in pure Python.** It speaks the real Redis protocol, so `redis-cli`, `redis-benchmark` and client libraries like redis-py connect to it unchanged. Strings, lists, hashes and sets, key expiry, persistence to an append-only file, password auth, about 1,200 lines with no dependencies.

```console
$ python -m minidis --aof data.aof
INFO minidis listening on 127.0.0.1:6380

$ redis-cli -p 6380
127.0.0.1:6380> SET session:42 "user=davyd" EX 60
OK
127.0.0.1:6380> TTL session:42
(integer) 60
127.0.0.1:6380> RPUSH jobs resize-image send-email
(integer) 2
127.0.0.1:6380> LPOP jobs
"resize-image"
127.0.0.1:6380> INCR page:views
(integer) 1
127.0.0.1:6380> GET jobs
(error) WRONGTYPE Operation against a key holding the wrong kind of value
```

## Why

Redis sits behind a lot of backends: caches, sessions, rate limiters, job queues. Building a small one is the fastest way to understand what it actually does: how a wire protocol is parsed from a TCP stream, why one thread can serve thousands of clients, how keys expire without a timer per key, and what "durable" means when the machine can lose power at any moment.

## Install and run

```bash
pip install git+https://github.com/Dav1dushka/minidis
minidis --port 6380 --aof data.aof --fsync everysec
```

or from a clone: `python -m minidis`. Python 3.10+, standard library only.

It listens on `127.0.0.1` by default. To accept other machines, set a password first: `MINIDIS_PASSWORD=... minidis --host 0.0.0.0`. The password is read from the environment rather than a flag because command-line arguments are visible to every user through `ps`.

## Commands

| Group | Commands |
| --- | --- |
| Strings | `GET` `SET` (with `NX` `XX` `EX` `PX` `EXAT` `PXAT` `KEEPTTL` `GET`) `MGET` `MSET` `INCR` `DECR` `INCRBY` `DECRBY` `APPEND` `STRLEN` |
| Keys | `DEL` `UNLINK` `EXISTS` `TYPE` `KEYS` (glob patterns) `RENAME` `DBSIZE` `FLUSHALL` |
| Expiry | `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `TTL` `PTTL` `PERSIST` |
| Lists | `LPUSH` `RPUSH` `LPOP` `RPOP` `LLEN` `LRANGE` |
| Hashes | `HSET` `HGET` `HDEL` `HGETALL` `HLEN` `HEXISTS` `HINCRBY` |
| Sets | `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SCARD` |
| Server | `PING` `ECHO` `AUTH` `INFO` `SELECT 0` `CLIENT` `BGREWRITEAOF` `QUIT` |

Error messages match Redis word for word, so client libraries raise the right exceptions.

## How it works

```
 TCP bytes ──▶ resp.Parser ──▶ commands.execute ──▶ store.Store
                (RESP2)          (table of 52          (dicts, deques,
                                  commands)             sets, expiry heap)
                                     │
                                     └──▶ aof.AOF ──▶ data.aof (fsync policy)
```

**The protocol** (`resp.py`). Clients send each command as an array of length-prefixed strings: `*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n`. Length prefixes make values binary-safe (a value can contain `\r\n` or zero bytes). The parser is incremental: a command can arrive split over many TCP reads, and one read can carry many commands (pipelining), so nothing is consumed until a whole command is in the buffer. Announced lengths are capped, so a client cannot make the server buffer gigabytes.

**One thread, no locks** (`server.py`). Every connection is an asyncio coroutine, but commands run as plain functions between two `await`s. Only one command runs at a time, so `INCR` from ten clients at once never loses an update, with no locks anywhere. This is the same model Redis uses, and the same trade-off: one slow command (`KEYS *` on a million keys) stalls everyone.

**Expiry** (`store.py`). A key with a TTL is removed in two ways: *lazily* when someone touches it after its deadline, and *actively* by a cycle that runs ten times a second for keys nobody reads again. Deadlines live in a min-heap, so each cycle looks only at keys that are actually due, and a budget caps how many it deletes per cycle so a mass expiry cannot freeze the server. When a key gets a new TTL, its old heap entry is not searched for; it is recognised as stale and skipped when popped.

**Persistence** (`aof.py`). Every write command is appended to a log in RESP form and replayed on start. Two details make that correct:

- Relative times are logged as absolute ones: `SET k v EX 60` is written as `SET k v` + `PEXPIREAT k <unix ms>`. Otherwise a restart 50 seconds later would give the key a fresh minute.
- `--fsync` chooses the trade-off between speed and safety: `always` (fsync before replying), `everysec` (at most about a second of writes can be lost) or `no`.

A crash can leave half a command at the end of the file; on load, that tail is dropped and the file cut back to the last whole command. `BGREWRITEAOF` compacts the log (a million `INCR`s become one `SET`) by writing a new file and atomically renaming it over the old one. Real Redis does this in a forked child using copy-on-write memory; minidis does it in place and blocks while it runs.

## Performance

`redis-benchmark` against minidis and a real `redis-server` on the same 2-vCPU cloud VM (Python 3.13, 20 clients, see `scripts/bench.sh`):

| | minidis | Redis 7 |
| --- | --- | --- |
| SET, one command per round trip | 27,000 req/s | 137,000 req/s |
| GET, one command per round trip | 27,000 req/s | 132,000 req/s |
| SET, pipelined ×16 | 142,000 req/s | 1,250,000 req/s |
| GET, pipelined ×16 | 178,000 req/s | 1,470,000 req/s |

About 5 times slower than Redis for ordinary traffic and 8 times slower when pipelined, where the interpreter rather than the network becomes the limit. Pipelining gives minidis a 5–6× boost for the same reason it helps Redis: fewer round trips and system calls per command.

## Tests

```bash
pip install pytest
pytest -q                # 63 tests: protocol, commands, expiry, AOF, real sockets
bash scripts/smoke.sh    # end to end with the real redis-cli (needs redis-tools)
```

The server tests start minidis on a random port and talk to it over TCP: pipelining 500 commands, a 2 MB value with every byte value, ten clients incrementing one counter, a protocol error that must close only that connection, data surviving a restart. Expiry tests use a fake clock, so they are instant and never flaky. CI also runs the official redis-py client against it.

## Not implemented

Transactions (`MULTI`/`EXEC`), pub/sub, blocking commands (`BLPOP`), sorted sets, streams, replication, RESP3, multiple databases, eviction under memory pressure. Some of these are good next steps.

## License

MIT

---

<a id="ru"></a>

# minidis — по-русски

**Небольшой Redis-совместимый сервер на чистом Python.** Он говорит на настоящем протоколе Redis, поэтому `redis-cli`, `redis-benchmark` и клиентские библиотеки вроде redis-py подключаются к нему без изменений. Строки, списки, хеши и множества, истечение ключей, сохранение в append-only файл, пароль — около 1200 строк без зависимостей.

Пример сессии — в английской части выше.

## Зачем

Redis стоит за множеством бэкендов: кэши, сессии, ограничители запросов, очереди задач. Написать свой маленький — самый быстрый способ понять, что он на самом деле делает: как протокол разбирается из потока TCP, почему один поток обслуживает тысячи клиентов, как ключи истекают без отдельного таймера на каждый и что значит «надёжно сохранено», когда сервер может в любой момент потерять питание.

## Установка и запуск

```bash
pip install git+https://github.com/Dav1dushka/minidis
minidis --port 6380 --aof data.aof --fsync everysec
```

Или из клона: `python -m minidis`. Python 3.10+, только стандартная библиотека.

По умолчанию слушает `127.0.0.1`. Чтобы принимать подключения с других машин, сначала задай пароль: `MINIDIS_PASSWORD=... minidis --host 0.0.0.0`. Пароль берётся из переменной окружения, а не из флага, потому что аргументы командной строки видны всем пользователям через `ps`.

## Команды

| Группа | Команды |
| --- | --- |
| Строки | `GET` `SET` (с `NX` `XX` `EX` `PX` `EXAT` `PXAT` `KEEPTTL` `GET`) `MGET` `MSET` `INCR` `DECR` `INCRBY` `DECRBY` `APPEND` `STRLEN` |
| Ключи | `DEL` `UNLINK` `EXISTS` `TYPE` `KEYS` (glob-шаблоны) `RENAME` `DBSIZE` `FLUSHALL` |
| Время жизни | `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `TTL` `PTTL` `PERSIST` |
| Списки | `LPUSH` `RPUSH` `LPOP` `RPOP` `LLEN` `LRANGE` |
| Хеши | `HSET` `HGET` `HDEL` `HGETALL` `HLEN` `HEXISTS` `HINCRBY` |
| Множества | `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SCARD` |
| Сервер | `PING` `ECHO` `AUTH` `INFO` `SELECT 0` `CLIENT` `BGREWRITEAOF` `QUIT` |

Тексты ошибок совпадают с Redis слово в слово, поэтому клиентские библиотеки выбрасывают правильные исключения.

## Как устроен

Схема — в английской части выше: байты TCP → `resp.Parser` → `commands.execute` → `store.Store`, а команды записи дополнительно уходят в `aof.AOF`.

**Протокол** (`resp.py`). Клиент отправляет каждую команду массивом строк с префиксом длины: `*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n`. Благодаря длине значения бинарно-безопасны (в них может быть `\r\n` или нулевые байты). Парсер инкрементальный: команда может прийти разрезанной на много чтений TCP, а одно чтение может содержать много команд (pipelining), поэтому из буфера ничего не забирается, пока команда не пришла целиком. Объявленные длины ограничены, чтобы клиент не мог заставить сервер буферизовать гигабайты.

**Один поток, без блокировок** (`server.py`). Каждое соединение — корутина asyncio, но сами команды выполняются как обычные функции между двумя `await`. Одновременно выполняется только одна команда, поэтому `INCR` от десяти клиентов сразу никогда не теряет обновления, и при этом нет ни одной блокировки. Это та же модель, что у Redis, и тот же компромисс: одна медленная команда (`KEYS *` на миллион ключей) тормозит всех.

**Истечение ключей** (`store.py`). Ключ с TTL удаляется двумя способами: *лениво* — когда к нему обращаются после срока, и *активно* — циклом, который запускается десять раз в секунду для ключей, которые больше никто не читает. Сроки хранятся в min-куче, поэтому каждый цикл смотрит только на ключи, срок которых действительно наступил, а бюджет ограничивает число удалений за цикл, чтобы массовое истечение не заморозило сервер. Когда ключу ставят новый TTL, старую запись в куче не ищут: при извлечении она распознаётся как устаревшая и пропускается.

**Сохранение** (`aof.py`). Каждая команда записи дописывается в журнал в формате RESP и воспроизводится при старте. Две детали делают это корректным:

- Относительное время записывается как абсолютное: `SET k v EX 60` превращается в `SET k v` + `PEXPIREAT k <unix ms>`. Иначе после перезапуска через 50 секунд ключ снова получил бы целую минуту.
- `--fsync` задаёт компромисс между скоростью и надёжностью: `always` (fsync перед ответом), `everysec` (теряется не больше примерно секунды записей) или `no`.

При сбое в конце файла может остаться половина команды; при загрузке этот хвост отбрасывается, а файл обрезается до последней целой команды. `BGREWRITEAOF` сжимает журнал (миллион `INCR` превращаются в один `SET`): пишет новый файл и атомарно переименовывает его поверх старого. Настоящий Redis делает это в дочернем процессе через fork и copy-on-write; minidis делает это на месте и на это время блокируется.

## Производительность

`redis-benchmark` против minidis и настоящего `redis-server` на одной и той же облачной машине с 2 vCPU (Python 3.13, 20 клиентов, см. `scripts/bench.sh`). Таблица — в английской части выше: minidis примерно в 5 раз медленнее Redis на обычном трафике и в 8 раз — с pipelining, где пределом становится интерпретатор, а не сеть. Pipelining ускоряет minidis в 5–6 раз по той же причине, что и Redis: меньше обменов по сети и системных вызовов на команду.

## Тесты

```bash
pip install pytest
pytest -q                # 63 теста: протокол, команды, истечение, AOF, настоящие сокеты
bash scripts/smoke.sh    # сквозная проверка настоящим redis-cli (нужен redis-tools)
```

Серверные тесты запускают minidis на случайном порту и общаются с ним по TCP: pipeline из 500 команд, значение на 2 МБ со всеми возможными байтами, десять клиентов, увеличивающих один счётчик, ошибка протокола, которая должна закрыть только своё соединение, данные, пережившие перезапуск. Тесты истечения используют фальшивые часы, поэтому проходят мгновенно и никогда не «мигают». В CI к серверу ещё подключается официальный клиент redis-py.

## Что не реализовано

Транзакции (`MULTI`/`EXEC`), pub/sub, блокирующие команды (`BLPOP`), sorted sets, streams, репликация, RESP3, несколько баз, вытеснение при нехватке памяти. Некоторые из них — хорошие следующие шаги.

## Лицензия

MIT
