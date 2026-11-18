# godulkit — Rencana MVP

Library Go untuk queue dan realtime dengan beberapa adapter yang berbagi abstraksi yang sama.

## Keputusan

| Topik | Keputusan |
|---|---|
| Module path | `github.com/abdul15irsyad/godulkit/<modul>` |
| Versi Go | `go 1.22` (naik otomatis jika dependency mensyaratkan versi lebih baru) |
| Struktur | multi-module; tiap modul di-tag sendiri (`queue/v0.1.0`, `queue-kafka/v0.1.0`, ...) |
| `go.work` | hanya untuk development lokal, **tidak di-commit** (`.gitignore`); dibuat lewat `make work` |
| Kafka | franz-go (pure Go, tanpa cgo) |
| RabbitMQ | amqp091-go (reconnect ditangani adapter) |
| SQS | aws-sdk-go-v2; hanya Standard queue |
| SQS FIFO | tidak masuk MVP |
| SNS fan-out | tidak masuk MVP |
| Auto-create queue/topic | opsi `WithAutoCreate(true)`, default **mati** di semua adapter |
| Realtime multi-instance | masuk MVP via broker Redis Pub/Sub |
| Realtime memakai `queue` sebagai broker | tidak |

## Struktur repo

```
godulkit/
├── Makefile            # work, test, test-isolated, lint, tidy
├── .gitignore          # go.work, go.work.sum
├── queue/              # interface, memory adapter, middleware, codec, queuetest
├── queue-kafka/        # franz-go
├── queue-rabbitmq/     # amqp091-go
├── queue-sqs/          # aws-sdk-go-v2/service/sqs
├── realtime/           # server, transport ws & sse, broker memory, realtimetest
├── realtime-redis/     # broker Redis Pub/Sub (redis/go-redis v9)
└── examples/
```

## Abstraksi queue

```go
type Message struct {
    ID        string
    Key       []byte            // kunci urutan; diabaikan adapter tanpa ordering
    Body      []byte
    Headers   map[string]string
    Timestamp time.Time
}

type Publisher interface {
    Publish(ctx context.Context, topic string, msgs ...*Message) error
    Close() error
}

type Handler func(ctx context.Context, msg *Message) error // nil = ack, error = nack

type Subscriber interface {
    Subscribe(ctx context.Context, topic string, h Handler, opts ...SubscribeOption) error
    Close() error
}
```

- Semantik pengiriman: at-least-once.
- `SubscribeOption`: `Group`, `Concurrency`, `DeadLetter`.
- Middleware `func(Handler) Handler`: Retry (backoff), Recover, Logging (`slog`).
- Helper generik: `codec.PublishJSON[T]`, `codec.HandleJSON[T]`.
- Fitur yang tidak umum di semua broker (delay, priority) tidak masuk MVP.

### Pemetaan adapter

| Konsep | Kafka | RabbitMQ | SQS |
|---|---|---|---|
| topic | topic | exchange (topic) + routing key | nama queue (URL di-cache) |
| Group | consumer group | nama queue yang di-bind | tidak didukung (1 queue = 1 group) |
| ack | commit offset | `Ack` | `DeleteMessage` (batch) |
| nack | retry → publish ke topic DLQ | `Nack` → DLX | visibility diubah ke 0 |
| DLQ | dikelola adapter | DLX | redrive policy (infra) |
| Key | partition key | diabaikan | diabaikan (Standard) |
| Headers | record headers | headers | satu attribute JSON `godulkit-headers` |

### Catatan khusus SQS

- Long polling (`WaitTimeSeconds=20`, `MaxNumberOfMessages=10`), worker sebanyak `Concurrency`.
- Heartbeat `ChangeMessageVisibility` selama handler berjalan.
- `Publish` dipecah per 10 entry; kegagalan parsial dilaporkan per pesan.
- `Message.ID` disimpan di attribute; retry count dari `ApproximateReceiveCount`.

### Conformance test

`queuetest.Run(t, factory, queuetest.Capabilities{FanOut, Ordering})` menguji: publish/consume, ack/nack + redelivery, load balance dalam group, fan-out antar group (jika didukung), graceful shutdown, context cancel. Test yang tidak relevan di-skip berdasarkan capability.

Infra test via testcontainers-go: Kafka (Redpanda/Kafka), RabbitMQ, LocalStack/ElasticMQ.

## Abstraksi realtime

```go
type Broker interface {
    Publish(ctx context.Context, channel string, data []byte) error
    Subscribe(ctx context.Context, channel string) (<-chan []byte, func(), error)
}

func New(b Broker, opts ...Option) *Server
func (s *Server) Publish(ctx context.Context, channel string, data any) error
func (s *Server) Handler(t Transport) http.Handler
```

- Transport (client ↔ server): WebSocket (`coder/websocket`), SSE.
- Broker (antar instance): memory (single instance), Redis Pub/Sub (multi-instance; sharded pub/sub `SPUBLISH` untuk Redis Cluster).
- Hook: `Authenticate(r *http.Request)`, `Authorize(conn, channel)`.
- Protokol: `{"type":"subscribe|unsubscribe|message","channel":"...","data":...}`.
- Client lambat: buffer send per koneksi; bila penuh, koneksi diputus.
- `realtimetest` untuk conformance broker.

## Workflow development

- `go.work` dan `go.work.sum` masuk `.gitignore`. Developer menjalankan `make work` (`go work init` + `go work use -r .`) setelah clone.
- Tidak ada `replace` di `go.mod` yang di-commit.
- `go mod tidy` dijalankan per modul dengan `GOWORK=off`.
- CI menjalankan dua mode:
  - **workspace**: CI membuat `go.work` (`make work`), lalu test semua modul dengan kode terbaru.
  - **isolated**: per modul dengan `GOWORK=off`: cek `go.mod` rapi, lalu `go test ./...`. Meniru kondisi user.
- Rilis lintas modul: tag modul dasar dulu (`queue`, `realtime`), `go get` versi itu di adapter, lalu tag adapter. Sebelum v1 boleh pakai pseudo-version dari `main`.

## Tahapan

1. **Fondasi** — `.gitignore`, Makefile, CI (GitHub Actions, golangci-lint), interface `queue`, adapter memory, middleware, codec, `queuetest`.
2. **queue-kafka** — publisher, subscriber + consumer group, DLQ, `WithAutoCreate`, lulus conformance.
3. **queue-rabbitmq** — topologi exchange/queue, reconnect + channel recovery, DLX, `WithAutoCreate`, lulus conformance.
4. **queue-sqs** — Standard queue, long polling, heartbeat visibility, batching, `WithAutoCreate`, lulus conformance.
5. **realtime** — server, WebSocket, SSE, broker memory, hook auth, `realtimetest`.
6. **realtime-redis** — broker Redis Pub/Sub, lulus `realtimetest`, contoh chat 2 instance di belakang load balancer.
7. **Rilis v0.1.0** — README per modul, `examples/`, godoc, tag per modul.

## Setelah MVP

- SQS FIFO, SNS fan-out
- OpenTelemetry middleware
- Broker realtime: NATS, PostgreSQL LISTEN/NOTIFY, Redis Streams (history/replay)
- Realtime: presence, history
- Adapter queue lain: NATS JetStream, Redis Streams
