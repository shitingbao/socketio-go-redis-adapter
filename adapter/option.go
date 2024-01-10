package adapter

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/zishang520/engine.io/v2/events"
	"github.com/zishang520/engine.io/v2/types"
	"github.com/zishang520/socket.io-go-parser/v2/parser"
	"github.com/zishang520/socket.io/v2/socket"
)

const (
	// MessageType
	INITIAL_HEARTBEAT SocketDataType = iota + 1
	HEARTBEAT
	BROADCAST
	SOCKETS_JOIN
	SOCKETS_LEAVE
	DISCONNECT_SOCKETS
	FETCH_SOCKETS
	FETCH_SOCKETS_RESPONSE
	SERVER_SIDE_EMIT
	SERVER_SIDE_EMIT_RESPONSE
	BROADCAST_CLIENT_COUNT
	BROADCAST_ACK

	// RequestType
	SOCKETS SocketDataType = iota + 1
	ALL_ROOMS
	REMOTE_JOIN
	REMOTE_LEAVE
	REMOTE_DISCONNECT
	REMOTE_FETCH
	Request_SERVER_SIDE_EMIT
	Request_BROADCAST
	Request_BROADCAST_CLIENT_COUNT
	Request_BROADCAST_ACK
)

type option struct {
	Address                          string
	Passsword                        string
	ServerId                         string
	Db                               int
	HeartbeatInterval                int
	HeartbeatTimeout                 int
	RequestsTimeout                  time.Duration
	PublishOnSpecificResponseChannel bool
}

var (
	HandMessagePool sync.Pool
	RequestIdPool   sync.Pool
)

func init() {
	HandMessagePool.New = func() interface{} {
		return &HandMessage{}
	}

	RequestIdPool.New = func() interface{} {
		return uuid.New()
	}
}

type Option func(*option)

type SocketDataType int

// WithRedisAddress eg : 127.0.0.1:6379
func WithRedisAddress(ads string) Option {
	return func(o *option) {
		o.Address = ads
	}
}

func WithRedisDb(db int) Option {
	return func(o *option) {
		o.Db = db
	}
}

func WithRedisHeartbeatInterval(tm int) Option {
	return func(o *option) {
		o.HeartbeatInterval = tm
	}
}

func WithRedisHeartbeatTimeout(tm int) Option {
	return func(o *option) {
		o.HeartbeatTimeout = tm
	}
}

type RedisAdapter struct {
	events.EventEmitter

	// serverId should be a unique identifier in the system to guide all nodes to join their own services.
	uid string // only uid

	// The number of ms between two heartbeats.
	// 5000
	HeartbeatInterval int

	// The number of ms without heartbeat before we consider a node down.
	// 10000
	HeartbeatTimeout int

	rdb *redis.Client
	ctx context.Context

	adapter socket.Adapter
	nsp     socket.NamespaceInterface
	rooms   *types.Map[socket.Room, *types.Set[socket.SocketId]]
	// sids    *types.Map[socket.SocketId, *types.Set[socket.Room]]

	_broadcast func(*parser.Packet, *socket.BroadcastOptions)

	requestsTimeout                  time.Duration // Multi-node response timeout
	publishOnSpecificResponseChannel bool

	channel                 string
	requestChannel          string
	responseChannel         string
	specificResponseChannel string
	requests                sync.Map
	ackRequests             sync.Map
	redisListeners          sync.Map //  map[string](func(channel, msg string))

	Subs  []*redis.PubSub
	PSubs []*redis.PubSub
}

func NewRedisAdapter(opts ...Option) (*RedisAdapter, error) {
	op := &option{
		Address:           "127.0.0.1:6379",
		HeartbeatInterval: 5000,
		HeartbeatTimeout:  10000,
		RequestsTimeout:   time.Second * 5,
	}
	for _, o := range opts {
		o(op)
	}

	r := redis.NewClient(&redis.Options{
		Addr:     op.Address,
		Password: op.Passsword,
		DB:       op.Db,
	})
	if err := r.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	return &RedisAdapter{
		rdb:               r,
		ctx:               context.Background(),
		uid:               uuid.New(),
		HeartbeatInterval: op.HeartbeatInterval,
		HeartbeatTimeout:  op.HeartbeatTimeout,

		requestsTimeout:                  op.RequestsTimeout,
		publishOnSpecificResponseChannel: op.PublishOnSpecificResponseChannel,

		requests:       sync.Map{},
		ackRequests:    sync.Map{}, // make(map[string]AckRequest),
		redisListeners: sync.Map{}, // make(map[string](func(string, string))),
	}, nil
}

// HandMessage message processing unit
// use sync.pool Recycle
type HandMessage struct {
	LocalHandMessage
	Channal   chan any     `json:"channal"` // 接受其他节点反馈的内容通道 socket 和 data
	MsgCount  atomic.Int32 `json:"msg_count"`
	CloseFlag atomic.Int32 `json:"close_flag"` // 关闭 HandMessage channel 的标志
}

type RemoteSocket struct {
	Id        socket.SocketId         `json:"id"`
	Handshake *socket.Handshake       `json:"handshake"`
	Rooms     *types.Set[socket.Room] `json:"rooms"`
	Data      any                     `json:"data"`
}

// this is the redis‘s information passed between channels
type LocalHandMessage struct {
	// Uid is each service unique id
	Uid string `json:"uid"`
	// Sid is socket id
	Sid         socket.SocketId             `json:"sid"`
	Type        SocketDataType              `json:"type"`
	RequestId   string                      `json:"request_id"` // every request id
	Rooms       []socket.Room               `json:"rooms"`
	Opts        *socket.BroadcastOptions    `json:"opts"`
	Close       bool                        `json:"close"`
	Sockets     []RemoteSocket              `json:"sockets"` // bool or []socket.Socket
	SocketIds   *types.Set[socket.SocketId] `json:"socket_ids"`
	Packet      *parser.Packet              `json:"packet"`
	ClientCount uint64                      `json:"client_count"`
	Responses   []any                       `json:"responses"`
	Data        any                         `json:"data"`
}

type AckRequest interface {
	ClientCountCallback(clientCount uint64)
	Ack([]any, error)
}

type ackRequest struct {
	ClientCountCallbackFun func(clientCount uint64)
	AckFun                 func([]any, error)
}

func (a *ackRequest) ClientCountCallback(clientCount uint64) {
	a.ClientCountCallbackFun(clientCount)
}

func (a *ackRequest) Ack(da []any, err error) {
	a.AckFun(da, err)
}

// 用于接受远程 socket 对象数据，兼容 interface json
type localRemoteSocket struct {
	id        socket.SocketId
	handshake *socket.Handshake
	rooms     *types.Set[socket.Room]
	data      any
}

func (r *localRemoteSocket) Id() socket.SocketId {
	return r.id
}

func (r *localRemoteSocket) Handshake() *socket.Handshake {
	return r.handshake
}

func (r *localRemoteSocket) Rooms() *types.Set[socket.Room] {
	return r.rooms
}

func (r *localRemoteSocket) Data() any {
	return r.data
}

func (h *HandMessage) Recycle() {
	h.Channal = make(chan any, 1)
	h.MsgCount = atomic.Int32{}
	h.CloseFlag = atomic.Int32{}
	h.Uid = ""
	h.Sid = ""
	h.Type = -1
	h.RequestId = ""
	h.Rooms = []socket.Room{}
	h.Opts = &socket.BroadcastOptions{}
	h.Close = false
	h.Sockets = []RemoteSocket{}
	h.SocketIds = &types.Set[socket.SocketId]{}
	h.Packet = &parser.Packet{}
	h.ClientCount = 0
	h.Responses = []any{}
	h.Data = nil
	HandMessagePool.Put(h)
}

// type set can not json ，temporary processing
// @review
type LocalHandMessageJson struct {
	Uid       string          `json:"uid"`
	Sid       socket.SocketId `json:"sid"`
	Type      SocketDataType  `json:"type"`
	RequestId string          `json:"request_id"`
	Rooms     []socket.Room   `json:"rooms"`
	Opts      struct {
		Rooms  map[socket.Room]types.Void `json:"rooms,omitempty"`
		Except map[socket.Room]types.Void `json:"except,omitempty"`
		Flags  *socket.BroadcastFlags     `json:"flags,omitempty"`
	} `json:"opts"`
	Close       bool                           `json:"close"`
	Sockets     []RemoteSocket                 `json:"sockets"` // bool or []socket.Socket
	SocketIds   map[socket.SocketId]types.Void `json:"socket_ids"`
	Packet      *parser.Packet                 `json:"packet"`
	ClientCount uint64                         `json:"client_count"`
	Responses   []any                          `json:"responses"`
	Data        any                            `json:"data"`
}

func (l LocalHandMessage) MarshalJSON() ([]byte, error) {
	da := LocalHandMessageJson{}
	da.Uid = l.Uid
	da.Sid = l.Sid
	da.Type = l.Type
	da.RequestId = l.RequestId
	da.Rooms = l.Rooms
	da.Opts = struct {
		Rooms  map[socket.Room]types.Void `json:"rooms,omitempty"`
		Except map[socket.Room]types.Void `json:"except,omitempty"`
		Flags  *socket.BroadcastFlags     `json:"flags,omitempty"`
	}{}
	if l.Opts != nil {
		da.Opts.Flags = l.Opts.Flags
	}

	if l.Opts != nil && l.Opts.Rooms != nil {
		da.Opts.Rooms = l.Opts.Rooms.All()
	}
	if l.Opts != nil && l.Opts.Except != nil {
		da.Opts.Except = l.Opts.Except.All()
	}

	da.Close = l.Close
	da.Sockets = l.Sockets

	if l.SocketIds != nil {
		da.SocketIds = l.SocketIds.All()
	}

	da.Packet = l.Packet
	da.ClientCount = l.ClientCount
	da.Responses = l.Responses
	da.Data = l.Data
	return json.Marshal(da)
}

func (h *LocalHandMessage) UnmarshalJSON(data []byte) error {
	da := LocalHandMessageJson{}
	if err := json.Unmarshal(data, &da); err != nil {
		return err
	}
	h.Uid = da.Uid
	h.Sid = da.Sid
	h.Type = da.Type
	h.RequestId = da.RequestId
	h.Rooms = da.Rooms

	h.Opts = &socket.BroadcastOptions{}
	h.Opts.Rooms = types.NewSet[socket.Room]()
	for k := range da.Opts.Rooms {
		h.Opts.Rooms.Add(k)
	}

	h.Opts.Except = types.NewSet[socket.Room]()
	for k := range da.Opts.Except {
		h.Opts.Except.Add(k)
	}
	h.Opts.Flags = da.Opts.Flags
	h.Close = da.Close
	h.Sockets = da.Sockets

	h.SocketIds = types.NewSet[socket.SocketId]()
	for k := range da.SocketIds {
		h.SocketIds.Add(k)
	}

	h.Packet = da.Packet
	h.ClientCount = da.ClientCount
	h.Responses = da.Responses
	h.Data = da.Data
	return nil
}
