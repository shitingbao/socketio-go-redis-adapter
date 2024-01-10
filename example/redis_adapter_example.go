package main

import (
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	redis "github.com/shitingbao/socketio-go-redis-adapter/adapter"
	"github.com/zishang520/socket.io/v2/socket"
)

func cross(ctx *gin.Context) {
	// Whitelist customization
	allowedOrigins := []string{"http://localhost:3002", "http://localhost:3001", "http://localhost:3000"}
	origin := ctx.Request.Header.Get("Origin")
	// log.Println("origin=:", origin, " Referer:", ctx.Request.Referer()) origin or Referer
	for _, allowedOrigin := range allowedOrigins {
		if origin == allowedOrigin {
			ctx.Writer.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
			break
		}
	}

	ctx.Header("Access-Control-Allow-Headers", "Content-Type,AccessToken,X-CSRF-Token, Authorization,x-device-sn,x-device-token")
	ctx.Header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	ctx.Header("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers, Content-Type,x-device-sn")
	ctx.Header("Access-Control-Allow-Credentials", "true")
	if ctx.Request.Method == "OPTIONS" {
		ctx.JSON(http.StatusOK, "ok")
		return
	}
	ctx.Next()
}

func ExampleRedisAdapter() {
	// the node
	go ExampleRedisAdapterNode(":8002")
	go ExampleRedisAdapterNode(":8001")

	// the other node
	// these node can can discover each other in redisAdapterTest's system
	// redisAdapterTest is my example serverName
	// go ExampleRedisAdapterNode(":8002")
	// ...
	// ...
	// other node
	ExampleRedisAdapterNode(":8000")
	//
	// ...
	// ...
}

func ExampleRedisAdapterNode(address string) {
	g := gin.Default()

	// srv is listen's address or http server
	// opts *ServerOptions
	io := socket.NewServer(nil, nil)

	rdsAdapter, err := redis.NewRedisAdapter(
		redis.WithRedisAddress("127.0.0.1:6379"),
	)
	if err != nil {
		log.Println(err)
		return
	}
	io.SetAdapter(rdsAdapter)
	namespace := io.Of("/", nil)

	namespace.On("connection", func(clients ...any) {
		log.Println("connect")
		client := clients[0].(*socket.Socket)
		client.On("ping", func(datas ...any) {
			log.Println("heart")
			client.Emit("pong", "pong")
		})

		client.On("broadcast", func(datas ...any) {
			// example datas is [map[event:test message:asdf room:stb]]
			da, ok := datas[0].(map[string]interface{})
			if !ok {
				client.Emit("error", "data err")
				return
			}
			log.Println("da==:", da["event"], da["message"], da["room"])
			// io.To(socket.Room(da["room"].(string))).Emit("test", da["message"])
			// you can broadcast
			io.To(socket.Room(da["room"].(string))).Timeout(1000*time.Millisecond).Emit("test", da["message"])

			// or with ack
			// @review not supported at the moment
			// io.To(socket.Room(da["room"].(string))).Timeout(1000*time.Millisecond).Emit("test", da["message"], func(msg []any, err error) {
			// 	log.Println("ack rollback==:", msg, err)
			// })

		})
		client.On("users", func(datas ...any) {
			// get all socket
			// example datas is room
			room, ok := datas[0].(string)
			if !ok {
				client.Emit("error", "data err")
				return
			}
			fs := io.In(socket.Room(room)).FetchSockets()
			ids := []socket.SocketId{}
			fs(func(sks []*socket.RemoteSocket, err error) {
				if err != nil {
					log.Println(err)
					return
				}
				for _, sck := range sks {
					// log.Println("Handshake=:", sck.Handshake())
					ids = append(ids, sck.Id())
				}
			})
			client.Emit("pong", ids)
		})
		client.On("join-room", func(datas ...any) {
			// example datas is room
			log.Println("join-room datas:", datas)
			room, ok := datas[0].(string)
			if !ok {
				client.Emit("error", "data err")
				return
			}

			// You can use `socket id` to join a room
			// The id is not used directly to find the corresponding connection object.
			// It's because this `socket id` is added to a room with this id as the key when connecting,
			// and the id is guaranteed to be unique.
			// There is only this connection in this unique room,
			// and then it is added to your corresponding room.
			// for details, see the _onconnect method of the socket object
			io.In(socket.Room(client.Id())).SocketsJoin(socket.Room(room))
			// client.Join(socket.Room(room))
			// when you join room,can broadcast other room
		})

		client.On("leave-room", func(datas ...any) {
			// example datas is room
			log.Println("leave-room datas:", datas)
			room, ok := datas[0].(string)
			if !ok {
				client.Emit("error", "data err")
				return
			}

			// client.Leave()
			// or
			io.In(socket.Room(client.Id())).SocketsLeave(socket.Room(room))
			// or leave room
			// client.Leave(socket.Room(room))
		})
		client.On("disconnect", func(...any) {
			log.Println("disconnect")
		})
	})
	sock := io.ServeHandler(nil)

	g.Use(cross)
	g.GET("/socket.io/", gin.WrapH(sock))
	g.POST("/socket.io/", gin.WrapH(sock))
	g.Run(address)
}
