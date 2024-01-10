package main

import (
	"log"

	"github.com/gin-gonic/gin"
	"github.com/zishang520/socket.io/v2/socket"
)

func ExampleGinSocket() {
	g := gin.Default()
	io := socket.NewServer(nil, nil)

	io.Of("/user", nil).On("connection", func(clients ...any) {
		log.Println("connect")
		client := clients[0].(*socket.Socket)
		client.On("ping", func(datas ...any) {
			log.Println("heart")
			client.Emit("pong", "pong")
		})
		client.On("disconnect", func(...any) {
			log.Println("disconnect")
		})
	})
	sock := io.ServeHandler(nil)

	// g.Use(cross)
	g.GET("/socket.io/", gin.WrapH(sock))
	g.POST("/socket.io/", gin.WrapH(sock))
	g.Run(":8000")
}
