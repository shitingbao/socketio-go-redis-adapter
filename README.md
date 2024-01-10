# 一.socket.io-go-redis-adapter

[中文](https://github.com/shitingbao/socketio-go-redis-adapter/blob/main/example/README.md)

1. see TypeScript adapter from [socket.io-adapter](https://github.com/socketio/socket.io-adapter)
2. see TypeScript redis-adapter from [socket.io-redis-adapter](https://github.com/socketio/socket.io-redis-adapter)
3. see go socket.io from [github.com/zishang520/socket.io](https://github.com/zishang520/socket.io)

# 二.Development

This project is to add an redis adapter to [github.com/zishang520/socket.io](https://github.com/zishang520/socket.io)  
Note that I am using his V2 version here (including the packages used in it)

# 三.example

look code [example](https://github.com/shitingbao/socketio-go-redis-adapter/blob/main/example/redis_adapter_example.go)

# 四.redis adapter process

## communication method

1.1 Use this method as before [socket.io-adapter](https://github.com/socketio/socket.io-adapter),Communication between nodes use the redis subscription model;To put it simply, every action on a node will be notified to other nodes through the subscription channel, but the connection will only remain local, and it will only synchronize the action to let other nodes know what has been done.  
1.2 And Multi-node layout uses nginx.see [reverse-proxy-nginx](https://socket.io/zh-CN/docs/v4/reverse-proxy/#nginx) or see [nginx_config_test](https://github.com/shitingbao/socketio-go-redis-adapter/blob/main/adapter/nginx_config_test),This is just a simple example. For specific situations, you need to add logic to your project yourself.
