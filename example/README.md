# 一.socket.io-go-redis-adapter

1. adapter ts 参考 [socket.io-adapter](https://github.com/socketio/socket.io-adapter)
1. redis-adapter 参考 [socket.io-redis-adapter](https://github.com/socketio/socket.io-redis-adapter)
1. golang socket.io 参考 [github.com/zishang520/socket.io](https://github.com/zishang520/socket.io)

# 二.目的

这个 redis 适配器是为了给 `github.com/zishang520/socket.io` 增加一个适配器 ，因为原来没有

# 三.例子和使用

参考完整代码 [example]()

# 四.redis adapter 过程简要

## 通讯方法

1.1 还是和 ts [socket.io-adapter](https://github.com/socketio/socket.io-adapter)中实现的一样 ,在不同节点使用 redis 的订阅模式来进行通讯;简单的来说，就是每次在一个节点上的动作，会通过订阅通道，通知其他节点，但是连接只会保持在本地，只是做动作的同步，让其他节点知道干了什么

1.2 而多节点布局使用 nginx.看这个原本的例子[reverse-proxy-nginx](https://socket.io/zh-CN/docs/v4/reverse-proxy/#nginx);  
或者看这个[nginx_config_test](),这是一个简单的使用例子，根据你的需求，应该做一些细微的更改
