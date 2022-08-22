##### memcacheq
- 支持命令get,set,delete,stats,stats queue等命令
- 支持子队列比如
```
    set a 0 0 5\r\nhello\r\n
    get a+1 // 中间使用+号代表子队列
    get a
```

> 目前全部基于内存存储，正在逐步添加持久化及对内存占用友好的存储机制
