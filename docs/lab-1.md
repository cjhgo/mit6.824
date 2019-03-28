

### 实验1,MapReduce
使用golang实现一个MapReduce库

#### 代码的组织结构/系统架构

1. 提供初始化参数(输入文件,自定义函数)
应用程序提供一组输入文件,输入文件的数目隐式定义了nMap,一个map函数,一个reduce函数,reduce任务的次数(nReduce)
(nMap启动多少个doMap处理输入文件,nReduce启动多少个doReduce处理中间key-value文件)
2. 启动master,开启rpc服务
基于第一部分的信息系统创建一个master.然后master启动一个rpc服务,等待worker注册
第4/5步产生的任务进来之后,master开始schedule
3. master doMap
master把每个输入文件作为一个task.在每个map task上至少调用一次doMap函数
串行执行时,master直接调用,分布式执行时通过`DoTask`rpc把任务分发给worker
doMap函数的具体功能

doMap函数读取文件内容,然后把文件内容作为参数,调用用户提供的map函数,然后把产生的中间key-value写到
nReduce数目的中间文件中,写入时通过hash(key)来决定一对key-value写入到nReduce个中间文件中哪一个里边.
一此doMap产生nReduce个中间文件,所以最终有nMap*nReduce个中间文件.
4. master doReduce
master接下来在每个reduce task上至少(直接或rpc)调用一次`doReduce`函数
doReduce函数会调用用户提供的reduce函数.reduce 任务产生nReduce个结果文件
5.master调用`mr.merge`,把上一步产生的nReduce个文件合并到一起
6.master给每一个worker发送一个shutdown rpc,然后关闭自己的rpc服务




#### 第一部分,写一个简单的MapReduce程序

