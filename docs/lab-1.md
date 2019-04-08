

### 实验1,MapReduce
使用golang实现一个MapReduce库



#### wordcount算法的伪代码
```go
func map(){
  //这里word要以单词为单位
  //使用strings.FieldsFunc可以对string分词
  res :=[]KeyValue
  for word in contents
    //这里的value是字符串1
    res.append({"key":wor,"value":"1"})
  return res
}
func reduce(){
  res := 0
  for value in Values:
    res += int(value)
  return string(res)
}
```

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
    - doMap函数的具体逻辑
    ```go
    输入参数:
    inFile,要处理的文件
    mapTask这是第几个文件,也就是第几个map task
    nReduce一共有多少个reduce任务
    //假如本次mapTask=1,nReduce=3
    //那么产生的中间文件为1-0,1-1,1-2
    //如果一共有2个输入文件,也就是2个mapTask
    //那么最终产生,0-0,0-1,0-2,1-0,1-1,1-2,nMapxnReduce=2x3个中间文件
    处理逻辑
    //读取文件内容
    content := read from inFile
    //在文件内容上调用map函数,返回[]KeyValue
    res := mapF(content)
    //创建nReduce个中间文件
    outFiles := create nReduce intermediate  file
    for ele : res
      //计算ele按key hash到哪个中间文件
      index = hash(ele.key)
      //转换为json格式,写入到中间文件
      outFiles[index].write(json(ele))        
    一次doMap产生nReduce个中间文件,    
    所以最终有nMap*nReduce个中间文件.
    ```    
4. master doReduce
master接下来在每个reduce task上至少(直接或rpc)调用一次`doReduce`函数
doReduce函数会调用用户提供的reduce函数.reduce 任务产生nReduce个结果文件
    - doReduce函数的具体逻辑
    ```go
    输入参数:
    reduceTask,这是第几个reduce任务    	
	outFile 我要写入结果的文件名
	nMap一共有几个map任务
    //加入nMap=2,reduceTask=1
    //那么我要处理的是0-1,1-1这两个中间文件
    处理逻辑
    res := []KeyValue
    //从nMap个文件中读取json data stream,把结果放入res
    for i : nMap
      //根据nMap,reduceTask确定中间文件的名字
      fileName := reduceName(i, reduceTask)
      //从中间文件io流中创建json decoder,
      dec := json.newdecoder(os.open(fileNmae))
      for{
          //不断解码,把结果添加到res后边
          dec.Decode(&tmp)
          append(res, tmp)
      }
    //创建outFile文件,用于存放reduce的结果
    f := os.Create(outFile)
    //从outfile文件流中创建编码器
    enc := json.newencoder(f)
    //res里边现在是nMap个文件中的KeyValue,
    //按Key排序,排序之后相同Key的元素相邻存放
    sort res by Key
    //遍历排序之后的res,
    for ele : res
      //把相同Key的value合并为slice,传给reduce函数
      if ele.key != oldkey//遇到不等,说明key在此变化,前边的key都一样
        reducedValue = reduce(oldkey,sliceValues)
        //把reduce之后的结果写入文件
        enc.encode(oldkey,reducedValue)
    ```
5. master调用`mr.merge`,把上一步产生的nReduce个文件合并到一起
6. master给每一个worker发送一个shutdown rpc,然后关闭自己的rpc服务




#### 写一个简单的MapReduce程序
#### 分布式地执行mapreduce任务
在之前的实现中,一次只会运行一个map或reduce任务
MapReduce框架最大的卖点就是只要程序是按map-reduce编程模型写的
系统能够自动把普通的串行代码并行化

master.go
worker.go
common_rpc.go

schedule.go
master在执行过程两次调用`schedule`,一次调度map任务,一次调度reduce任务
shedule的功能是把任务分发给空闲的worker,
分发之后,schedule等待worker完成任务
schedule从`registerChan`这个参数中了解系统一共有多少个worker
schedule通过`Worker.DoTask`这样一个rpc调用让worker执行任务
common_rpc.go文件里定义了这个rpc调用的参数`DoTaskArgs`
schedule要通过common_rpc.go中的`call`函数来向worker发起rpc调用
格式是这样的`call(<rpc_address>,<rpc_name>,<rpc_arg>,<rpc_res>)`
从`registerChan`可以获取rpc_address
rpc_name是`worker.DoTask`,即
rpc_arg是`DoTaskArgs`类型的参数




##### 并行执行的流程

```go
//运行入口
//test-test.go中的这个函数并行执行mapreduce程序
func TestParallelBasic(t *testing.T) {
  //串行部分,准备工作
  mr := setup()
  {-------------
    //创建20个input文件
    files := makeInputs(nMap)
    //根据uid,pi创建Master的address
    master := port("master")
    //调用master.go中的这个Distributed函数,启动master
    mr := Distributed("test", files, nReduce, master)
    {-------------------------------------------------
      //创建master
      mr = newMaster(master)
      //调用位于master_rpc.go中的startRPCServermethod
      //启动master的rpc服务:接收来自worker的地址注册
      mr.startRPCServer()
      //调用master.go中run函数
      go mr.run(jobName, files, nreduce,
        //就地创建的schedule函数
        func(phase jobPhase) {
          ch := make(chan string)
          go mr.forwardRegistrations(ch)
            {--------------------------
              //i记录我们通过ch通知了多少个worker
              i := 0
              for {//死循环,要么用channel发消息,要么陷入等待
                mr.Lock()
                if len(mr.workers) > i {
                  // there's a worker that we haven't told schedule() about.
                  w := mr.workers[i]
                  go func() { ch <- w }() // send without holding the lock.
                  i = i + 1
                } else {
                  //已经通知完了所有的worker,通过条件变量陷入等待
                  //Master.Register事件的发生
                  mr.newCond.Wait()
                }
                mr.Unlock()
              }
            }
          //调用位于schedule.go中的schedule函数
          schedule(mr.jobName, mr.files, mr.nReduce, phase, ch)
          {----------------------------------------------------
            //for task go call
            //对每一个任务启动一个协程,来执行DoTask rpc
            go call(workers, "Worker.DoTask",args,nil)
            {-------------------------------------
              //此处是rpc调用,执行的是位于worker.go中的DoTask函数的源码
              switch arg.Phase 
              {
              case mapPhase:
                //调用位于common_map.go中的doMap函数
                doMap(arg.JobName, arg.TaskNumber, arg.File, arg.NumOtherPhase, wk.Map)
                {----------------------------------------------------------------------
                  //从文件中读取内容,用mapF函数处理                  
                  contents := read from infile                    
                  res := mapF(inFile, contents)//得到的res是Key-Value array
                  //创建nReduce个中间文件,并关联json.Encoder
                  encSlice :=make([]*json.Encoder,nReduce)
                  for i:=0; i < nReduce; i++
                  {
                    f=os.Create(filename_i)
                    encSlice[i]=json.NewEncoder(f)
                  }
                  //遍历res,对每个key用hash映射到某个文件
                  for _,ele := range res
                  {
                    r:=ihash(ele.Key)%nReduce
                    //json编码key-value,写入到文件
                    encSlice[r].Encode(&ele)
                  }
                }
              case reducePhase:
                //调用位于common_reduce.go中的doReduce函数
                doReduce(arg.JobName, arg.TaskNumber, mergeName(arg.JobName, arg.TaskNumber), arg.NumOtherPhase, wk.Reduce)
                {----------------------------------------------------------------------------------------------------------   
                  contents := []KeyValue{}
                  //从nMap个文件中读取key-value
                  //map阶段产生了nMapxnReduce个中间文件,根据我是第几个reduce任务,判断读取哪nMap个文件
                  for i:=0; i < nMap; i++
                  {                    
                    raw := os.Open(fnmaei)//打开文件
                    dec := json.NewDecoder(raw)//关联Decoder
                    for dec.Decode(&tmp)
                      contents = append(contents, tmp)//把解析出的key-value添加到KeyValue数组
                  }
                  //把从nMap个文件中读出的KeyValue数组按Key排序
                  sort.Slice(contents).byKey
                  outF:= os.Create(outFile)//创建输出文件
                  enc := json.NewEncoder(outF)//关联编码器
                  valuesToReduce := []string{contents[0].Value}
                  //遍历排序之后的KeyValue数组
                  for i := 1; i <len(contents); i++
                  {
                    //把同key的value连起来,用reduceF函数处理
                    reducedValue := reduceF(Key, Values)
                    enc.Encode({Key, reducedValue})//写入到输出文件
                  }
                }
              }
            }
            //等待协程完成,即等待任务完成,才能进入下一阶段
            wait()
          }
        },
        //就地创建的finish函数
        func() {
          mr.stats = mr.killWorkers()
          {--------------------------
            mr.Lock()defer mr.Unlock()
            ntasks := make([]int, 0, len(mr.workers))
            for _, w := range mr.workers 
            {
              debug("Master: shutdown worker %s\n", w)
              var reply ShutdownReply
              ok := call(w, "Worker.Shutdown", new(struct{}), &reply)
              {------------------------------------------------------
                //此处是rpc调用
                wk.Lock()defer wk.Unlock()
                res.Ntasks = wk.nTasks
                wk.nRPC = 1
              }
              ntasks = append(ntasks, reply.Ntasks)
            }
          }
          //调用位于maser_rpc.go中的stopRPCServermethod
          //关闭master的rpc服务          
          mr.stopRPCServer()
          {-----------------
          	var reply ShutdownReply
            ok := call(mr.address, "Master.Shutdown", new(struct{}), &reply)
            {-------------------------------------
              //此处是rpc调用
              close(mr.shutdown)//关闭channel
	            mr.l.Close() //关闭listener
            }
          }
        })
      {---------------------------------------------------------
        //执行map
        schedule(mapPhase)
        //执行reduce
        schedule(reducePhase)
        //执行参数中的finish函数
        finish()
        //把reduce的结果合并为一个
	      mr.merge()
	      mr.doneChannel <- true      
      }
    }
  }
  //两次执行位于worker.go中的RunWorker函数
  for i := 0; i < 2; i++ 
  {
    //RunWorker和
		go RunWorker(mr.address, port("worker"+strconv.Itoa(i)),
      MapFunc, ReduceFunc, -1, nil)
      {-----------------------------------------------------
        wk := new(Worker)
        wk.init(name,Map,Reduce,nRpc,parallelism)
        rpcs := rpc.NewServer()
        rpcs.Register(wk)        
        l, e := net.Listen("unix", me)        
        wk.l = l
        wk.register(MasterAddress)
        {--------------------------
          args := new(RegisterArgs)
          args.Worker = wk.name
          //worker把自己的地址作为args参数通过rpc调用注册给master
          ok := call(master, "Master.Register", args, new(struct{}))
          {---------------------------------------------------------
            //此处是位于master.go上Register函数的远程源码,这正是rpc的意义
            //存在多个worker通过这个rpc调用修改workers数组的可能,所以要加锁
            mr.Lock()defer mr.Unlock()
            //向master的workers数组添加自己的信息
	          mr.workers = append(mr.workers, args.Worker)
            //通过条件变量告诉forwardRegistrations函数,有新的worker可以通知
            mr.newCond.Broadcast()
          }
        }
        for {//死循环
          wk.Lock()
          if wk.nRPC == 0 {
            wk.Unlock()
            break
          }
          wk.Unlock()
          //worker侦听-接收-处理进入的链接
          conn, err := wk.l.Accept()
          if err == nil {
            wk.Lock()
            wk.nRPC--
            wk.Unlock()
            go rpcs.ServeConn(conn)
          } else {
            break
          }
        }
        wk.l.Close()     
      }
	}
  mr.Wait()
  ---------
    <-mr.doneChannel
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}
```


