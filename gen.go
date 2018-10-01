package loadgenerator

import (
	"errors"
	"sync/atomic"
	"fmt"
	"math"
	"bytes"
	"context"
	"LoadGenerator/lib"
	"time"
	"LoadGenerator/helper/log"
	
)


// 日志记录器。
var logger = log.DLogger()

// myGenerator 代表载荷发生器的实现类型。
type myGenerator struct {
	caller      lib.Caller           // 调用器。
	timeoutNS   time.Duration        // 处理超时时间，单位：纳秒。
	lps         uint32               // 每秒载荷量。
	durationNS  time.Duration        // 负载持续时间，单位：纳秒。
	concurrency uint32               // 载荷并发量。
	tickets     lib.GoTickets        // Goroutine票池。
	ctx         context.Context      // 上下文。
	cancelFunc  context.CancelFunc   // 取消函数。
	callCount   int64                // 调用计数。
	status      uint32               // 状态。
	resultCh    chan *lib.CallResult // 调用结果通道。
}

// NewGenerator 会新建一个载荷发生器。
func NewGenerator(pset ParamSet) (lib.Generator, error) {

	logger.Infoln("New a load generator...")
	if err := pset.Check(); err != nil {
		return nil, err
	}
	gen:= &myGenerator{
		caller:     pset.Caller,
		timeoutNS:  pset.TimeoutNS,
		lps:        pset.LPS,
		durationNS: pset.DurationNS,
		status:     lib.STATUS_ORIGINAL,
		resultCh:   pset.ResultCh,
	}
	if err := gen.init(); err != nil {
		return nil, err
	}
	return gen, nil
}

//初始化载荷发生器
func (gen *myGenerator) init() error{
	//通过内容写入缓冲器尾部
	var buf bytes.Buffer
	buf.WriteString("Initializing the load generator...")
	//载荷并发量≈载荷响应时间/载荷发送时间
	var total64=int64(gen.timeoutNS)/int64(1e9/gen.lps)+1
   if total64>math.MaxInt32{
	   total64=math.MaxInt32
   }
   //载荷并发量
   gen.concurrency=uint32(total64)
   //创建票池
   tickets,err:=lib.NewGoticket(gen.concurrency)
   if err!=nil{
	   return err
   }
   //票池赋值
   gen.tickets=tickets
   //通过内容写入缓冲器尾部 表示实例化完成
   buf.WriteString(fmt.Sprintf("Done . (concurrency=%d)",gen.concurrency))
   //记录日志
   logger.Infoln(buf.String())
   return nil
}
//callOne 会向载荷承受方发起一次调用


func (gen *myGenerator) Start() bool{
   logger.Infoln("Starting load generator...")
  //检查发生器是否为可启动状态
  if !atomic.CompareAndSwapUint32(&gen.status,lib.STATUS_ORIGINAL,lib.STATUS_STARTING){
	  if !atomic.CompareAndSwapUint32(&gen.status,lib.STATUS_STOPPED,lib.STATUS_STARTING){
		  return false
	  }
  }

  //设置节流阀
  var throttle <-chan time.Time
  if gen.lps>0{
	  interval:=time.Duration(1e9/gen.lps)
	  logger.Infof("Setting throttle (%v)...",interval)
	  throttle=time.Tick(interval)
  }

   //初始化上下文和取消函数  gen.durationNS表示负载持续时间，触达超时时间会发送取消信号
	gen.ctx,gen.cancelFunc=context.WithTimeout(context.Background(),gen.durationNS)
	
	//初始化调用计数
	gen.callCount=0

	//设置状态为已启动
	atomic.StoreUint32(&gen.status,lib.STATUS_STARTED)


    go func (){
		//TODO:生成并发送载荷
		logger.Infoln("Generating loads...")
		gen.genLoad(throttle)
		logger.Infof("Stopped. (call count: %d)", gen.callCount)
	}()
	return true
}
//手动停止
func (gen *myGenerator) Stop() bool{
     if !atomic.CompareAndSwapUint32(&gen.status,lib.STATUS_STARTED,lib.STATUS_STOPPING){
		 return false
	 }
	 gen.cancelFunc()
	 for{
		 if atomic.LoadUint32(&gen.status)==lib.STATUS_STOPPED{
			 break
		 }
		 time.Sleep(time.Microsecond)
	 }

	return true
}

func (gen *myGenerator) Status() uint32{
	return atomic.LoadUint32(&gen.status)
}

func (gen *myGenerator) CallCount() int64{
	return atomic.LoadInt64(&gen.callCount)
}


//产生载荷并向承受方发送
func (gen *myGenerator) genLoad(throttle <-chan time.Time){
   for{
	select {
	//告诉阻塞函数它应该在超时过后放弃它的工作。
	case <-gen.ctx.Done():
		//TODO:设置发生器状态停止，并关闭通道
		gen.prepareToStop(gen.ctx.Err())
		return
	default:
	}
	//异步调用票池的获得及归还操作
	gen.asyncCall()
	if gen.lps>0{
       select{
	   case <-throttle:
	   case <-gen.ctx.Done():
		//TODO：设置发生器状态停止，并关闭通道
		gen.prepareToStop(gen.ctx.Err())
		return
	   }
	}

   }
}

//停止发生器
func (gen *myGenerator) prepareToStop(ctxError error){
	//TODO：记录日志 及错误
	logger.Infof("Prepare to stop load generator (cause: %s)...", ctxError)
	atomic.CompareAndSwapUint32(&gen.status,lib.STATUS_STARTED,lib.STATUS_STOPPING)
	//TODO：记录日志
	logger.Infof("Closing result channel...")
	close(gen.resultCh)
	atomic.StoreUint32(&gen.status,lib.STATUS_STOPPED)
	
}

// printIgnoredResult 打印被忽略的结果。
func (gen *myGenerator) printIgnoredResult(result *lib.CallResult, cause string) {
	resultMsg := fmt.Sprintf(
		"ID=%d, Code=%d, Msg=%s, Elapse=%v",
		result.ID, result.Code, result.Msg, result.Elapse)

	logger.Warnf("Ignored result: %s. (cause: %s)\n", resultMsg, cause)
}
//异步调用
func (gen *myGenerator) asyncCall(){
	gen.tickets.Take()
	go func(){
     defer func(){
		 if p:=recover();p!=nil{
			 err,ok:=interface{}(p).(error)
			 var errMsg string
			 if ok{
                 errMsg=fmt.Sprintf("Async Call Panic! (error:%s)",err)
			 }else{
				errMsg = fmt.Sprintf("Async Call Panic! (clue: %#v)", p)
			 }
			 //TODO：日志记录
			 logger.Errorln(errMsg)
			 result:=&lib.CallResult{
				 ID:-1,
				 Code:lib.RET_CODE_FATAL_CALL,
				 Msg:errMsg,
			 }
			gen.sentResult(result)
		 }
       gen.tickets.Return()
	 }()
	 //
	 rawReq:=gen.caller.BuildReq()
	 //调用状态:0 未调用或调用中；1 调用完成；2调用超时
	 var callStatus uint32
	 //
	 timer:=time.AfterFunc(gen.timeoutNS,func(){
       if !atomic.CompareAndSwapUint32(&callStatus,0,2){
		   return
	   }
	   result:=&lib.CallResult{
		   ID:rawReq.ID,
		   Req:rawReq,
		   Code:lib.RET_CODE_WARNING_CALL_TIMEOUT,
		   Msg:fmt.Sprintf("Timeout (expected: < %v)",gen.timeoutNS),
		   Elapse:gen.timeoutNS,
	   }
	   gen.sentResult(result)
         
	 })
	  rawResp:=gen.callOne(&rawReq)
	  if !atomic.CompareAndSwapUint32(&callStatus,0,1){
		  return
	  }
	  timer.Stop()

	  var result *lib.CallResult
      if rawResp.Err!=nil{
		  result=&lib.CallResult{
			  ID:rawReq.ID,
			  Req:rawReq,
			  Code:lib.RET_CODE_ERROR_CALL,
			  Msg:rawResp.Err.Error(),
			  Elapse:rawResp.Elapse,
		  }
		  
	  }else{
		result=gen.caller.CheckResp(rawReq,*rawResp)
		result.Elapse=rawResp.Elapse
	  }
	  gen.sentResult(result)
	}()

}
//发送调用结果
func (gen *myGenerator) sentResult(result *lib.CallResult) bool{
   if atomic.LoadUint32(&gen.status)!=lib.STATUS_STARTED{
	   
	    //TODO:发送阻塞结果
		gen.printIgnoredResult(result,"stopped load generator")
	    return false
   }
   select{
    case gen.resultCh<-result:
	    return true
	default:
		//TODO:发送阻塞结果
		gen.printIgnoredResult(result,"full result channel")
		return false
   }
}
//检查载荷响应
func (gen *myGenerator) callOne(rawReq *lib.RawReq) *lib.RawResp{
	  atomic.AddInt64(&gen.callCount,1)
	  if rawReq==nil{
		  return &lib.RawResp{ID:-1,Err:errors.New("Invalid raw request.")}
	  }
	  start:=time.Now().UnixNano() //当前时刻纳秒
	  resp,err:=gen.caller.Call(rawReq.Req,gen.timeoutNS)
	  end:=time.Now().UnixNano()
	  elapsedTime:=time.Duration(end-start)
	  var rawResp lib.RawResp
	  if err!=nil{
		   errMsg:=fmt.Sprintf("Sync call error:%s",err)
		   rawResp=lib.RawResp{
			   ID:rawReq.ID,
			   Err:errors.New(errMsg),
			   Elapse:elapsedTime,
		   }
	  }else{
		rawResp=lib.RawResp{
			ID:rawReq.ID,
			Resp:resp,
			Elapse:elapsedTime,
		}
	  }
	  return &rawResp
}


