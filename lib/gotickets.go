package lib

import (
	"errors"
	"fmt"
)

//用于表示goroutine票池的接口
type GoTickets interface{
  //获得一张票
  Take()
  //归还一张票
  Return()
  //票池是否被激活
  Active() bool
  //票的总数
  Total() uint32
  //剩余的票数
  Remainder() uint32
}
//票池的实现
type myGoTickets struct{
	total uint32 //票的总数
	ticketCh chan struct{} //票的容器
	active bool //票池是否被激活
}

//初始化票池
func (gt *myGoTickets) init(total uint32) bool{
 //检查参数是否合法
 if gt.active{
   return false
 }

 if total==0{
   return false
 }
 //初始化通道，以载荷并发量作为缓冲数量
 ch:=make(chan struct{},total)
 u:=int(total)
 for i:=0;i<u;i++{
   ch<-struct{}{}
 }
 gt.ticketCh=ch
 gt.active=true
 gt.total=total
 return true
}
//创建票池
func NewGoticket(total uint32) (GoTickets,error){
  gt:=myGoTickets{}
  if !gt.init(total){
    errMsg:=fmt.Sprintf("The goroutine ticket can not be initialzed! (total:%d)",total)
    return nil,errors.New(errMsg)
  }
  return &gt,nil

}

//获得一张票
func (gt *myGoTickets) Take(){
   <-gt.ticketCh
}
//归还一张票
func (gt *myGoTickets) Return(){
  gt.ticketCh<-struct{}{}

}
//是否被激活
func (gt *myGoTickets) Active() bool{
   return gt.active

}
//票的总数
func (gt *myGoTickets) Total() uint32{
  return gt.total

}
//剩余的票
func (gt *myGoTickets) Remainder() uint32{
  return uint32(len(gt.ticketCh))
}