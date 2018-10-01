package testhelper

import(
	loadlib "LoadGenerator/lib"
	"time"
	"math/rand"
	"encoding/json"
	"bytes"
	"net"
	"bufio"
	"fmt"
)

const (
	DELIM = '\n' // 分隔符。
)

var operators=[]string{"+","-","*","/"}

//用于表示TCP通信器的结构
type TCPComm struct{
	addr string //IP地址：端口号
}

// NewTCPComm 会新建一个TCP通讯器。
func NewTCPComm(addr string) loadlib.Caller {
	return &TCPComm{addr: addr}
}




//构建一个请求
func (comm *TCPComm) BuildReq() loadlib.RawReq{
	id:=time.Now().UnixNano()
	sreq:=ServerReq{
		ID:id,
		Operands:[]int{
			int(rand.Int31n(1000)+1),
			int(rand.Int31n(1000)+1),
		},
		Operator: func() string {
			return operators[rand.Int31n(100)%4]
		}(),
	}
	bytes,err:=json.Marshal(sreq)
	if err!=nil{
		panic(err)
	}
	rawReq:=loadlib.RawReq{ID:id,Req:bytes}
	return rawReq
}

//发起一次通信
func (comm *TCPComm) Call(req []byte,timeoutNS time.Duration) ([]byte,error){
	conn,err:=net.DialTimeout("tcp",comm.addr,timeoutNS)
	if err!=nil{
	 return nil,err
	}
	_,err=write(conn,req,DELIM)
	if err!=nil{
		return nil,err
	}
	return read(conn,DELIM)

}

//检查响应内容
func (comm *TCPComm) CheckResp(rawReq loadlib.RawReq,rawResp loadlib.RawResp) *loadlib.CallResult{
	//对调用结果进行初始化
	var commResult loadlib.CallResult
	commResult.ID=rawResp.ID
	commResult.Req=rawReq
	commResult.Resp=rawResp

    //数据转换成结构体类型
	var sreq ServerReq
	err:=json.Unmarshal(rawReq.Req,&sreq)

	//检查发生错误返回以下结果
	if err!=nil{
		commResult.Code=loadlib.RET_CODE_FATAL_CALL
		commResult.Msg=fmt.Sprintf("Incorrectly formatted Req : %s \n",string(rawReq.Req))
		return &commResult
	}

	var sresp ServerResp
	err=json.Unmarshal(rawResp.Resp,&sresp)
	if err!=nil{
		commResult.Code=loadlib.RET_CODE_ERROR_RESPONSE
		commResult.Msg=fmt.Sprintf("Incorrectly formatted Resp : %s",string(rawResp.Resp))
		return &commResult
	}

	if sresp.ID != sreq.ID {
		commResult.Code = loadlib.RET_CODE_ERROR_RESPONSE
		commResult.Msg =
			fmt.Sprintf("Inconsistent raw id! (%d != %d)\n", rawReq.ID, rawResp.ID)
		return &commResult
	}
	if sresp.Err != nil {
		commResult.Code = loadlib.RET_CODE_ERROR_CALEE
		commResult.Msg =
			fmt.Sprintf("Abnormal server: %s!\n", sresp.Err)
		return &commResult
	}
	if sresp.Result != op(sreq.Operands, sreq.Operator) {
		commResult.Code = loadlib.RET_CODE_ERROR_RESPONSE
		commResult.Msg =
			fmt.Sprintf(
				"Incorrect result: %s!\n",
				genFormula(sreq.Operands, sreq.Operator, sresp.Result, false))
		return &commResult
	}
	commResult.Code = loadlib.RET_CODE_SUCCESS
	commResult.Msg = fmt.Sprintf("Success. (%s)", sresp.Formula)
    return &commResult

}


// read 会从连接中读数据直到遇到参数delim代表的字节。
func read(conn net.Conn, delim byte) ([]byte, error) {
	readBytes := make([]byte, 1)
	var buffer bytes.Buffer
	for {
		_, err := conn.Read(readBytes)
		if err != nil {
			return nil, err
		}
		readByte := readBytes[0]
		if readByte == delim {
			break
		}
		buffer.WriteByte(readByte)
	}
	return buffer.Bytes(), nil
}

// write 会向连接写数据，并在最后追加参数delim代表的字节。
func write(conn net.Conn, content []byte, delim byte) (int, error) {
	writer := bufio.NewWriter(conn)
	n, err := writer.Write(content)
	if err == nil {
		writer.WriteByte(delim)
	}
	if err == nil {
		err = writer.Flush()
	}
	return n, err
}
 


