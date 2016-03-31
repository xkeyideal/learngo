package main

import (
    "github.com/streadway/amqp"
    "fmt"
    "os"
    "time"
    "syscall"
    "os/signal"
)

const (
    username string = "xukai"
    passwd string = "xukai"
    vhost string = "testmq"
    port string = "5672"
    host string = "localhost"
    ExchangeForRecv = "response"

    ExchangeForSend = "send_msg"
    RoutingKey1 = "testmipub.server1"
    RoutingKey2 = "testmipub.server2"

    RespDefaultQueue = "testmq_recv"
)

type MQHandler struct {
    conn *amqp.Connection
    recvCh *amqp.Channel
    sendCh *amqp.Channel
    queue amqp.Queue
}

func NewMQHandler() (*MQHandler, error){
    conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/%s", username, passwd, host, port, vhost))
    if err != nil {
        fmt.Println(err.Error())
        return nil,err
    }

    recvCh, err := conn.Channel()
    if err != nil {
        fmt.Println(err.Error())
        return nil,err
    }

    sendCh, err := conn.Channel()
    if err != nil {
        fmt.Println(err.Error())
        return nil,err
    }

    err = recvCh.ExchangeDeclare(
        ExchangeForSend,
        "topic",
        true,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        fmt.Println(err.Error())
        return nil,err
    }

    queueName := "test_recv_msg2"
    queue, err := recvCh.QueueDeclare(
        queueName,
        true,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        fmt.Println(err.Error())
        return nil,err
    }

    err = recvCh.QueueBind(
            queueName,
            RoutingKey2,
            ExchangeForSend,
            false,
            nil,
        )
    if err != nil {
        fmt.Println(err.Error())
        return nil,err
    }

    err = sendCh.ExchangeDeclare(
        ExchangeForRecv,
        "topic",
        true,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        fmt.Println(err.Error())
        return nil,err
    }

    mqHandler := &MQHandler{
            conn:   conn,
            recvCh: recvCh,
            sendCh: sendCh,
            queue:  queue,
    }
    return mqHandler, nil
}

func (mq *MQHandler) Send(routingKey string, body []byte) error {
    err := mq.sendCh.Publish(
        ExchangeForRecv,
        routingKey,
        false,
        false,
        amqp.Publishing{
            ContentType: "text/plain",
            Body:        body,
        })
    if err != nil {
        fmt.Sprintf("[MQ] Send Message To %s Fail: %s\n", routingKey, err.Error())
    }
    return err
}

func (mq *MQHandler) ConsumeMsg() error {
    msgs, err := mq.recvCh.Consume(mq.queue.Name,"",true,false,false,false,nil)
    if err != nil {
      return err
    }
    for msg := range msgs {
        fmt.Printf("%s, %s\n",string(msg.Body),msg.RoutingKey)
        //fmt.Println(string(msg.Body))
        fmt.Println("===============")
    }
    return nil
}

func (mq *MQHandler) Close() {
  mq.conn.Close()
  mq.recvCh.Close()
  mq.sendCh.Close()
}

func main(){
    var err error
    mqhandler, err := NewMQHandler()
    if err != nil {
        os.Exit(1)
    }
    go mqhandler.ConsumeMsg()
    var RoutingKey = "test.recv.response"

    ticker := time.NewTicker(10 * time.Second)
    for _ = range ticker.C {
        //str1 := fmt.Sprintf("hello world1: %d", time.Now().Unix())
        str2 := fmt.Sprintf("hello world2: %d", time.Now().Unix())
        //mqhandler.Send(RoutingKey,[]byte(str1));
        mqhandler.Send(RoutingKey,[]byte(str2));
    }
    //mqhandler.Send(RoutingKey,[]byte("hello world"));
    //mqhandler.SendMsg("testmipub.server2",[]byte("2222222222"));

    signals := make(chan os.Signal, 1)
    signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
    sig := <-signals
    fmt.Println(sig.String())
    mqhandler.Close()
    os.Exit(0)
}