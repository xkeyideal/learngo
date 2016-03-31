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
    RoutingKey = "test.recv.response"

    RespDefaultQueue = "testmq_recv"
)

type MQHandler struct {
    conn *amqp.Connection
    recvCh *amqp.Channel
    sendCh *amqp.Channel
    queue amqp.Queue
}

func NewMQHandler() (*MQHandler, error) {
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
    queueName := "test_recv_response"
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

    routingKeys := []string{RoutingKey}
    for _, routingKey := range routingKeys {
        err := recvCh.QueueBind(
          queueName,
          routingKey,
          ExchangeForRecv,
          false,
          nil,
        )
        if err != nil {
          return nil, err
        }
    }

    err = sendCh.ExchangeDeclare(
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

    return &MQHandler{
        conn: conn,
        recvCh: recvCh,
        sendCh: sendCh,
        queue: queue,
    },nil
}

func (mq *MQHandler) BindQueue(routingKeys []string) (error){
    for _, routingKey := range routingKeys {
        err := mq.recvCh.QueueBind(
          mq.queue.Name,routingKey,ExchangeForRecv,false,nil,
        )
        if err != nil {
          return err
        }
    }
    return nil
}

func (mq *MQHandler) SendMsg(routingKey string, body []byte) error {
     err := mq.sendCh.Publish(
        ExchangeForSend,
        routingKey,
        false,
        false,
        amqp.Publishing{
           ContentType: "text/plain",
           Body: body,
        })
    return err;
}

func (mq *MQHandler) ConsumeMsg() error {
    msgs, err := mq.recvCh.Consume(mq.queue.Name,"",true,false,false,false,nil)
    if err != nil {
      return err
    }
    for msg := range msgs {
        fmt.Printf("%s, %s\n",string(msg.Body),msg.RoutingKey)
        fmt.Println("===============")
        //mq.SendMsg(RespDefaultQueue,[]byte(string(msg.Body) + "response"))
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
    // routingKeys := []string{RoutingKey}
    // mqhandler.BindQueue(routingKeys)
    if err != nil {
        os.Exit(1)
    }
    go mqhandler.ConsumeMsg()


    ticker := time.NewTicker(10 * time.Second)
    for _ = range ticker.C {
        str1 := fmt.Sprintf("1111111111: %d", time.Now().Unix())
        str2 := fmt.Sprintf("2222222222: %d", time.Now().Unix())
        mqhandler.SendMsg("testmipub.server1",[]byte(str1));
        mqhandler.SendMsg("testmipub.server2",[]byte(str2));
    }

    signals := make(chan os.Signal, 1)
    signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
    sig := <-signals
    fmt.Println(sig.String())
    mqhandler.Close()
    os.Exit(0)
}
