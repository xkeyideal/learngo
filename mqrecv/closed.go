package main

import (
    "os"
    "os/signal"
    "syscall"
    "fmt"
    "time"
)

var (
    errChan chan int
    closeChan chan struct{}
)

func Recv(sigCh chan struct{}) {
    for {
        select {
            case _, ok := <- sigCh:
                if !ok {
                    fmt.Println("Recv finished")
                    return
                }
            default:
                fmt.Printf("Recv, %d\n",time.Now().Unix())
                time.Sleep(10 * time.Second)
        }
    }
}

func Send(sigCh chan struct{}) {
    for {
        select {
            case _, ok := <- sigCh:
                if !ok {
                    fmt.Println("Send finished")
                    return
                }
            default:
                fmt.Printf("Send, %d\n",time.Now().Unix())
                time.Sleep(10 * time.Second)
        }
    }
}

func Start() {
    for {
        select {
        case _, ok := <- closeChan:
            if !ok {
                fmt.Printf("Closed\n")
            }
        default:
            sigChan := make(chan struct{})
            go Recv(sigChan)
            go Send(sigChan)
            select{
            case err := <- errChan:
                close(sigChan)
                fmt.Printf("Error: %d\n",err)
            case _, ok := <- closeChan:
                if !ok {
                    fmt.Printf("Closed2\n")
                    return
                }
            }
        }
    }
}

func Error() {
    for {
        time.Sleep(20 * time.Second)
        errChan <- 1
        fmt.Println("Send error")
    }
}

func Stop() {
    close(closeChan)
}

func main() {
    errChan = make(chan int)
    closeChan = make(chan struct{})
    go Start()
    go Error()

    signals := make(chan os.Signal, 1)
    signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
    sig := <-signals
    fmt.Println(sig.String())
    Stop()
    time.Sleep(2 * time.Second)
    os.Exit(0)
}