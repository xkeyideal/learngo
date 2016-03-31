package main

import (
    "os"
    "os/signal"
    "syscall"
    "fmt"
    "time"
    "runtime"
)

var (
    exitReadyChan chan bool
    errChan chan int
    closeChan chan struct{}
)

func Recv(sigCh chan struct{}) {
    for {
        select {
            case _, ok := <- sigCh:
                if !ok {
                    //fmt.Println("Recv finished")
                    fmt.Printf("Recv finished, %d\n",time.Now().Unix())
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
                    //fmt.Println("Send finished")
                    fmt.Printf("Send finished, %d\n",time.Now().Unix())
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
                exitReadyChan <- true
                return
            }
        default:
            sigChan := make(chan struct{})
            go Recv(sigChan)
            go Send(sigChan)
            select{
            case err := <- errChan:
                close(sigChan)
                fmt.Printf("Error: %d, %d\n",err,time.Now().Unix())
            case _, ok := <- closeChan:
                if !ok {
                    close(sigChan)
                    fmt.Printf("Closed2\n")
                    exitReadyChan <- true
                    return
                }
            }
        }
    }
}

func Error() {
    cnt := 1
    for {
        time.Sleep(20 * time.Second)
        errChan <- cnt
        cnt += 1
        fmt.Printf("Send Error: %d\n",time.Now().Unix())
    }
}

func Stop() {
    close(closeChan)
    fmt.Println("Stop")
}

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU())

    exitReadyChan = make(chan bool)
    errChan = make(chan int)
    closeChan = make(chan struct{})
    go Start()
    go Error()

    signals := make(chan os.Signal, 1)
    signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
    sig := <-signals
    fmt.Println(sig.String())
    Stop()
    <-exitReadyChan // wait goroutine `Start` close first
    os.Exit(0)
}