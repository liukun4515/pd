package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	var ch chan string
	ch = make(chan string, 10)

	wg := sync.WaitGroup{}
	wg.Add(1)
	fmt.Println(len(ch))
	go func() {
		wg.Done()
		ch <- "hello"

	}()
	time.Sleep(time.Second)
	fmt.Println(len(ch))
	fmt.Println(len(ch))
	fmt.Println(len(ch))
	s := <-ch
	fmt.Println(len(ch))
	fmt.Println(s)
	wg.Wait()
	close(ch)
}
