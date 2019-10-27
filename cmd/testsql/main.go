package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var addr = flag.String("addr", "localhost:3306/test", "mysql addr")
var proc = flag.Int("proc", 10, "proc number")
var sqls = flag.String("sql", "select * from test", "sql to execute")

var max float64
var num int
var timesum float64
var limited int

var l sync.Mutex

func printStatistics() {
	fmt.Println("query sum:", num)
	fmt.Printf("query average cost: %.5fs\n", timesum/float64(num))
	fmt.Printf("query max cost: %.5fs\n", max)
	fmt.Println("limited by throttle:", limited)
}

func execute(wg *sync.WaitGroup) {
	defer wg.Done()
	db, err := sql.Open("mysql", *addr)
	defer db.Close()
	if err != nil {
		panic(err)
	}
	start := time.Now()
	_, err = db.Query(*sqls)
	if err != nil {
		fmt.Println("query err:", err)
		l.Lock()
		limited = limited + 1
		l.Unlock()
	}
	elapsed := time.Since(start)
	elapsedSec := elapsed.Seconds()

	l.Lock()
	num = num + 1
	timesum = timesum + elapsedSec
	if elapsedSec > max {
		max = elapsedSec
	}
	l.Unlock()

	fmt.Println("Query took %s", elapsed)
}

func handleChannel() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		printStatistics()
		os.Exit(0)
	}()
}

func main() {
	flag.Parse()
	fmt.Println("mysql addr:", *addr)
	fmt.Println("proc number:", *proc)
	fmt.Println("sql to execute:", *sqls)
	handleChannel()
	var wg sync.WaitGroup
	for {
		for i := 0; i < *proc; i++ {
			wg.Add(1)
			go execute(&wg)
		}
		wg.Wait()
		time.Sleep(1 * time.Second)
	}
}
