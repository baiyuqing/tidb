package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	addr string
	proc int
	sqls string

	max     float64
	num     int
	timesum float64
	limited int
	l       sync.Mutex

	latencyC    chan float64
	throughputC chan int
	quitC       chan struct{}

	logger *log.Logger
)

func init() {
	flag.StringVar(&addr, "addr", "localhost:3306/test", "mysql addr")
	flag.IntVar(&proc, "proc", 10, "proc number")
	flag.StringVar(&sqls, "sql", "select * from test", "sql to execute")
	flag.Parse()

	latencyC = make(chan float64, 10240)
	throughputC = make(chan int, 10240)

}

func printStatistics() {
	fmt.Println("query sum:", num)
	fmt.Printf("query average cost: %.5fs\n", timesum/float64(num))
	fmt.Printf("query max cost: %.5fs\n", max)
	fmt.Println("limited by throttle:", limited)
}

func execute(wg *sync.WaitGroup) {
	defer wg.Done()
	db, err := sql.Open("mysql", addr)
	defer db.Close()
	if err != nil {
		panic(err)
	}
	start := time.Now()
	_, err = db.Query(sqls)
	if err != nil {
		// fmt.Println("query err:", err)
		logger.Println("query err:", err)

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

	addCount(1)
	addLatency(elapsedSec)

	logger.Printf("Query took %s\n", elapsed)
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

func addCount(c int) {
	throughputC <- c
}

func addLatency(d float64) {
	latencyC <- d
}

func showStatitistic() {
	ticker := time.Tick(10 * time.Second)
	var total, last int
	var totalElapsed, lastElapsed float64
	for {
		select {
		case count := <-throughputC:
			total += int(count)
			if count < 0 {
				break
			}
		case elapsed := <-latencyC:
			totalElapsed += elapsed

		case <-ticker:
			qps := float64(total-last) / 10.0
			avgLatency := (totalElapsed - lastElapsed) / 10.0
			last = total
			lastElapsed = totalElapsed
			fmt.Printf("%0.2f\tQPS, total: %v\n", qps, total)
			fmt.Printf("%0.2f\tAvgLatency per sec, total: %v\n", totalElapsed, avgLatency)
		}
	}
}

func main() {

	fmt.Println("mysql addr:", addr)
	fmt.Println("proc number:", proc)
	fmt.Println("sql to execute:", sqls)

	logfile, err := os.Create("./mysql-stress.log")
	if err != nil && err != os.ErrExist {
		fmt.Println(err)
		return
	}
	logger = log.New(logfile, "[stats]", log.Lshortfile|log.LstdFlags)

	handleChannel()
	go showStatitistic()
	var wg sync.WaitGroup
	for {
		for i := 0; i < proc; i++ {
			wg.Add(1)
			go execute(&wg)
		}
		wg.Wait()
		time.Sleep(1 * time.Second)
	}
}
