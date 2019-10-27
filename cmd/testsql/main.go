package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	_ "github.com/go-sql-driver/mysql"
)

var (
	addr string
	proc int
	sqls string

	cuser string

	max     float64
	num     int
	timesum float64
	limited int
	l       sync.Mutex

	latencyC    chan float64
	throughputC chan int
	quitC       chan struct{}

	logger  *log.Logger
	pattern = "user.%v.metric.%v"
	client  statsd.Statter
)

func init() {
	flag.StringVar(&addr, "addr", "localhost:3306/test", "mysql addr")
	flag.IntVar(&proc, "proc", 10, "proc number")
	flag.StringVar(&sqls, "sql", "select * from test", "sql to execute")
	flag.Parse()

	latencyC = make(chan float64, 10240)
	throughputC = make(chan int, 10240)
	quitC = make(chan struct{})
	cuser = strings.Split(addr, ":")[0]

	var err error
	//client, err = statsd.NewClient("localhost:8126", "hack")
	client, err = statsd.NewClient("status:8126", "hack")

	if err != nil {
		logger.Println(err)
		return
	}

}

func printStatistics() {
	fmt.Println("query sum:", num)
	fmt.Printf("query average cost: %.5fs\n", timesum/float64(num))
	fmt.Printf("query max cost: %.5fs\n", max)
	fmt.Println("limited by throttle:", limited)
}

func execute(wg *sync.WaitGroup, db *sql.DB) {
	defer wg.Done()
	// db, err := sql.Open("mysql", addr)
	// defer db.Close()
	// if err != nil {
	// 	panic(err)
	// }
	start := time.Now()
	rows, err := db.Query(sqls)
	if err != nil {
		// fmt.Println("query err:", err)
		logger.Println("query err:", err)

		l.Lock()
		limited = limited + 1
		l.Unlock()
	}
	if rows != nil {
		defer rows.Close()
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
		close(quitC)
		os.Exit(0)
	}()
}

func addCount(c int) {
	throughputC <- c
}

func addLatency(d float64) {
	latencyC <- d
}

func getQuota() {
	db, err := sql.Open("mysql", addr)
	defer db.Close()
	if err != nil {
		panic(err)
	}

	for {
		rows, err := db.Query("select user_name,quota_limit from mysql.quota where user_name = 'hackathon';")
		if err != nil {
			// fmt.Println("query err:", err)
			fmt.Println("query err:", err)
			return
		}
		var user string
		var quota_count int64
		// data.Scan(dest ...interface{})
		for rows.Next() {
			if err := rows.Scan(&user, &quota_count); err != nil {
				fmt.Println(err)
				return
			}
		}
		rows.Close()
		// fmt.Printf("user: %v, quota: %v\n", user, quota_count)
		client.Gauge(fmt.Sprintf(pattern, user, "quota-count"), quota_count, 1)
		time.Sleep(10 * time.Second)
	}

}

func showStatitistic() {

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
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
		case <-quitC:
			fmt.Println("herer")
			return
		case <-ticker.C:
			qps := float64(total-last) / 10.0
			avgLatency := (totalElapsed - lastElapsed) / 10.0
			last = total
			lastElapsed = totalElapsed
			fmt.Printf("%0.2f\tQPS, total: %v\n", qps, total)
			fmt.Printf("%0.2f\tAvgLatency per sec, total: %v\n", avgLatency, totalElapsed)
			client.Raw(fmt.Sprintf(pattern, cuser, "qps"), fmt.Sprintf("%0.2f|g", qps), 1)
			client.Raw(fmt.Sprintf(pattern, cuser, "avg-latency"), fmt.Sprintf("%0.2f|g", avgLatency), 1)
			// client.Gauge(fmt."u.%v", int64, f)
		}
	}
}

func main() {

	fmt.Println("mysql addr:", addr)
	fmt.Println("proc number:", proc)
	fmt.Println("sql to execute:", sqls)
	go getQuota()
	logfile, err := os.Create("./mysql-stress.log")
	if err != nil && err != os.ErrExist {
		fmt.Println(err)
		return
	}
	logger = log.New(logfile, "[stats]", log.Lshortfile|log.LstdFlags)

	handleChannel()
	go showStatitistic()
	conns := make([]*sql.DB, proc+1)
	for i := 0; i < proc; i++ {
		db, err := sql.Open("mysql", addr)
		defer db.Close()
		if err != nil {
			panic(err)
		}
		conns[i] = db
	}

	var wg sync.WaitGroup
	for {
		for i := 0; i < proc; i++ {
			wg.Add(1)
			go execute(&wg, conns[i])
		}
		wg.Wait()
		// time.Sleep(1 * time.Second)
	}
}
