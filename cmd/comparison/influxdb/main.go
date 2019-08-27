package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	client "github.com/influxdata/influxdb1-client/v2"
)

type ProArgs struct {
	host         string
	username     string
	password     string
	db           string
	testDataPath string
	writeClients int
	rowsPerWrite int
	readClients  int
}

var (
	totalWrites int64 = 0
	totalReads  int64 = 0
)

func main() {
	// Configuration
	var arguments ProArgs

	// Parse options
	flag.StringVar(&(arguments.host), "host", "http://localhost:8086", "Server host to connect")
	flag.StringVar(&(arguments.username), "user", "", "Username used to connect to server")
	flag.StringVar(&(arguments.password), "pass", "", "Password used to connect to server")
	flag.StringVar(&(arguments.db), "db", "db", "DB to insert data")
	flag.StringVar(&(arguments.testDataPath), "testDataPath", "./testdata", "Raw csv data")
	flag.IntVar(&(arguments.writeClients), "writeClients", 3, "Number of write clients")
	flag.IntVar(&(arguments.rowsPerWrite), "rowsPerWrite", 1000, "Number of rows per write")
	flag.IntVar(&(arguments.readClients), "readClients", 2, "Number of read clients")

	flag.Parse()

	if arguments.writeClients > 0 {
		writeData(&arguments)
	}

	if arguments.readClients > 0 {
		readData(&arguments)
	}
}

func writeData(arguments *ProArgs) {
	log.Println("write data")
	log.Println("---- writeClients:", arguments.writeClients)
	log.Println("---- testDataPath:", arguments.testDataPath)
	log.Println("---- rowsPerWrite:", arguments.rowsPerWrite)

	tags, err := fetchTags(arguments.testDataPath)
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(arguments.writeClients)

	st := time.Now()
	for i := 0; i < arguments.writeClients; i++ {
		go writeDataImp(&wg, arguments, tags)
	}

	wg.Wait()

	elapsed := time.Since(st)
	seconds := float64(elapsed) / float64(time.Second)

	log.Println("---- Spent", seconds, "seconds to insert", totalWrites, "records, speed:", float64(totalWrites)/seconds, "Rows/Second")
}

func readData(arguments *ProArgs) {
	log.Println("read data")
	log.Println("---- readClients:", arguments.readClients)
	log.Println("---- testDataPath:", arguments.testDataPath)

	tags, err := fetchTags(arguments.testDataPath)
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(arguments.readClients)

	st := time.Now()
	for i := 0; i < arguments.readClients; i++ {
		go readDataImp(&wg, arguments, tags)
	}

	wg.Wait()

	elapsed := time.Since(st)
	seconds := float64(elapsed) / float64(time.Second)

	log.Println("---- Spent", seconds, "seconds to query", totalReads, "records, speed:", float64(totalReads)/seconds, "Rows/Second")
}

func writeDataImp(wg *sync.WaitGroup, arguments *ProArgs, tags []map[string]string) {
	defer wg.Done()

	// Connect to the server
	cli, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     arguments.host,
		Username: arguments.username,
		Password: arguments.password,
		Timeout:  10 * time.Minute,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer cli.Close()

	// Create database
	_, err = cli.Query(client.Query{
		Database: arguments.db,
		Command:  fmt.Sprintf("create database %s", arguments.db),
	})
	if err != nil {
		log.Fatal(err)
	}

	// Write data
	counter := 0
	totalRecords := 0

	beginTs := time.Now().Unix()
	var bp client.BatchPoints

	for i, t := range tags {
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  arguments.db,
			Precision: "ms",
		})
		if err != nil {
			log.Fatal(err)
		}

		fields := map[string]interface{}{
			"v": i,
		}

		pt, err := client.NewPoint("metric", t, fields, time.Unix(beginTs, int64(i)*int64(time.Millisecond)))
		if err != nil {
			log.Fatalln("Error: ", err)
		}

		bp.AddPoint(pt)
		counter++

		if counter >= arguments.rowsPerWrite {
			if err := cli.Write(bp); err != nil {
				log.Fatal(err)
			}

			totalRecords += counter
			counter = 0
		}
	}

	totalRecords += counter
	if counter > 0 {
		if err := cli.Write(bp); err != nil {
			log.Fatal(err)
		}
	}

	atomic.AddInt64(&totalWrites, int64(totalRecords))
}

func readDataImp(wg *sync.WaitGroup, arguments *ProArgs, tags []map[string]string) {
	defer wg.Done()

	cli, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     arguments.host,
		Username: arguments.username,
		Password: arguments.password,
		Timeout:  10 * time.Minute,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer cli.Close()

	totalRecords := 0
	query := client.Query{
		Database: arguments.db,
	}

	for _, t := range tags {
		query.Command = fmt.Sprintf("select * from metric where unemdcai='%s' and time > now() - 10s", t["unemdcai"])

		_, err := cli.Query(query)
		if err != nil {
			log.Fatal(err)
		}

		totalRecords++
	}

	atomic.AddInt64(&totalReads, int64(totalRecords))
}

func fetchTags(path string) ([]map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var tags []map[string]string

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		m := make(map[string]string)

		r := strings.NewReplacer("\"", "", "{", "", "}", "")
		str := r.Replace(scanner.Text())

		kvs := strings.Split(str, ",")
		for _, kv := range kvs {
			split := strings.Split(kv, ":")
			m[split[0]] = split[1]
		}

		tags = append(tags, m)
	}

	return tags, nil
}
