package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/baudtime/baudtime/msg"
	"github.com/baudtime/baudtime/msg/gateway"
	"github.com/baudtime/baudtime/tcp/client"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ProArgs struct {
	host         string
	db           string
	testDataPath string
	writeClients int
	rowsPerWrite int
	readClients  int
}

var (
	totalWrites int64 = 0
	totalReads  int64 = 0
	metricLabel       = msg.Label{Name: "__name__", Value: "metric"}
)

func main() {
	// Configuration
	var arguments ProArgs

	// Parse options
	flag.StringVar(&(arguments.host), "host", "localhost:8088", "Server host to connect")
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

	labels, err := fetchLabels(arguments.testDataPath)
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(arguments.writeClients)

	st := time.Now()
	for i := 0; i < arguments.writeClients; i++ {
		go writeDataImp(&wg, arguments, labels)
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

	labels, err := fetchLabels(arguments.testDataPath)
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(arguments.readClients)

	st := time.Now()
	for i := 0; i < arguments.readClients; i++ {
		go readDataImp(&wg, arguments, labels)
	}

	wg.Wait()

	elapsed := time.Since(st)
	seconds := float64(elapsed) / float64(time.Second)

	log.Println("---- Spent", seconds, "seconds to query", totalReads, "records, speed:", float64(totalReads)/seconds, "Rows/Second")
}

func writeDataImp(wg *sync.WaitGroup, arguments *ProArgs, labels [][]msg.Label) {
	defer wg.Done()

	addrProvider := client.NewStaticAddrProvider(arguments.host)
	cli := client.NewGatewayClient("name", addrProvider)

	defer cli.Close()

	// Write data
	counter := 0
	totalRecords := 0
	r := &gateway.AddRequest{}

	beginTs := time.Now().Unix()

	for i, lbs := range labels {
		t := 1000 * (beginTs + int64(i))

		r.Series = append(r.Series, &msg.Series{
			Labels: lbs,
			Points: []msg.Point{{t, float64(i)}},
		})

		counter++

		if counter >= arguments.rowsPerWrite {
			if _, err := cli.SyncRequest(context.Background(), r); err != nil {
				log.Fatal(err)
			}
			r.Series = r.Series[:0]

			totalRecords += counter
			counter = 0
		}
	}

	totalRecords += counter
	if counter > 0 {
		if _, err := cli.SyncRequest(context.Background(), r); err != nil {
			log.Fatal(err)
		}
	}

	atomic.AddInt64(&totalWrites, int64(totalRecords))
}

func readDataImp(wg *sync.WaitGroup, arguments *ProArgs, labels [][]msg.Label) {
	defer wg.Done()

	addrProvider := client.NewStaticAddrProvider(arguments.host)
	cli := client.NewGatewayClient("name", addrProvider)

	defer cli.Close()

	totalRecords := 0
	query := &gateway.InstantQueryRequest{
		Timeout: "10m",
	}

	//var err error
	for _, lb := range labels {
		query.Query = fmt.Sprintf("metric{unemdcai=%q}[10s]", lb[0].Value)

		_, err := cli.SyncRequest(context.Background(), query)
		if err != nil {
			log.Fatal(err)
		}

		totalRecords++
	}

	atomic.AddInt64(&totalReads, int64(totalRecords))
}

func fetchLabels(path string) ([][]msg.Label, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)

	var mets [][]msg.Label

	for scanner.Scan() {
		lbls := make([]msg.Label, 0, 10)

		r := strings.NewReplacer("\"", "", "{", "", "}", "")
		s := r.Replace(scanner.Text())

		labelChunks := strings.Split(s, ",")
		for _, labelChunk := range labelChunks {
			split := strings.Split(labelChunk, ":")
			lbls = append(lbls, msg.Label{Name: split[0], Value: split[1]})
		}

		lbls = append(lbls, metricLabel)

		// Order of the k/v labels matters, don't assume we'll always receive them already sorted.
		sort.Slice(lbls, func(i, j int) bool {
			return lbls[i].Name < lbls[i].Name
		})

		mets = append(mets, lbls)
	}
	return mets, nil
}
