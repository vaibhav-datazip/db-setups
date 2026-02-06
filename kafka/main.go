package main

// json based

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
)

const (
	topic         = "vbhv"
	partitions    = 3
	totalRecords  = 2_000_000
	batchSize     = 200
	flushMaxBytes = 4 * 1024 * 1024
	workers       = 4
	verboseEvery  = 10_000
	duration      = 8 * time.Minute // total desired insertion time
)

type EventPayload struct {
	EventName        string  `json:"event_name"`
	EventType        string  `json:"event_type"`
	EventTriggeredTM string  `json:"event_triggered_tm"`
	ClientKey        string  `json:"client_key"`
	CMID             int     `json:"cm_id"`
	EmpID            int     `json:"emp_id"`
	EmpName          string  `json:"emp_name"`
	ServerType       string  `json:"server_type"`
	RequestIP        string  `json:"request_ip"`
	Log              string  `json:"log"`
	IsRecovered      bool    `json:"is_recovered"`
	EmpTenure1       float32 `json:"emp_tenure1"`
	EmpTenure2       float64 `json:"emp_tenure2"`
}

var brokers = []string{
	"localhost:29092",
}

func main() {
	start := time.Now()

	var sent int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// one writer per worker
	writers := make([]*kafka.Writer, workers)
	for i := 0; i < workers; i++ {
		writers[i] = &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.RoundRobin{},
			BatchSize:    batchSize,
			BatchBytes:   flushMaxBytes,
			Compression:  compress.Snappy,
			Async:        true,
			RequiredAcks: kafka.RequireOne,
		}
		defer writers[i].Close()
	}

	// throttle calculation
	totalBatches := totalRecords / batchSize
	sleepPerBatch := duration / time.Duration(totalBatches)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			wr := writers[id]

			from := int64(id) * (totalRecords / workers)
			to := from + (totalRecords / workers)
			if id == workers-1 {
				to = totalRecords
			}

			batch := make([]kafka.Message, 0, batchSize)

			for i := from; i < to; i++ {
				p := EventPayload{
					EventName:        fmt.Sprintf("event_%d", i),
					EventType:        "UI_CLICK",
					EventTriggeredTM: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
					ClientKey:        fmt.Sprintf("client_%d", 101+(i%100)),
					CMID:             101 + int(i%50),
					EmpID:            2000 + int(i%5000),
					EmpName:          fmt.Sprintf("User_%d", i%1000),
					ServerType:       "PROD",
					RequestIP:        fmt.Sprintf("192.168.%d.%d", i%255, (i/255)%255),
					Log:              "Volume test",
					IsRecovered:      false,
					EmpTenure1:       float32(i%10001)/7.0 + 0.000001,
					EmpTenure2:       float64(i%10001)/7.0 + 0.000001,
				}

				val, _ := json.Marshal(p)
				batch = append(batch, kafka.Message{Value: val})

				if len(batch) == batchSize {
					if err := wr.WriteMessages(ctx, batch...); err != nil {
						panic(err)
					}

					atomic.AddInt64(&sent, int64(len(batch)))
					batch = batch[:0]

					// throttle
					time.Sleep(sleepPerBatch)
				}

				if i%verboseEvery == 0 && i > 0 {
					fmt.Printf(
						"worker-%d sent %d total=%d\n",
						id,
						i-from,
						atomic.LoadInt64(&sent),
					)
				}
			}

			// flush remainder
			if len(batch) > 0 {
				if err := wr.WriteMessages(ctx, batch...); err != nil {
					panic(err)
				}
				atomic.AddInt64(&sent, int64(len(batch)))
			}
		}(w)
	}

	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf(
		"done: %d msgs in %.1f s => %.0f msg/s\n",
		atomic.LoadInt64(&sent),
		elapsed.Seconds(),
		float64(atomic.LoadInt64(&sent))/elapsed.Seconds(),
	)
}
