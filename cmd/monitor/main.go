package main

import (
	"context"
	"fmt"
	"log"
	"time"

	// Standard Redis driver
	"github.com/redis/go-redis/v9"
	// Protobuf marshalling
	"google.golang.org/protobuf/proto"

	// Your internal Genome models
	marketv1 "github.com/Jehoi-ga-ada/aegis-genome/gen/go/market/v1"
)

func main() {
	ctx := context.Background()

	// 1. Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// Check connection
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}

	fmt.Println("Monitor started. Watching stream: market:btcusdt:trades")
	fmt.Println("---------------------------------------------------------")

	// 2. Start consuming from the "End" of the stream ($)
	lastID := "$"

	for {
		// XRead blocks until a new trade arrives
		streams, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{"market:btcusdt:trades", lastID},
			Block:   0, // Wait indefinitely for new data
			Count:   1, // Process one at a time for precise monitoring
		}).Result()

		if err != nil {
			log.Printf("Redis read error: %v", err)
			time.Sleep(time.Second) // Wait before retrying
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				// Update lastID so we don't read the same message twice
				lastID = msg.ID

				// 3. Extract binary data
				rawData, ok := msg.Values["d"].(string)
				if !ok {
					continue
				}

				// 4. Unmarshal Protobuf
				event := &marketv1.MarketEvent{}
				err := proto.Unmarshal([]byte(rawData), event)
				if err != nil {
					log.Printf("Failed to unmarshal: %v", err)
					continue
				}

				// 5. Calculate Latencies
				now := time.Now().UnixNano()
				
				// Time from Binance to your Pulse Adapter
				networkLat := (event.ReceivedTimeNs - event.EventTimeNs) / 1_000_000
				
				// Time from Pulse Adapter to this Monitor (via Redis)
				redisLat := (now - event.ReceivedTimeNs) / 1_000_000
				
				// Total time from Binance to this Monitor
				totalLat := (now - event.EventTimeNs) / 1_000_000

				fmt.Printf("[%s] Price: %-10s | Net: %3dms | Redis: %3dms | Total: %3dms\n",
					event.Symbol,
					event.GetTick().Price,
					networkLat,
					redisLat,
					totalLat,
				)
			}
		}
	}
}