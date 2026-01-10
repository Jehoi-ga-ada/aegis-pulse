package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	// "google.golang.org/protobuf/encoding/protojson"
	marketv1 "github.com/Jehoi-ga-ada/aegis-genome/gen/go/market/v1"
	"github.com/Jehoi-ga-ada/aegis-pulse/internal/adapters/binance"
	"github.com/redis/go-redis/v9"
)

func main() {
	// 1. Setup cancellation
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2. Setup Redis (Infrastructure)
	rdb := redis.NewClient(&redis.Options{Addr: "localhost: 6739"})
	defer rdb.Close()

	// 3. Start Adapter
	adapter := &binance.TradeAdapter{}
	eventChan, err := adapter.Subscribe(ctx, "btcusdt")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Adapter started. Listening for trades...")

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var latestEvent *marketv1.MarketEvent

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down...")
			return
		case event, ok := <-eventChan:
			if !ok {
				return
			}
			// 1. UPDATE: Always capture the newest data in memory.
			// This is an instant operation that never blocks.
			latestEvent = event

		case <-ticker.C:
			// 2. LOG: Only print the latest snapshot twice per second.
			if latestEvent != nil {
				price := latestEvent.GetTick().Price
				latency := (latestEvent.ReceivedTimeNs - latestEvent.EventTimeNs) / 1_000_000
				log.Printf("Price: %s | Latency: %dms", price, latency)
			}
		}
	}
}