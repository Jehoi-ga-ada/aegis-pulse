package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/Jehoi-ga-ada/aegis-pulse/internal/adapters/binance"
	"github.com/redis/rueidis"
	"google.golang.org/protobuf/proto"
)

func main() {
	// 1. Setup cancellation
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2. Setup Ruedis (Infrastructure)
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"localhost:6379"}, 
	})
	if err != nil {
		log.Fatalf("Redis connect error: %v", err)
	}
	defer client.Close()

	// 3. Start Adapter
	adapter := &binance.TradeAdapter{}
	eventChan, err := adapter.Subscribe(ctx, "btcusdt")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Adapter started. Listening for trades...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down...")
			return
		case event, ok := <-eventChan:
			if !ok { return }

			data, _ := proto.Marshal(event)

			go func(d []byte) {
				cmd := client.B().Xadd().
					Key("market:btcusdt:trades").
					Maxlen().Almost().Threshold(strconv.Itoa(1000)). // Use Almost() for high-speed ~ trimming
					Id("*").                          // Auto-generate Redis ID
					FieldValue().FieldValue("d", string(d)).       // Field "d", Value is binary data
					Build()

				err := client.Do(ctx, cmd).Error()
				
				if err != nil && err != rueidis.Nil {
					log.Printf("Redis error: %v", err)
				}
			}(data)
		}
	}
}