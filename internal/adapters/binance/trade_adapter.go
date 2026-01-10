package binance

import (
	"context"
	"log"

	client "github.com/binance/binance-connector-go/clients/spot"
	"github.com/binance/binance-connector-go/clients/spot/src/websocketstreams/models"
	"github.com/binance/binance-connector-go/common/common"
	marketv1 "github.com/Jehoi-ga-ada/aegis-genome/gen/go/market/v1"
)

type TradeAdapter struct {
	wsClient *client.BinanceSpotClient
}

func (a *TradeAdapter) GetExchangeName() string {
	return "binance"
}

func (a *TradeAdapter) Subscribe(ctx context.Context, symbol string) (<-chan *marketv1.MarketEvent, error) {
	out := make(chan *marketv1.MarketEvent, 10000)

	config := common.NewConfigurationWebsocketStreams(
		common.WithWsStreamsBasePath(common.SpotWebsocketStreamsProdUrl),
	)

	wsClient := client.NewBinanceSpotClient(
		client.WithWebsocketStreams(config),
	)

	err := wsClient.WebsocketStreams.Connect()
	if err != nil {
		log.Fatalf("Error connecting to WebSocket: %v", err)
	}

	handler, err := wsClient.WebsocketStreams.WebSocketStreamsAPI.Trade().Symbol(symbol).Execute()
	if err != nil {
		log.Fatalf("Error subscribing to stream: %v", err)
	}

	handler.On("message", func(message models.TradeResponse) {
        // DO NOT DO LOGIC HERE. 
        // Launch a goroutine to handle the mapping/serialization.
        go func(msg models.TradeResponse) {
            var side marketv1.Tick_Side
            if msg.GetSmallm() {
                side = marketv1.Tick_SIDE_SELL
            } else {
                side = marketv1.Tick_SIDE_BUY
            }

            event := &marketv1.MarketEvent{
                Symbol:         symbol,
                EventTimeNs:    msg.GetE() * 1_000_000,
                ReceivedTimeNs: common.GetTimestamp() * 1_000_000,
                Payload: &marketv1.MarketEvent_Tick{
                    Tick: &marketv1.Tick{
                        Price:    msg.GetP(),
                        Quantity: msg.GetQ(),
                        Side:     side,
                    },
                },
            }

            select {
            case out <- event:
            default:
                // If 'out' is full, we drop the message. 
                // This prevents the "Memory Balloon" effect.
            }
        }(message) 
    })

	go func() {
        <-ctx.Done()
        handler.Unsubscribe()
        close(out)
    }()

    return out, nil
}