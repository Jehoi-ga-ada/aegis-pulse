package ingestion

import (
	"context"
	marketv1 "github.com/Jehoi-ga-ada/aegis-genome/gen/go/market/v1"
)

type MarketProvider interface {
	Subscribe(ctx context.Context, symbol string) (<-chan *marketv1.MarketEvent, error)
	GetExchangeName() string
}