package forex

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	auth = ""
)

func init() {
	auth, _ = os.LookupEnv("POLYGON_API_KEY")
	auth = strings.TrimSpace(auth)
}

func getAPIKey(t *testing.T) string {
	if len(auth) == 0 {
		t.Fatal("ENV key 'POLYGON_API_KEY' not set")
	}
	return auth
}

func TestWS_Open(t *testing.T) {
	ws, err := OpenWS(WsUrl, getAPIKey(t))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := ws.SubscribeQuotes("EUR/USD", func(q WsQuote) {
		fmt.Println("EUR/USD: ", fromUnixMs(q.Timestamp), " Exchange:", q.Exchange, "  b:", fmt.Sprintf("%.5f", q.Bid), "  a:", fmt.Sprintf("%.5f", q.Ask), "  s:", fmt.Sprintf("%.1f", (q.Ask-q.Bid)*10000))
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = sub

	_, _ = ws.SubscribeQuotes("GBP/USD", func(q WsQuote) {
		fmt.Println("GBP/USD: ", fromUnixMs(q.Timestamp), " Exchange:", q.Exchange, "  b:", fmt.Sprintf("%.5f", q.Bid), "  a:", fmt.Sprintf("%.5f", q.Ask), "  s:", fmt.Sprintf("%.1f", (q.Ask-q.Bid)*10000))
	})
	//ws.SubscribeAggregates("EUR/USD", func(q WsAggregate) {
	//	fmt.Println(q)
	//})
	time.Sleep(time.Hour * 24)
}

func fromUnixMs(ms int64) time.Time {
	sec := ms / 1000
	nanos := (ms % 1000) * 1000000
	return time.Unix(sec, nanos)
}
