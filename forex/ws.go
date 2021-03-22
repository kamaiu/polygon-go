package forex

import (
	"context"
	"errors"
	"fmt"
	"github.com/fasthttp/websocket"
	"github.com/mailru/easyjson/jlexer"
	"io"
	"net/http"
	"os"
	"reflect"
	"sync"
	"unsafe"
)

var (
	ErrNilHandler  = errors.New("nil handler")
	ErrFrameTooBig = errors.New("frame too big")
)

type WsQuoteHandler func(q WsQuote)

type WsAggregateHandler func(q WsAggregate)

type WsQuoteSubscription struct {
	name    string
	last    int64
	pair    *WsPair
	handler WsQuoteHandler
	mu      sync.Mutex
}

func (s *WsQuoteSubscription) Name() string {
	return s.name
}

func (s *WsQuoteSubscription) Unsubscribe() {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.pair
	s.pair = nil
	if p == nil {
		return
	}

	// Remove from subs map on pair
	p.mu.Lock()
	delete(p.quotes, s)
	p.mu.Unlock()
}

type WsAggregateSubscription struct {
	name    string
	last    int64
	pair    *WsPair
	handler WsAggregateHandler
	mu      sync.Mutex
}

func (s *WsAggregateSubscription) Name() string {
	return s.name
}

func (s *WsAggregateSubscription) Unsubscribe() {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.pair
	s.pair = nil
	if p == nil {
		return
	}

	// Remove from subs map on pair
	p.mu.Lock()
	delete(p.aggregates, s)
	p.mu.Unlock()
}

type Exchange struct {
	Num  int64
	Name string
}

type WsPair struct {
	ws         *WS
	Pair       string
	quotes     map[*WsQuoteSubscription]struct{}
	aggregates map[*WsAggregateSubscription]struct{}
	mu         sync.RWMutex
}

func (p *WsPair) subscribeQuotes(handler WsQuoteHandler) (*WsQuoteSubscription, error) {
	if handler == nil {
		return nil, ErrNilHandler
	}
	p.mu.Lock()
	if p.quotes == nil {
		p.quotes = make(map[*WsQuoteSubscription]struct{})
	}
	sub := &WsQuoteSubscription{
		name:    p.Pair,
		pair:    p,
		handler: handler,
	}
	first := len(p.quotes) == 0
	p.quotes[sub] = struct{}{}
	p.mu.Unlock()

	if first {
		if err := p.ws.writeSubscribe(p.Pair, false); err != nil {
			p.mu.Lock()
			delete(p.quotes, sub)
			p.mu.Unlock()
			return nil, err
		}
	}

	return sub, nil
}

func (p *WsPair) onQuote(quote WsQuote) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for s := range p.quotes {
		h := s.handler
		if h != nil {
			h(quote)
		}
	}
}

func (p *WsPair) subscribeAggregate(handler WsAggregateHandler) (*WsAggregateSubscription, error) {
	if handler == nil {
		return nil, ErrNilHandler
	}
	p.mu.Lock()
	if p.aggregates == nil {
		p.aggregates = make(map[*WsAggregateSubscription]struct{})
	}
	sub := &WsAggregateSubscription{
		name:    p.Pair,
		pair:    p,
		handler: handler,
	}
	first := len(p.aggregates) == 0
	p.aggregates[sub] = struct{}{}
	p.mu.Unlock()

	if first {
		if err := p.ws.writeSubscribe(p.Pair, true); err != nil {
			p.mu.Lock()
			delete(p.aggregates, sub)
			p.mu.Unlock()
			return nil, err
		}
	}

	return sub, nil
}

func (p *WsPair) onAggregate(aggregate WsAggregate) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.aggregates == nil {
		return
	}
	for s := range p.aggregates {
		h := s.handler
		if h != nil {
			h(aggregate)
		}
	}
}

type WsStatus struct {
	Status  string
	Message string
}

type WsQuote struct {
	Exchange  int64
	Ask       float64 `json:"a"` // The ask price.
	Bid       float64 `json:"b"` // The bid price.
	Timestamp int64   `json:"t"` // The Timestamp in Unix MS.
}

type WsAggregate struct {
	Open   float64 `json:"o"`  // The open price for this aggregate window.
	High   float64 `json:"h"`  // The high price for this aggregate window.
	Low    float64 `json:"l"`  // The low price for this aggregate window.
	Close  float64 `json:"ws"` // The close price for this aggregate window.
	Volume int64   `json:"v"`  // The volume of trades during this aggregate window.
	Start  int64   `json:"s"`  // The start time for this aggregate window in Unix Milliseconds.
	End    int64   `json:"e"`  // The end time for this aggregate window in Unix Milliseconds.
}

const (
	StatusConnected    = "connected"
	StatusAuthSuccess  = "auth_success"
	StatusAuthRequired = "auth_required"
	StatusSuccess      = "success"
	StatusError        = "error"
)

type wsEventType int

const (
	wsEventTypeNone      wsEventType = 0
	wsEventTypeStatus    wsEventType = 1
	wsEventTypeQuote     wsEventType = 2
	wsEventTypeAggregate wsEventType = 3
)

const (
	WsUrl          = "wss://socket.polygon.io/forex"
	MaxWsFrameSize = 1024 * 1024 * 4 // 4mb
)

type WS struct {
	auth   string
	c      *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	parser wsParser

	closeCode int
	closeText string
	wg        sync.WaitGroup
	mu        sync.Mutex
}

func OpenWS(url, auth string) (*WS, error) {
	ctx, cancel := context.WithCancel(context.Background())
	wc, resp, err := websocket.DefaultDialer.DialContext(ctx, url, http.Header{})
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode != 200 && resp.StatusCode != 101 {
		cancel()
		return nil, StatusCodeError{resp.StatusCode}
	}

	ws := &WS{
		auth:   auth,
		c:      wc,
		ctx:    ctx,
		cancel: cancel,
	}
	ws.parser = newWsParser(ws.onStatus)
	err = ws.writeAuth(ws.auth)
	if err != nil {
		_ = ws.Close()
		return nil, err
	}
	ws.wg.Add(1)
	go ws.run()

	return ws, nil
}

func (ws *WS) onStatus(status WsStatus) {
	switch status.Status {
	case StatusAuthSuccess:
		fmt.Println("Authenticated!!!", status.Message)
	case StatusAuthRequired:
		fmt.Println("Needs Auth", status.Message)
	case StatusConnected:
		fmt.Println("Connected", status.Message)
	case StatusSuccess:
		fmt.Println("Success", status.Message)
	case StatusError:
		fmt.Println("Error", status.Message)
	default:
		fmt.Println(status.Status, status.Message)
	}
}

func (ws *WS) Close() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	c := ws.c
	if c == nil {
		return os.ErrClosed
	}
	if ws.cancel != nil {
		ws.cancel()
		ws.cancel = nil
	}
	ws.c = nil
	return c.Close()
}

func (ws *WS) run() {
	defer func() {
		ws.wg.Done()
	}()

	c := ws.c

	c.SetCloseHandler(func(code int, text string) error {
		ws.mu.Lock()
		defer ws.mu.Unlock()
		ws.closeCode = code
		ws.closeText = text
		return nil
	})

	var (
		mt, n int
		r     io.Reader
		err   error
		b     = make([]byte, 4096)
	)

LOOP:
	for {
		mt, r, err = c.NextReader()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break LOOP
		}
		switch mt {
		case websocket.TextMessage, websocket.BinaryMessage:
			n, err = r.Read(b)
			b = b[:n]
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				break LOOP
			}
			if len(b) == cap(b) {
				for {
					if len(b) == cap(b) {
						if len(b)+1024 >= MaxWsFrameSize {
							err = ErrFrameTooBig
							break LOOP
						}
						b = append(b, make([]byte, 1024)...)[:len(b)]
					}
					n, err = r.Read(b[len(b):cap(b)])
					b = b[:len(b)+n]
					if err != nil {
						if err == io.EOF {
							err = nil
						}
						break LOOP
					}
				}
			}
			ws.parser.parse(b)
			b = b[0:cap(b)]
		}
	}
}

var (
	wsMessageAuthBegin         = []byte("{\"action\":\"auth\",\"params\":\"")
	wsMessageAuthEnd           = []byte("\"}")
	wsMessageSubscribeBegin    = []byte("{\"action\":\"subscribe\",\"params\":\"C.")
	wsMessageSubscribeBeginAgg = []byte("{\"action\":\"subscribe\",\"params\":\"CA.")
	wsMessageSubscribeEnd      = []byte("\"}")
)

func stob(s string) []byte {
	header := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: header.Data,
		Len:  header.Len,
		Cap:  header.Len,
	}))
}

func (ws *WS) writeAuth(auth string) error {
	wr, err := ws.c.NextWriter(websocket.TextMessage)
	if err != nil {
		return err
	}
	_, err = wr.Write(wsMessageAuthBegin)
	if err != nil {
		_ = wr.Close()
		return err
	}
	_, err = wr.Write(stob(auth))
	if err != nil {
		_ = wr.Close()
		return err
	}
	_, err = wr.Write(wsMessageAuthEnd)
	if err != nil {
		_ = wr.Close()
		return err
	}
	return wr.Close()
}

func (ws *WS) writeSubscribe(ticker string, aggregate bool) error {
	wr, err := ws.c.NextWriter(websocket.TextMessage)
	if err != nil {
		return err
	}
	if aggregate {
		_, err = wr.Write(wsMessageSubscribeBeginAgg)
	} else {
		_, err = wr.Write(wsMessageSubscribeBegin)
	}
	if err != nil {
		_ = wr.Close()
		return err
	}
	_, err = wr.Write(stob(ticker))
	if err != nil {
		_ = wr.Close()
		return err
	}
	_, err = wr.Write(wsMessageSubscribeEnd)
	if err != nil {
		_ = wr.Close()
		return err
	}
	return wr.Close()
}

func (ws *WS) SubscribeQuotes(ticker string, handler WsQuoteHandler) (*WsQuoteSubscription, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.c == nil {
		return nil, os.ErrClosed
	}
	existing := ws.parser.pairs[ticker]
	if existing == nil {
		existing = &WsPair{
			ws:   ws,
			Pair: ticker,
		}
		ws.parser.pairs[ticker] = existing
	}
	sub, err := existing.subscribeQuotes(handler)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

func (ws *WS) SubscribeAggregates(ticker string, handler WsAggregateHandler) (*WsAggregateSubscription, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.c == nil {
		return nil, os.ErrClosed
	}
	existing := ws.parser.pairs[ticker]
	if existing == nil {
		existing = &WsPair{
			ws:   ws,
			Pair: ticker,
		}
		ws.parser.pairs[ticker] = existing
	}
	sub, err := existing.subscribeAggregate(handler)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

type wsParser struct {
	statusFn  func(status WsStatus)
	exchanges map[int64]Exchange // Interns
	pairs     map[string]*WsPair // Interns

	status     string
	message    string
	b, a       float64
	o, h, l, c float64
	v, s, e, t int64
	x          int64
	ev         wsEventType
}

func newWsParser(statusFn func(status WsStatus)) wsParser {
	return wsParser{
		statusFn:  statusFn,
		exchanges: make(map[int64]Exchange),
		pairs:     make(map[string]*WsPair),
	}
}

// zero-alloc json parser for quotes and aggregates
func (ctx *wsParser) parse(b []byte) {
	in := &jlexer.Lexer{Data: b}
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}

	ctx.ev = wsEventTypeNone
	var pair *WsPair

	in.Delim('[')
	for !in.IsDelim(']') {
		in.Delim('{')
		for !in.IsDelim('}') {
			key := in.UnsafeFieldName(false)
			in.WantColon()
			if in.IsNull() {
				in.Skip()
				in.WantComma()
				continue
			}
			switch key {
			case "ev":
				switch in.UnsafeString() {
				case "C":
					ctx.ev = wsEventTypeQuote
					pair = nil
					ctx.x = -1  // exchange
					ctx.b = 0.0 // ask
					ctx.a = 0.0 // bid
					ctx.t = 0   // timestamp

				case "CA":
					ctx.ev = wsEventTypeAggregate
					pair = nil
					ctx.o = 0.0 // open
					ctx.h = 0.0 // high
					ctx.l = 0.0 // low
					ctx.c = 0.0 // close
					ctx.s = 0   // start
					ctx.e = 0   // end
					ctx.v = 0   // volume

				case "status":
					ctx.ev = wsEventTypeStatus
					ctx.status = StatusError
					ctx.message = ""
				}
			// exchange
			case "x":
				id := in.Int64()
				exchange := ctx.exchanges[id]
				if len(exchange.Name) == 0 {
					exchange = Exchange{
						Num:  id,
						Name: "",
					}
					ctx.exchanges[exchange.Num] = exchange
				}
				ctx.x = exchange.Num
				//in.Skip()

			case "a":
				ctx.a = in.Float64()
			case "b":
				ctx.b = in.Float64()
			case "t":
				ctx.t = in.Int64()
			case "o":
				ctx.o = in.Float64()
			case "h":
				ctx.h = in.Float64()
			case "l":
				ctx.l = in.Float64()
			case "ws":
				ctx.c = in.Float64()
			case "v":
				ctx.v = in.Int64()
			case "s":
				ctx.s = in.Int64()
			case "e":
				ctx.e = in.Int64()

			case "p", "pair":
				str := in.UnsafeString()
				pair = ctx.pairs[str]

			case "status":
				str := in.UnsafeString()
				status := StatusError
				switch str {
				case StatusAuthRequired:
					status = StatusAuthRequired
				case StatusAuthSuccess:
					status = StatusAuthSuccess
				case StatusError:
					status = StatusError
				case StatusConnected:
					status = StatusConnected
				case StatusSuccess:
					status = StatusSuccess
				default:
					status = string(str)
				}
				ctx.status = status

			case "message":
				ctx.message = string(in.String())

			default:
				in.SkipRecursive()
			}

			in.WantComma()
		}
		in.Delim('}')

		switch ctx.ev {
		case wsEventTypeQuote:
			if pair != nil {
				pair.onQuote(WsQuote{
					Exchange:  ctx.x,
					Ask:       ctx.a,
					Bid:       ctx.b,
					Timestamp: ctx.t,
				})
			}

		case wsEventTypeStatus:
			if ctx.statusFn != nil {
				ctx.statusFn(WsStatus{
					Status:  ctx.status,
					Message: ctx.message,
				})
			}

		case wsEventTypeAggregate:
			if pair != nil {
				pair.onAggregate(WsAggregate{
					Open:   ctx.o,
					High:   ctx.h,
					Low:    ctx.l,
					Close:  ctx.c,
					Volume: ctx.v,
					Start:  ctx.s,
					End:    ctx.e,
				})
			}
		}

		in.WantComma()
	}
	in.Delim(']')

	if isTopLevel {
		in.Consumed()
	}
}
