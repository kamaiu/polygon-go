package polygon

import (
	"github.com/mailru/easyjson/jlexer"
)

type parseContext struct {
	sym            map[string]string
	exchanges      map[string]*Exchange
	exchangesByNum []*Exchange
}

type forexContext struct {
}

type Exchange struct {
}

func parse(b []byte) {
	in := &jlexer.Lexer{Data: b}
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
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
			case "status":
			case "message":
			}
			in.WantComma()
		}
		in.Delim('}')
		in.WantComma()
	}
	in.Delim(']')

	if isTopLevel {
		in.Consumed()
	}
}
