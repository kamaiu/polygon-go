package forex

import "fmt"

type StatusCodeError struct {
	Code int
}

func (s StatusCodeError) Error() string {
	return fmt.Sprintf("%d", s.Code)
}
