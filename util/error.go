package util

type ClientErr struct {
	Err  string
	Desc string
}

func (e *ClientErr) Error() string {
	return e.Err
}

func (e *ClientErr) Description() string {
	return e.Desc
}

func NewClientErr(err string, description string) *ClientErr {
	return &ClientErr{err, description}
}
