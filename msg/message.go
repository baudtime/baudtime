package msg

type Marshaler interface {
	//Marshal([]byte) ([]byte, error)
	MarshalTo(b []byte) (int, error)
	Size() int
}

type Unmarshaler interface {
	Unmarshal([]byte) error
}

type Message interface {
	Marshaler
	Unmarshaler
}
