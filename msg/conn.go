//go:generate msgp -tests=false

package msg

type CtrlCode byte

const (
	CtrlCode_CloseRead CtrlCode = iota
	CtrlCode_CloseWrite
)

func (c CtrlCode) String() string {
	switch c {
	case CtrlCode_CloseRead:
		return "close read"
	case CtrlCode_CloseWrite:
		return "close write"
	}
	return "unknown"
}

type ConnCtrl struct {
	Code CtrlCode `msg:"code"`
}
