//go:generate msgp -tests=false

package msg

type CtrlCode byte

const (
	CtrlCode_CloseRead CtrlCode = iota
	CtrlCode_CloseWrite
)

type ConnCtrl struct {
	Code CtrlCode `msg:"code"`
}
