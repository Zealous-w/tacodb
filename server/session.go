package server

import (
	"bufio"
	"github.com/tidwall/redcon"
)

type Session struct {
	RemoteAddr      string
	closeAfterReply bool
	rBuf            *bufio.Reader
	wBuf            *bufio.Writer
}

type Client struct {
	Conn redcon.Conn
	Cmds *redcon.Command
}
