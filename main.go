package main

import (
	"flag"
	"fmt"
	"github.com/Zealous-w/redcon"
	"github.com/Zealous-w/tacodb/command"
	"github.com/Zealous-w/tacodb/server"
	"github.com/Zealous-w/tacodb/store"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
)

var (
	flagHost  = flag.String("h", "127.0.0.1", "host name")
	flagPort  = flag.String("p", "6380", "port")
	flagPath  = flag.String("d", "./data/", "directory")
	flagStore = flag.String("s", "leveldb", "kv store [boltdb, leveldb]")
)

var (
	workers = server.NewWorkerProcess(3)
)

func msgCommandDispatcher(conn redcon.Conn, cmd redcon.Command) {
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		if len(cmd.Args) < 2 {
			conn.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd.Args))
			return
		}
		err := server.MsgCmd.Dispatcher(strings.ToLower(string(cmd.Args[0])), &server.Client{Conn: conn}, cmd.Args...)
		if err != nil {
			conn.WriteError("ERR '" + err.Error() + "'")
		}
		return
	case "info":
		conn.WriteArray(3)
		conn.WriteString("# Server")
		conn.WriteString(fmt.Sprintf("pid:%d", os.Getpid()))
		conn.WriteNull()
		return
	case "select":
		conn.WriteString("OK")
		return
	case "detach":
		hconn := conn.Detach()
		go func() {
			defer hconn.Close()
			hconn.WriteString("OK")
			hconn.Flush()
		}()
		return
	case "ping":
		conn.WriteString("PONG")
	case "quit":
		conn.WriteString("OK")
		conn.Close()
	}
}

func signalHandler(s *redcon.Server) {
	sg := make(chan os.Signal, 1)
	signal.Notify(sg, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGPIPE, syscall.SIGHUP)
	go func() {
		for {
			select {
			case sig := <-sg:
				s.Close()
				log.Printf("receive signal %+v", sig)
				return
			}
		}
	}()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	db, close := store.NewDBStore(*flagStore, *flagPath)
	defer close()
	workers.Start()

	log.Printf("tacodb start success, store:%s addr:%s", *flagStore, *flagHost+":"+*flagPort)
	c := command.NewRedisCommand(db)
	server := redcon.NewServer(*flagHost+":"+*flagPort,
		msgCommandDispatcher,
		func(conn redcon.Conn) bool {
			conn.SetContext(c)
			return true
		},
		nil)
	signalHandler(server)
	_ = server.ListenAndServe()
	log.Printf("tacodb exit, bye bye...")
}
