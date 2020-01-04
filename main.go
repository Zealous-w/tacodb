package main

import (
	"flag"
	"fmt"
	"github.com/tidwall/redcon"
	"log"
	"os"
	"os/signal"
	"tacodb/command"
	"tacodb/server"
	"tacodb/store"
	"sort"
	"strings"
	"syscall"
)

var (
	flagHost  = flag.String("h", "127.0.0.1", "host name")
	flagPort  = flag.String("p", "6380", "port")
	flagPath  = flag.String("d", "./data/", "directory")
	flagStore = flag.String("s", "leveldb", "kv store")
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
		workers.Push(cmd.Args[1], &server.Client{Conn: conn, Cmds: &cmd})
		return
	case "info":
		s := workers.Stats()
		slc := make([]int, 0, len(s))
		for k := range s {
			slc = append(slc, k)
		}
		sort.Ints(slc)
		////
		conn.WriteArray(len(slc) + 4)
		conn.WriteString("# Server")
		conn.WriteString(fmt.Sprintf("pid:%d", os.Getpid()))

		conn.WriteString("# thread stats")
		for _, v := range slc {
			conn.WriteString(fmt.Sprintf("tid:%d, count:%d", v, s[v]))
		}
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
	flag.Parse()
	db := store.NewDBStore(*flagStore)
	err := db.Open(*flagPath)
	if err != nil {
		log.Printf("open db failed %+v", err)
		return
	}
	defer db.Close()
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
	err = server.ListenAndServe()
	workers.Close()
	log.Printf("tacodb exit, bye bye...")
}
