package server

import (
	"context"
	"github.com/Zealous-w/tacodb/util"
	"log"
	"strings"
	"sync"
	"time"
)

type WorkerProcess struct {
	wg       sync.WaitGroup
	parallel uint32
	ctx      context.Context
	cancel   context.CancelFunc
	queues   []chan *Client
	stats    sync.Map
}

func NewWorkerProcess(num uint32) *WorkerProcess {
	return &WorkerProcess{
		wg:       sync.WaitGroup{},
		parallel: 1 << num,
		queues:   make([]chan *Client, int(1<<num)),
		stats:    sync.Map{},
	}
}

func (c *WorkerProcess) Start() {
	c.ctx, c.cancel = context.WithCancel(context.Background())
	for k := range c.queues {
		c.queues[k] = make(chan *Client, 10240)
		c.wg.Add(1)
		go c.loop(c.ctx, k)
	}
}

func (c *WorkerProcess) Push(key []byte, task *Client) {
	index := util.BKDRHash(key) & (c.parallel - 1)
	c.queues[index] <- task
}

func (c *WorkerProcess) loop(ctx context.Context, index int) {
	defer c.wg.Done()

	count := uint64(0)
	t := time.NewTicker(10 * time.Second)
	for {
		select {
		case task := <-c.queues[index]:
			err := MsgCmd.Dispatcher(strings.ToLower(string(task.Cmds.Args[0])), &Client{Conn: task.Conn}, task.Cmds.Args...)
			if err != nil {
				task.Conn.WriteError("ERR '" + err.Error() + "'")
			}
			task.Conn.FlushOut()
			count++
		case <-t.C:
			c.stats.Store(index, count)
		case <-ctx.Done():
			return
		}
	}
}

func (c *WorkerProcess) Close() {
	c.cancel()
	c.wg.Wait()
	log.Printf("workers thread exit")
}

func (c *WorkerProcess) Stats() map[int]uint64 {
	ret := make(map[int]uint64)
	c.stats.Range(func(key, value interface{}) bool {
		ret[key.(int)] = value.(uint64)
		return true
	})
	return ret
}
