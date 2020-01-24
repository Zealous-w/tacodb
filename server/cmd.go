package server

import (
	"fmt"
	"github.com/Zealous-w/tacodb/command"
	"reflect"
	"runtime"
	"strconv"
	"strings"
)

type ExecFunc func(c *Client, args ...[]byte) error

type Command struct {
	cmds map[string]ExecFunc
}

func NewCommand() *Command {
	c := &Command{
		cmds: make(map[string]ExecFunc),
	}
	c.init()
	return c
}

var MsgCmd *Command

func init() {
	MsgCmd = NewCommand()
}

func (c *Command) init() {
	register := func(execFunc ExecFunc) {
		fullName := runtime.FuncForPC(reflect.ValueOf(execFunc).Pointer()).Name()
		slc := strings.Split(fullName, ".")
		if len(slc) == 0 {
			panic(fmt.Errorf("cmd register func error %s", fullName))
		}

		name := strings.ToLower(strings.TrimLeft(slc[len(slc)-1], "cmd"))
		if _, ok := c.cmds[name]; ok {
			panic(fmt.Errorf("cmd register repeated %s", name))
		}
		c.cmds[name] = execFunc
	}
	/////////
	register(cmdSet)
	register(cmdGet)
	register(cmdDel)
	register(cmdHSet)
	register(cmdHGet)
	register(cmdHDel)
	register(cmdHGetAll)
	register(cmdHKeys)
	register(cmdSAdd)
	register(cmdSRem)
	register(cmdSMembers)
	register(cmdSCard)
	register(cmdLPush)
	register(cmdLPop)
	register(cmdRPush)
	register(cmdRPop)
	register(cmdLRange)
	register(cmdLTrim)
	register(cmdLLen)
	register(cmdZAdd)
	register(cmdZRem)
	register(cmdZRange)
	register(cmdZIncrby)
	register(cmdZCount)
	register(cmdZRevRange)
	register(cmdZRank)
	register(cmdZCard)
}

func (c *Command) Dispatcher(cmd string, client *Client, args ...[]byte) error {
	if _, ok := c.cmds[cmd]; !ok {
		return fmt.Errorf("not found cmds %s", cmd)
	}
	return c.cmds[cmd](client, args...)
}

/////////
func cmdSet(c *Client, args ...[]byte) error {
	if len(args) != 3 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	err := db.Set(args[1], args[2], 0)
	if err != nil {
		return err
	}
	c.Conn.WriteString("OK")
	return nil
}

func cmdGet(c *Client, args ...[]byte) error {
	if len(args) != 2 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret := db.Get(args[1])
	if ret == nil {
		c.Conn.WriteNull()
		return nil
	}
	c.Conn.WriteString(string(ret))
	return nil
}

func cmdDel(c *Client, args ...[]byte) error {
	if len(args) != 2 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret := db.Del(args[1])
	c.Conn.WriteInt(ret)
	return nil
}

func cmdHSet(c *Client, args ...[]byte) error {
	if len(args) < 4 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	err := db.HSet(args[1], args[2:]...)
	if err != nil {
		c.Conn.WriteNull()
		return err
	}
	c.Conn.WriteString(string("OK"))
	return nil
}

func cmdHGet(c *Client, args ...[]byte) error {
	if len(args) != 3 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret, err := db.HGet(args[1], args[2:]...)
	if err != nil {
		c.Conn.WriteNull()
		return nil
	}
	if len(ret) == 0 {
		c.Conn.WriteNull()
		return nil
	}
	c.Conn.WriteString(string(ret[0]))
	return nil
}

func cmdHDel(c *Client, args ...[]byte) error {
	if len(args) != 3 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret, err := db.HDel(args[1], args[2:]...)
	if err != nil {
		c.Conn.WriteNull()
		return nil
	}
	c.Conn.WriteInt(int(ret))
	return nil
}

func cmdHGetAll(c *Client, args ...[]byte) error {
	if len(args) != 2 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret, err := db.HGetAll(args[1])
	if err != nil {
		c.Conn.WriteNull()
		return nil
	}
	if len(ret) == 0 {
		c.Conn.WriteNull()
		return nil
	}
	c.Conn.WriteArray(len(ret) * 2)
	for _, v := range ret {
		c.Conn.WriteBulk(v.V0)
		c.Conn.WriteBulk(v.V1)
	}
	return nil
}

func cmdHKeys(c *Client, args ...[]byte) error {
	if len(args) != 2 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret := db.HKeys(args[1])
	if ret == nil {
		c.Conn.WriteNull()
		return nil
	}
	if len(ret) == 0 {
		c.Conn.WriteNull()
		return nil
	}
	c.Conn.WriteArray(len(ret))
	for _, v := range ret {
		c.Conn.WriteBulk(v)
	}
	return nil
}

func cmdSAdd(c *Client, args ...[]byte) error {
	if len(args) < 3 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret := db.SAdd(args[1], args[2:]...)
	if ret != nil {
		c.Conn.WriteNull()
		return nil
	}
	c.Conn.WriteString("OK")
	return nil
}

func cmdSRem(c *Client, args ...[]byte) error {
	if len(args) < 3 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret := db.SRem(args[1], args[2:]...)
	if ret != nil {
		c.Conn.WriteString("ERR " + ret.Error())
		return nil
	}
	c.Conn.WriteString("OK")
	return nil
}

func cmdSMembers(c *Client, args ...[]byte) error {
	if len(args) != 2 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret, err := db.SMembers(args[1], args[2:]...)
	if err != nil {
		c.Conn.WriteString("ERR " + err.Error())
		return nil
	}

	if len(ret) == 0 {
		c.Conn.WriteNull()
		return nil
	}
	c.Conn.WriteArray(len(ret))
	for _, v := range ret {
		c.Conn.WriteBulk(v)
	}
	return nil
}

func cmdSCard(c *Client, args ...[]byte) error {
	if len(args) != 2 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret, err := db.SCard(args[1])
	if err != nil {
		c.Conn.WriteString("ERR " + err.Error())
		return nil
	}

	c.Conn.WriteInt(int(ret))
	return nil
}

func cmdLPush(c *Client, args ...[]byte) error {
	if len(args) < 3 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	err := db.LPush(args[1], args[2:]...)
	if err != nil {
		c.Conn.WriteString("ERR " + err.Error())
		return nil
	}

	c.Conn.WriteString("OK")
	return nil
}

func cmdLPop(c *Client, args ...[]byte) error {
	if len(args) != 2 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret := db.LPop(args[1])
	if ret == nil {
		c.Conn.WriteString("ERR Not Found item")
		return nil
	}

	c.Conn.WriteBulk(ret)
	return nil
}

func cmdRPush(c *Client, args ...[]byte) error {
	if len(args) < 3 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	err := db.RPush(args[1], args[2:]...)
	if err != nil {
		c.Conn.WriteString("ERR " + err.Error())
		return nil
	}

	c.Conn.WriteString("OK")
	return nil
}

func cmdRPop(c *Client, args ...[]byte) error {
	if len(args) != 2 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret := db.RPop(args[1])
	if ret == nil {
		c.Conn.WriteString("ERR Not Found item")
		return nil
	}

	c.Conn.WriteString(string(ret))
	return nil
}

func cmdLRange(c *Client, args ...[]byte) error {
	if len(args) != 4 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	start, err := strconv.Atoi(string(args[2]))
	if err != nil {
		c.Conn.WriteError("ERR value is not an integer or out of range")
		return nil
	}
	end, err := strconv.Atoi(string(args[3]))
	if err != nil {
		c.Conn.WriteError("ERR value is not an integer or out of range")
		return nil
	}
	ret := db.LRange(args[1], start, end)
	if ret == nil {
		c.Conn.WriteString("empty list or set ")
		return nil
	}

	c.Conn.WriteArray(len(ret))
	for _, v := range ret {
		c.Conn.WriteBulk(v)
	}
	return nil
}

func cmdLTrim(c *Client, args ...[]byte) error {
	if len(args) != 4 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	start, err := strconv.Atoi(string(args[2]))
	if err != nil {
		c.Conn.WriteError("ERR value is not an integer or out of range")
		return nil
	}
	end, err := strconv.Atoi(string(args[3]))
	if err != nil {
		c.Conn.WriteError("ERR value is not an integer or out of range")
		return nil
	}
	err = db.LTrim(args[1], start, end)
	if err != nil {
		c.Conn.WriteString("empty list or set ")
		return nil
	}

	c.Conn.WriteString("OK")
	return nil
}

func cmdLLen(c *Client, args ...[]byte) error {
	if len(args) != 2 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret := db.LLen(args[1])
	c.Conn.WriteInt(int(ret))
	return nil
}

func cmdZAdd(c *Client, args ...[]byte) error {
	if len(args) != 4 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	score, err := strconv.ParseUint(string(args[2]), 10, 64)
	if err != nil {
		c.Conn.WriteError("ERR value is not an integer or out of range")
		return nil
	}
	err = db.ZAdd(args[1], score, args[3])
	if err != nil {
		c.Conn.WriteError("ERR " + err.Error())
		return nil
	}
	c.Conn.WriteInt(int(1))
	return nil
}

func cmdZRem(c *Client, args ...[]byte) error {
	if len(args) != 3 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	err := db.ZRem(args[1], args[2:]...)
	if err != nil {
		c.Conn.WriteError("ERR " + err.Error() + ":" + string(args[1]))
		return nil
	}
	c.Conn.WriteInt(int(1))
	return nil
}

func cmdZRange(c *Client, args ...[]byte) error {
	if len(args) < 4 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret := db.ZRange(args[1], args[2:]...)
	if ret == nil {
		c.Conn.WriteError("ERR not found key" + ":" + string(args[1]))
		return nil
	}
	c.Conn.WriteArray(len(ret))
	for _, v := range ret {
		c.Conn.WriteBulk(v)
	}
	return nil
}

func cmdZIncrby(c *Client, args ...[]byte) error {
	if len(args) != 4 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret, err := db.ZIncrby(args[1], args[2:]...)
	if err != nil {
		c.Conn.WriteError("ERR " + err.Error())
		return nil
	}
	c.Conn.WriteBulk(ret)
	return nil
}

func cmdZCount(c *Client, args ...[]byte) error {
	if len(args) != 4 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret, err := db.ZCount(args[1], args[2:]...)
	if err != nil {
		c.Conn.WriteError("ERR " + err.Error())
		return nil
	}
	c.Conn.WriteInt(ret)
	return nil
}

func cmdZRevRange(c *Client, args ...[]byte) error {
	if len(args) < 4 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret := db.ZRevRange(args[1], args[2:]...)
	if ret == nil {
		c.Conn.WriteError("ERR not found key" + ":" + string(args[1]))
		return nil
	}
	c.Conn.WriteArray(len(ret))
	for _, v := range ret {
		c.Conn.WriteBulk(v)
	}
	return nil
}

func cmdZRank(c *Client, args ...[]byte) error {
	if len(args) != 3 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret, err := db.ZRank(args[1], args[2])
	if err != nil {
		c.Conn.WriteNull()
		return nil
	}
	c.Conn.WriteInt(ret)
	return nil
}

func cmdZCard(c *Client, args ...[]byte) error {
	if len(args) != 2 {
		c.Conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return nil
	}
	db := c.Conn.Context().(*command.RedisCommand)
	ret, err := db.ZCard(args[1])
	if err != nil {
		c.Conn.WriteNull()
		return nil
	}
	c.Conn.WriteInt(ret)
	return nil
}
