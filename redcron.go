package redcron

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedCron struct {
	cfg      Config
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	stopping int32
	no       int32
}

func New(cfg Config) (c *RedCron) {
	c = &RedCron{
		cfg: cfg,
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

func (c *RedCron) Run(name string, repeatSec int, offsetSec int, f func(context.Context)) {
	cp := cronProperties{
		name:      name,
		repeatSec: repeatSec,
		offsetSec: offsetSec,
		no:        atomic.AddInt32(&c.no, 1),
	}
	c.run(cp, f)
}

func (c *RedCron) Background(name string, repeatSec int, offsetSec int, f func(context.Context)) *RedCron {
	cp := cronProperties{
		name:      name,
		repeatSec: repeatSec,
		offsetSec: offsetSec,
		no:        atomic.AddInt32(&c.no, 1),
	}
	go c.run(cp, f)
	return c
}

func (c *RedCron) run(cp cronProperties, f func(context.Context)) {
	if cp.name == "" {
		panic(errors.New("name must be non-empty"))
	}
	if cp.repeatSec <= 0 {
		panic(errors.New("repeatSec must be greater than zero"))
	}

	if c.stopping != 0 {
		return
	}

	c.wg.Add(1)
	defer c.wg.Done()

	for c.ctx.Err() == nil && c.stopping == 0 {
		var tm time.Time
		select {
		case <-c.ctx.Done():
			return
		case tm = <-time.After(getDriftInterval()):
		}

		if (tm.Unix()-int64(cp.offsetSec))%int64(cp.repeatSec) != 0 {
			continue
		}

		cont := false
		func() {
			rctx, rctxCancel := context.WithTimeout(context.Background(), opTimeout)
			defer rctxCancel()
			if !c.setNX(rctx, cp, tm) {
				cont = true
			}
		}()
		if cont {
			continue
		}

		func() {
			fctx, fctxCancel := context.WithCancel(c.ctx)
			defer fctxCancel()

			var wg sync.WaitGroup

			wg.Add(1)
			go func() {
				defer wg.Done()
				tkr := time.NewTicker(tickerInterval)
				defer tkr.Stop()
				for fctx.Err() == nil {
					select {
					case <-fctx.Done():
						return
					case <-tkr.C:
						var ok bool
						func() {
							rctx, rctxCancel := context.WithTimeout(context.Background(), opTimeout)
							defer rctxCancel()
							ok = c.set(rctx, cp, tm)
						}()
						if !ok {
							fctxCancel()
							return
						}
					}
				}
			}()

			f(fctx)
			fctxCancel()
			wg.Wait()

			rctx, rctxCancel := context.WithTimeout(context.Background(), opTimeout)
			defer rctxCancel()
			c.del(rctx, cp)
		}()
	}
}

func (c *RedCron) Stop(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&c.stopping, 0, 1) {
		return
	}

	stopped := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(stopped)
	}()

	select {
	case <-ctx.Done():
	case <-stopped:
	}

	c.cancel()
	<-stopped
}

func (c *RedCron) set(ctx context.Context, cp cronProperties, tm time.Time) (ok bool) {
	cmd := c.cfg.Client.Set(ctx, genKey(cp), genVal(cp, tm), waitDur)
	if e := cmd.Err(); e != nil {
		c.cfg.performError(e, cp)
		return false
	}
	return true
}

func (c *RedCron) setNX(ctx context.Context, cp cronProperties, tm time.Time) (ok bool) {
	var cmd *redis.BoolCmd

	cmd = c.cfg.Client.SetNX(ctx, genTmKey(cp, tm), genVal(cp, tm), waitDur+time.Duration(cp.repeatSec)*time.Second)
	if e := cmd.Err(); e != nil {
		c.cfg.performError(e, cp)
		return false
	}
	if !cmd.Val() {
		return false
	}

	cmd = c.cfg.Client.SetNX(ctx, genKey(cp), genVal(cp, tm), waitDur)
	if e := cmd.Err(); e != nil {
		c.cfg.performError(e, cp)
		return false
	}
	if !cmd.Val() {
		return false
	}

	return true
}

func (c *RedCron) del(ctx context.Context, cp cronProperties) (ok bool) {
	cmd := c.cfg.Client.Del(ctx, genKey(cp))
	if e := cmd.Err(); e != nil {
		c.cfg.performError(e, cp)
		return false
	}
	return cmd.Val() >= 1
}

type cronProperties struct {
	name      string
	repeatSec int
	offsetSec int
	no        int32
}

func getDriftInterval() time.Duration {
	return time.Second/32 + time.Duration(rand.Int63n(int64(time.Second)/4))
}

func genKey(cp cronProperties) string {
	return fmt.Sprintf("%q", cp.name)
}

func genTmKey(cp cronProperties, tm time.Time) string {
	return fmt.Sprintf("%q %d", cp.name, tm.Unix())
}

func genVal(cp cronProperties, tm time.Time) string {
	result := &struct {
		Time      time.Time
		Name      string
		RepeatSec int
		OffsetSec int
	}{
		Time:      tm.UTC(),
		Name:      cp.name,
		RepeatSec: cp.repeatSec,
		OffsetSec: cp.offsetSec,
	}
	b, _ := json.Marshal(result)
	return string(b)
}

const (
	opTimeout      = 5 * time.Second
	tickerInterval = 1 * time.Second
	waitDur        = 1 * time.Minute
)
