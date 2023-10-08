package redcron

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type RedCron struct {
	cfg      Config
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	stopping int32
}

func New(cfg Config) (c *RedCron) {
	c = &RedCron{
		cfg: cfg,
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

func (c *RedCron) Run(name string, repeatSec int, offsetSec int, f func(context.Context), tags ...string) {
	cp := cronProperties{
		name:      name,
		repeatSec: repeatSec,
		offsetSec: offsetSec,
		tags:      tags,
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
		case tm = <-time.After(time.Second/32 + time.Duration(rand.Int63n(int64(time.Second)/16))):
		}

		if (tm.Unix()-int64(cp.offsetSec))%int64(cp.repeatSec) != 0 {
			continue
		}

		if !c.setNX(c.ctx, cp, tm) {
			continue
		}

		func() {
			fctx, fctxCancel := context.WithCancel(c.ctx)
			defer fctxCancel()

			var wg sync.WaitGroup

			wg.Add(1)
			go func() {
				defer wg.Done()
				tkr := time.NewTicker(time.Second / 2)
				defer tkr.Stop()
				for fctx.Err() == nil {
					select {
					case <-fctx.Done():
						return
					case <-tkr.C:
						var ok bool
						func() {
							rctx, rctxCancel := context.WithTimeout(context.Background(), time.Second)
							defer rctxCancel()
							ok = c.set(rctx, cp, tm, false)
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

			rctx, rctxCancel := context.WithTimeout(context.Background(), time.Second)
			defer rctxCancel()
			c.set(rctx, cp, tm, true)
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

func (c *RedCron) set(ctx context.Context, cp cronProperties, tm time.Time, finish bool) (ok bool) {
	d := time.Duration(cp.repeatSec) * time.Second
	if !finish {
		d += time.Second
	} else {
		now := time.Now()
		d -= now.Sub(tm)
		if d <= 0 {
			return c.del(ctx, cp)
		}
		t := now.Add(d).Truncate(time.Second)
		if now.After(t) {
			t = t.Add(time.Second)
		}
		d = t.Sub(now)
	}
	cmd := c.cfg.Client.Set(ctx, cp.name, tm.Unix(), d)
	if e := cmd.Err(); e != nil {
		c.cfg.performError(e, cp)
		return false
	}
	return true
}

func (c *RedCron) setNX(ctx context.Context, cp cronProperties, tm time.Time) (ok bool) {
	cmd := c.cfg.Client.SetNX(ctx, cp.name, tm.Unix(), time.Duration(cp.repeatSec)*time.Second+time.Second)
	if e := cmd.Err(); e != nil {
		c.cfg.performError(e, cp)
		return false
	}
	return cmd.Val()
}

func (c *RedCron) del(ctx context.Context, cp cronProperties) (ok bool) {
	cmd := c.cfg.Client.Del(ctx, cp.name)
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
	tags      []string
}
