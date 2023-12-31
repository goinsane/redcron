// Package redcron provides RedCron struct to run cron jobs periodically by using Redis.

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

	"github.com/redis/go-redis/v9"
)

// RedCron registers and runs cron jobs.
type RedCron struct {
	cfg     Config
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	crons   map[string]cronProperties
	cronsMu sync.Mutex
	stopped int32
}

// New creates a new RedCron struct.
func New(cfg Config) (c *RedCron) {
	c = &RedCron{
		cfg:   cfg,
		crons: make(map[string]cronProperties),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

// Register registers a new cron job by the given parameters. It returns the underlying RedCron.
func (c *RedCron) Register(name string, repeatSec int, offsetSec int, f func(context.Context)) *RedCron {
	if name == "" {
		panic(errors.New("name must be non-empty"))
	}
	if repeatSec <= 0 {
		panic(errors.New("repeatSec must be greater than zero"))
	}
	cp := cronProperties{
		name:      name,
		repeatSec: repeatSec,
		offsetSec: offsetSec,
	}
	c.cronsMu.Lock()
	defer c.cronsMu.Unlock()
	if _, ok := c.crons[name]; ok {
		panic(fmt.Errorf("cron %q already registered", name))
	}
	c.crons[name] = cp
	go c.run(cp, f)
	return c
}

// Stop stops triggering cron jobs and waits for all jobs are finished.
// When ctx has been done, all contexts of jobs are cancelled.
// If ctx is nil, all contexts of jobs are cancelled immediately.
func (c *RedCron) Stop(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&c.stopped, 0, 1) {
		//return
	}

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	if ctx != nil {
		select {
		case <-ctx.Done():
		case <-done:
		}
	}

	c.cancel()
	<-done
}

func (c *RedCron) run(cp cronProperties, f func(context.Context)) {
	c.wg.Add(1)
	defer c.wg.Done()

	for c.ctx.Err() == nil && c.stopped == 0 {
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
			rctx, rcancel := context.WithTimeout(context.Background(), opTimeout)
			defer rcancel()
			if !c.setNX(rctx, cp, tm) {
				cont = true
			}
		}()
		if cont {
			continue
		}

		func() {
			fctx, fcancel := context.WithCancel(c.ctx)
			defer fcancel()

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
							rctx, rcancel := context.WithTimeout(context.Background(), opTimeout)
							defer rcancel()
							ok = c.set(rctx, cp, tm)
						}()
						if !ok {
							fcancel()
							return
						}
					}
				}
			}()

			f(fctx)
			fcancel()
			wg.Wait()

			func() {
				rctx, rcancel := context.WithTimeout(context.Background(), opTimeout)
				defer rcancel()
				c.del(rctx, cp)
			}()
		}()
	}
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
