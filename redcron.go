package redcron

import (
	"context"
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

func New(cfg Config) (r *RedCron) {
	r = &RedCron{
		cfg: cfg,
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	return r
}

func (r *RedCron) Run(f func(context.Context)) {
	if r.stopping != 0 {
		return
	}

	r.wg.Add(1)
	defer r.wg.Done()

	for r.ctx.Err() == nil && r.stopping == 0 {
		var tm time.Time
		select {
		case <-r.ctx.Done():
			return
		case tm = <-time.After(time.Second/32 + time.Duration(rand.Int63n(int64(time.Second)/16))):
		}

		if (tm.Unix()-int64(r.cfg.OffsetSec))%int64(r.cfg.RepeatSec) != 0 {
			continue
		}

		if !r.setNX(r.ctx, tm) {
			continue
		}

		func() {
			fctx, fctxCancel := context.WithCancel(r.ctx)
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
							ok = r.set(rctx, tm, false)
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
			r.set(rctx, tm, true)
		}()
	}
}

func (r *RedCron) Stop(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&r.stopping, 0, 1) {
		return
	}

	stopped := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(stopped)
	}()

	select {
	case <-ctx.Done():
	case <-stopped:
	}

	r.cancel()
	<-stopped
}

func (r *RedCron) set(ctx context.Context, tm time.Time, finish bool) (ok bool) {
	d := time.Duration(r.cfg.RepeatSec) * time.Second
	if !finish {
		d += time.Second
	} else {
		now := time.Now()
		d -= now.Sub(tm)
		if d <= 0 {
			return r.del(ctx)
		}
		t := now.Add(d).Truncate(time.Second)
		if now.After(t) {
			t = t.Add(time.Second)
		}
		d = t.Sub(now)
	}
	cmd := r.cfg.Client.Set(ctx, r.cfg.Name, tm.Unix(), d)
	if e := cmd.Err(); e != nil {
		r.cfg.onError(e)
		return false
	}
	return true
}

func (r *RedCron) setNX(ctx context.Context, tm time.Time) (ok bool) {
	cmd := r.cfg.Client.SetNX(ctx, r.cfg.Name, tm.Unix(), time.Duration(r.cfg.RepeatSec)*time.Second+time.Second)
	if e := cmd.Err(); e != nil {
		r.cfg.onError(e)
		return false
	}
	return cmd.Val()
}

func (r *RedCron) del(ctx context.Context) (ok bool) {
	cmd := r.cfg.Client.Del(ctx, r.cfg.Name)
	if e := cmd.Err(); e != nil {
		r.cfg.onError(e)
		return false
	}
	return cmd.Val() >= 1
}
