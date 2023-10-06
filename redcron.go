package redcron

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedCron struct {
	Client    *redis.Client
	Name      string
	RepeatSec int
	OffsetSec int
	OnError   func(error)
}

func (r *RedCron) Run(ctx context.Context, f func(context.Context)) {
	for ctx.Err() == nil {
		var tm time.Time
		select {
		case <-ctx.Done():
			return
		case tm = <-time.After(time.Second/32 + time.Duration(rand.Int63n(int64(time.Second)/16))):
		}

		if (tm.Unix()-int64(r.OffsetSec))%int64(r.RepeatSec) != 0 {
			continue
		}

		if !r.setNX(ctx, tm) {
			continue
		}

		func() {
			fctx, fctxCancel := context.WithCancel(ctx)
			defer fctxCancel()

			var wg sync.WaitGroup

			wg.Add(1)
			go func() {
				defer wg.Done()
				tkr := time.NewTicker(time.Second)
				defer tkr.Stop()
				for fctx.Err() == nil {
					select {
					case <-fctx.Done():
						return
					case <-tkr.C:
						var ok bool
						func() {
							rctx, rctxCancel := context.WithTimeout(context.Background(), time.Second/2)
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

			rctx, rctxCancel := context.WithTimeout(ctx, time.Second/2)
			defer rctxCancel()
			r.set(rctx, tm, true)
		}()
	}
}

func (r *RedCron) set(ctx context.Context, tm time.Time, finish bool) (ok bool) {
	d := time.Duration(r.RepeatSec) * time.Second
	if !finish {
		d += time.Second
	} else {
		d -= time.Now().Sub(tm)
		if d <= 0 {
			return r.del(ctx)
		}
		d2 := d.Truncate(time.Second)
		if d-d2 > 0 {
			d2 += time.Second
		}
		d = d2
	}
	cmd := r.Client.Set(ctx, r.Name, tm.Unix(), d)
	if e := cmd.Err(); e != nil {
		r.onError(e)
		return false
	}
	return true
}

func (r *RedCron) setNX(ctx context.Context, tm time.Time) (ok bool) {
	cmd := r.Client.SetNX(ctx, r.Name, tm.Unix(), time.Duration(r.RepeatSec)*time.Second+time.Second)
	if e := cmd.Err(); e != nil {
		r.onError(e)
		return false
	}
	return cmd.Val()
}

func (r *RedCron) del(ctx context.Context) (ok bool) {
	cmd := r.Client.Del(ctx, r.Name)
	if e := cmd.Err(); e != nil {
		r.onError(e)
		return false
	}
	return cmd.Val() >= 1
}

func (r *RedCron) onError(err error) {
	if r.OnError != nil {
		r.OnError(err)
	}
}
