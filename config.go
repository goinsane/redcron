package redcron

import "github.com/go-redis/redis/v8"

type Config struct {
	Client  *redis.Client
	OnError func(err error, tags ...string)
}

func (c *Config) performError(err error, cp cronProperties) {
	if c.OnError != nil {
		c.OnError(err, cp.tags...)
	}
}
