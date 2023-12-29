package redcron

import "github.com/redis/go-redis/v9"

// Config holds RedCron config.
type Config struct {
	Client  *redis.Client
	OnError func(err error, name string)
}

func (c *Config) performError(err error, cp cronProperties) {
	if c.OnError != nil {
		c.OnError(err, cp.name)
	}
}
