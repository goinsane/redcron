package redcron

import "github.com/go-redis/redis/v8"

type Config struct {
	Client  *redis.Client
	OnError func(error)
}

func (c *Config) performError(err error) {
	if c.OnError != nil {
		c.OnError(err)
	}
}
