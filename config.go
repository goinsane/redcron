package redcron

import "github.com/go-redis/redis/v8"

type Config struct {
	Client    *redis.Client
	Name      string
	RepeatSec int
	OffsetSec int
	OnError   func(error)
}

func (c *Config) onError(err error) {
	if c.OnError != nil {
		c.OnError(err)
	}
}
