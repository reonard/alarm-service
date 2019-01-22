package db

import (
	"github.com/patrickmn/go-cache"
	"time"
)

var InMemoryCache *cache.Cache

func InitCache() {

	InMemoryCache = cache.New(24*time.Hour, 24*time.Minute)
}
