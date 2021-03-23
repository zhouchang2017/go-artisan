package cache

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/tal-tech/go-zero/core/logx"
)

var (
	enableSync bool
	pubDb      *redis.Client
)

// redis订阅通讯，用于清除运行中内存缓存
const subChannel = "model.cached.delete"

type message struct {
	Hostname string
	Table    string
	Keys     []string
}

func SyncLocalCache(ctx context.Context, rdb *redis.Client) {
	enableSync = true
	pubDb = rdb

	subscribe := pubDb.Subscribe(ctx, subChannel)
	channel := subscribe.Channel()

	defer func() {
		subscribe.Close()
		pubDb.Close()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-channel:
			if msg.Channel == subChannel {
				var req message
				if err := json.UnmarshalFromString(msg.Payload, &req); err != nil {
					logx.Errorf("%s", err.Error())
					continue
				}
				if store, ok := stores[req.Table]; ok {
					store.deleteLocalCache(req.Keys...)
				}
			}
		}
	}
}

func publishDeleteLocalCache() {
	if pubDb != nil {
		//pubDb.Publish()
	}
}
