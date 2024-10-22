package redis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"submission-sequencer-collector/config"
	"time"

	"github.com/go-redis/redis/v8"
)

var RedisClient *redis.Client

func NewRedisClient() *redis.Client {
	db, err := strconv.Atoi(config.SettingsObj.RedisDB)
	if err != nil {
		log.Fatalf("Incorrect redis db: %s", err.Error())
	}
	return redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", config.SettingsObj.RedisHost, config.SettingsObj.RedisPort), // Redis server address
		Password:     "",                                                                               // no password set
		DB:           db,
		PoolSize:     1000,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
		DialTimeout:  5 * time.Second,
		IdleTimeout:  5 * time.Minute,
	})
}

func AddToSet(ctx context.Context, set string, keys ...string) error {
	if err := RedisClient.SAdd(ctx, set, keys).Err(); err != nil {
		return fmt.Errorf("unable to add to set: %s", err.Error())
	}
	return nil
}

func GetSetKeys(ctx context.Context, set string) []string {
	return RedisClient.SMembers(ctx, set).Val()
}

func RemoveFromSet(ctx context.Context, set, key string) error {
	return RedisClient.SRem(context.Background(), set, key).Err()
}

func Delete(ctx context.Context, set string) error {
	return RedisClient.Del(ctx, set).Err()
}

func Expire(ctx context.Context, key string, expiration time.Duration) error {
	return RedisClient.Expire(ctx, key, expiration).Err()
}

func Get(ctx context.Context, key string) (string, error) {
	val, err := RedisClient.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		} else {
			return "", err
		}
	}
	return val, nil
}

func Set(ctx context.Context, key, value string, expiration time.Duration) error {
	return RedisClient.Set(ctx, key, value, expiration).Err()
}

// StoreEpochDetails stores the epoch ID in the master set and its associated details in Redis
func StoreEpochDetails(ctx context.Context, dataMarketAddress, epochID, details string, expiration time.Duration) error {
	// Store the epoch ID in the master set
	if err := AddToSet(ctx, EpochMarkerSet(dataMarketAddress), epochID); err != nil {
		return fmt.Errorf("failed to add epoch ID %s to master set for data market %s: %w", epochID, dataMarketAddress, err)
	}

	// Store the details for the specified epochID related to the given data market in Redis
	if err := Set(ctx, EpochMarkerDetails(dataMarketAddress, epochID), details, expiration); err != nil {
		return fmt.Errorf("failed to store epoch marker details in Redis: %w", err)
	}

	return nil
}

func RemoveEpochFromRedis(ctx context.Context, dataMarketAddress, epochID string) error {
	// Remove the epoch marker from the master set
	if err := RedisClient.SRem(ctx, EpochMarkerSet(dataMarketAddress), epochID).Err(); err != nil {
		// Set an expiry as a fallback if removal fails
		if expireErr := RedisClient.Expire(ctx, EpochMarkerSet(dataMarketAddress), 20*time.Minute).Err(); expireErr != nil {
			return fmt.Errorf("failed to set expiry for epoch marker after removal failure: %w", expireErr)
		}

		return fmt.Errorf("failed to delete epoch marker %s from Redis: %w", epochID, err)
	}

	if expireErr := RedisClient.Expire(ctx, EpochMarkerDetails(dataMarketAddress, epochID), 30*time.Minute).Err(); expireErr != nil {
		return fmt.Errorf("failed to set expiry for epoch details after removal failure: %w", expireErr)
	}

	return nil
}
