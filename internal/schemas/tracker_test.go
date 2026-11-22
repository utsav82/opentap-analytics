package schemas

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/opentap/opentap/internal/models"
	"github.com/opentap/opentap/pkg/cache"
	"github.com/redis/go-redis/v9"
)

// Intercept only Redis pipelines, keeping the real Track/Flush command paths
// while making outages deterministic and avoiding an external Redis service.
type trackerRedisHook struct{ count func() error }

func (h *trackerRedisHook) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (h *trackerRedisHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h *trackerRedisHook) ProcessPipelineHook(_ redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(_ context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if cmd.Name() == "pfcount" && h.count != nil {
				if err := h.count(); err != nil {
					return err
				}
			}
			cmd.(*redis.IntCmd).SetVal(3)
		}
		return nil
	}
}

type trackerRepository struct {
	Repository
	upsert func([]*models.EventSchema) error
}

func (r *trackerRepository) UpsertBatch(_ context.Context, batch []*models.EventSchema) error {
	return r.upsert(batch)
}

func TestFlushRetainsFailedBatchAndConcurrentObservations(t *testing.T) {
	for _, failure := range []string{"redis", "database"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			outage := errors.New("temporary outage")
			entered, release := make(chan struct{}), make(chan struct{})
			block := true
			fail := true
			failOperation := func() error {
				if block {
					block = false
					close(entered)
					<-release
				}
				if fail {
					return outage
				}
				return nil
			}
			saved := map[string]models.EventSchema{}
			repo := &trackerRepository{upsert: func(batch []*models.EventSchema) error {
				if failure == "database" {
					if err := failOperation(); err != nil {
						return err
					}
				}
				for _, s := range batch {
					saved[s.PropertyName] = *s
				}
				return nil
			}}
			client := redis.NewClient(&redis.Options{})
			t.Cleanup(func() { client.Close() })
			hook := &trackerRedisHook{}
			if failure == "redis" {
				hook.count = failOperation
			}
			client.AddHook(hook)
			tracker := NewTracker(repo, &cache.RedisClient{Client: client})
			project := uuid.New()
			event := func(properties map[string]interface{}) *models.Event {
				return &models.Event{ProjectID: project, EventName: "purchase", Properties: properties}
			}
			if err := tracker.Track(ctx, event(map[string]interface{}{"amount": 10, "original": true})); err != nil {
				t.Fatal(err)
			}
			key := distinctKey(project, "purchase", "amount")
			first := time.Now().Add(-time.Hour).UTC()
			tracker.pending[key].FirstSeen = first
			type result struct {
				count int
				err   error
			}
			done := make(chan result, 1)
			go func() { n, err := tracker.Flush(ctx); done <- result{n, err} }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				close(release)
				t.Fatal("flush did not reach injected outage")
			}
			trackErr := tracker.Track(ctx, event(map[string]interface{}{"amount": "unknown", "new": "value"}))
			last := tracker.pending[key].LastSeen
			close(release)
			got := <-done
			if trackErr != nil {
				t.Fatal(trackErr)
			}
			if !errors.Is(got.err, outage) || got.count != 0 {
				t.Fatalf("flush = %d, %v; want zero and outage", got.count, got.err)
			}
			if len(saved) != 0 {
				t.Fatal("outage unexpectedly persisted a batch")
			}
			if n, err := tracker.Flush(ctx); !errors.Is(err, outage) || n != 0 {
				t.Fatalf("repeated outage = %d, %v", n, err)
			}
			fail = false
			if n, err := tracker.Flush(ctx); err != nil || n != 3 {
				t.Fatalf("retry = %d, %v; want three schemas", n, err)
			}
			amount := saved["amount"]
			if amount.PropertyType != "mixed" || !amount.FirstSeen.Equal(first) || !amount.LastSeen.Equal(last) {
				t.Fatalf("retry lost merged observations: %+v", amount)
			}
			if saved["original"].PropertyType != "boolean" || saved["new"].PropertyType != "string" || amount.DistinctValuesCount != 3 {
				t.Fatalf("retry lost properties or counts: %+v", saved)
			}
			if n, err := tracker.Flush(ctx); err != nil || n != 0 {
				t.Fatalf("successful batch still pending: %d, %v", n, err)
			}
		})
	}
}
