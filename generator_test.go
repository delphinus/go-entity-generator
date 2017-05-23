package generator

import (
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/datastore"
)

type testHoge struct {
	ID   int64 `datasotre:"-" goon:"id"`
	Name string
}

func testServer() (context.Context, context.CancelFunc, error) {
	ctx, done, err := aetest.NewContext()
	if err != nil {
		return nil, nil, errors.Wrap(err, "error in NewContext")
	}

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		<-ctx.Done()
		done()
	}()

	return ctx, cancel, nil
}

func TestNew(t *testing.T) {
	ctx, cancel, err := testServer()
	if err != nil {
		t.Fatalf("error in testServer: %v", err)
	}

	_ = New(ctx, &Options{
		Appender: func(ctx context.Context, entities []interface{}, i int, k *datastore.Key) []interface{} {
			return entities
		},
		Query: datastore.NewQuery("testHoge"),
	})

	cancel()
}
