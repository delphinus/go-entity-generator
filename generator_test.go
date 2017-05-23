package generator

import (
	"testing"

	"github.com/mjibson/goon"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/datastore"
)

type testParent struct {
	ID int64 `datastore:"-" goon:"id"`
}

type testHoge struct {
	ID     int64          `datasotre:"-" goon:"id"`
	Parent *datastore.Key `datastore:"-" goon:"parent"`
	Name   string
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
		Appender: func(ctx context.Context, entities []interface{}, i int, k *datastore.Key, parentKey *datastore.Key) []interface{} {
			return entities
		},
		Query: datastore.NewQuery("testHoge"),
	})

	cancel()
}

const allHoges = 55
const allFugas = 55 - 23
const fetchLimit = 10

func createSampleHoge(ctx context.Context) (*datastore.Key, error) {
	g := goon.FromContext(ctx)

	p := testParent{ID: 1}
	parentKey, err := g.Put(&p)
	if err != nil {
		return nil, errors.Wrap(err, "error in Put")
	}

	h := make([]*testHoge, 55)
	for i := range h {
		name := ""
		if i < allHoges-allFugas {
			name = "Hoge Fugao"
		} else {
			name = "Fuga Hogeo"
		}
		h[i] = &testHoge{
			Parent: parentKey,
			Name:   name,
		}
	}

	if _, err := g.PutMulti(h); err != nil {
		return nil, errors.Wrap(err, "error in PutMulti")
	}

	return parentKey, nil
}

func TestFetchAll(t *testing.T) {
	ctx, cancel, err := testServer()
	if err != nil {
		t.Fatalf("error in testServer: %v", err)
	}
	defer cancel()

	parentKey, err := createSampleHoge(ctx)
	if err != nil {
		t.Fatalf("error in createSampleHoge: %v", err)
	}

	ch := New(ctx, &Options{
		Appender: func(ctx context.Context, entities []interface{}, i int, k *datastore.Key, parentKey *datastore.Key) []interface{} {
			return append(entities, &testHoge{
				ID:     k.IntID(),
				Parent: parentKey,
			})
		},
		FetchLimit: fetchLimit,
		ParentKey:  parentKey,
		Query:      datastore.NewQuery("testHoge").Ancestor(parentKey),
	})

	count := 0
	for unit := range ch {
		if unit.Err != nil {
			cancel()
			t.Fatalf("error in unit: %+v", unit.Err)
		}

		for _, e := range unit.Entities {
			if _, ok := e.(*testHoge); !ok {
				t.Fatalf("e is not *testHoge: %+v", e)
			}
			count++
		}
	}

	if count != allHoges {
		t.Fatalf("number differs => expected: %d, result: %d", allHoges, count)
	}
}
