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

type testOldHoge struct {
	_kind   string         `goon:"kind,testHoge"`
	ID      int64          `datastore:"-" goon:"id"`
	Parent  *datastore.Key `datastore:"-" goon:"parent"`
	OldName string
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
const chunkSize = 10

func createSampleHoge(ctx context.Context) (*datastore.Key, error) {
	g := goon.FromContext(ctx)

	p := testParent{ID: 1}
	parentKey, err := g.Put(&p)
	if err != nil {
		return nil, errors.Wrap(err, "error in Put")
	}

	h := make([]*testHoge, allHoges)
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

	oldHoge := testOldHoge{
		OldName: "Old Hoge",
		Parent:  parentKey,
	}
	if _, err := g.Put(&oldHoge); err != nil {
		return nil, errors.Wrap(err, "error in Put")
	}

	return parentKey, nil
}

func testFetch(ctx context.Context, expected int, o *Options) error {
	o.Appender = func(ctx context.Context, entities []interface{}, i int, k *datastore.Key, parentKey *datastore.Key) []interface{} {
		return append(entities, &testHoge{
			ID:     k.IntID(),
			Parent: parentKey,
		})
	}
	if o.ChunkSize == 0 {
		o.ChunkSize = chunkSize
	}

	ch := New(ctx, o)

	count := 0
	for unit := range ch {
		if unit.Err != nil {
			return errors.Wrap(unit.Err, "error in unit")
		}

		for _, e := range unit.Entities {
			if _, ok := e.(*testHoge); !ok {
				return errors.Errorf("e is not *testHoge: %+v", e)
			}
			count++
		}
	}

	if count != expected {
		return errors.Errorf("number differs => expected: %d, result: %d", allHoges, count)
	}

	return nil
}

func TestFetchAll(t *testing.T) {
	ctx, cancel, err := testServer()
	if err != nil {
		t.Fatalf("error in testServer: %+v", err)
	}
	defer cancel()

	parentKey, err := createSampleHoge(ctx)
	if err != nil {
		t.Fatalf("error in createSampleHoge: %+v", err)
	}

	q := datastore.NewQuery("testHoge").Ancestor(parentKey)
	if err := testFetch(ctx, allHoges, &Options{
		IgnoreErrFieldMismatch: true,
		ParentKey:              parentKey,
		Query:                  q,
	}); err != nil {
		t.Fatalf("error in testFetch: %+v", err)
	}
}

func TestFetchFuga(t *testing.T) {
	ctx, cancel, err := testServer()
	if err != nil {
		t.Fatalf("error in testServer: %+v", err)
	}
	defer cancel()

	parentKey, err := createSampleHoge(ctx)
	if err != nil {
		t.Fatalf("error in createSampleHoge: %+v", err)
	}

	q := datastore.NewQuery("testHoge").Ancestor(parentKey).Filter("Name =", "Fuga Hogeo")
	if err := testFetch(ctx, allFugas, &Options{
		ParentKey: parentKey,
		Query:     q,
	}); err != nil {
		t.Fatalf("error in testFetch: %+v", err)
	}
}

func TestChangeChunkSize(t *testing.T) {
	ctx, cancel, err := testServer()
	if err != nil {
		t.Fatalf("error in testServer: %+v", err)
	}
	defer cancel()

	parentKey, err := createSampleHoge(ctx)
	if err != nil {
		t.Fatalf("error in createSampleHoge: %+v", err)
	}

	q := datastore.NewQuery("testHoge").Ancestor(parentKey).Filter("Name =", "Fuga Hogeo")
	if err := testFetch(ctx, allFugas, &Options{
		ChunkSize: 5,
		ParentKey: parentKey,
		Query:     q,
	}); err != nil {
		t.Fatalf("error in testFetch: %+v", err)
	}
}
