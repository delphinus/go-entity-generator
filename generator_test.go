package generator

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/mjibson/goon"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
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

func appender(ctx context.Context, entities []interface{}, i int, k *datastore.Key, parentKey *datastore.Key) []interface{} {
	return append(entities, &testHoge{
		ID:     k.IntID(),
		Parent: parentKey,
	})
}

func testFetch(ctx context.Context, expected int, o *Options) error {
	if o.Appender == nil {
		o.Appender = appender
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

func TestNoOptions(t *testing.T) {
	ctx, cancel, err := testServer()
	if err != nil {
		t.Fatalf("error in testServer: %+v", err)
	}
	defer cancel()

	_ = New(ctx, nil)
}

func TestNoQuery(t *testing.T) {
	ctx, cancel, err := testServer()
	if err != nil {
		t.Fatalf("error in testServer: %+v", err)
	}
	defer cancel()

	_ = New(ctx, &Options{ChunkSize: 5})
}

func TestIgnoreAll(t *testing.T) {
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
	if err := testFetch(ctx, 0, &Options{
		Appender: func(ctx context.Context, entities []interface{}, i int, k *datastore.Key, parentKey *datastore.Key) []interface{} {
			return []interface{}{}
		},
		IgnoreErrFieldMismatch: true,
		ParentKey:              parentKey,
		Query:                  q,
	}); err != nil {
		t.Fatalf("error in testFetch: %+v", err)
	}
}

func TestCatchErrFieldMismatch(t *testing.T) {
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
	err = testFetch(ctx, 0, &Options{
		ParentKey: parentKey,
		Query:     q,
	})

	if err == nil {
		t.Fatalf("no error in testFetch")
	}

	err = errors.Cause(err)
	mErr, ok := err.(appengine.MultiError)
	if !ok {
		t.Fatalf("err is not MultiError: %+v", err)
	}

	found := false
	for _, e := range mErr {
		if e == nil {
			continue
		}
		ty := reflect.TypeOf(e).String()
		expected := reflect.TypeOf(&datastore.ErrFieldMismatch{}).String()
		if ty != expected {
			t.Fatalf("type: %s, expected: %s", ty, expected)
		}
		found = true
	}

	if !found {
		t.Fatalf("ErrFieldMismatch is not found")
	}
}

func TestFilterNoEntities(t *testing.T) {
	ctx := context.Background()
	someEntities := []interface{}{}
	someEntitiesStr := fmt.Sprintf("%s", someEntities)
	var someErr error

	entities, err := filter(ctx, someEntities, someErr)
	entitiesStr := fmt.Sprintf("%s", entities)
	if someEntitiesStr != entitiesStr || someErr != err {
		t.Fatalf("entities or err differs")
	}
}

func TestFilterInvalidError(t *testing.T) {
	ctx := context.Background()
	someEntities := []interface{}{1}
	someEntitiesStr := fmt.Sprintf("%s", someEntities)
	someErr := errors.New("hoge error")

	entities, err := filter(ctx, someEntities, someErr)
	entitiesStr := fmt.Sprintf("%s", entities)
	if someEntitiesStr != entitiesStr || someErr != err {
		t.Fatalf("entities or err differs")
	}
}

func TestFilterInvalidMultiErrorWithDifferentLength(t *testing.T) {
	ctx, cancel, err := testServer()
	if err != nil {
		t.Fatalf("error in testServer: %+v", err)
	}
	defer cancel()

	someEntities := []interface{}{1, 2, 3}
	someEntitiesStr := fmt.Sprintf("%s", someEntities)
	someErr := appengine.MultiError([]error{errors.New("hoge error")})
	someErrStr := fmt.Sprintf("%s", someErr)

	entities, err := filter(ctx, someEntities, someErr)
	entitiesStr := fmt.Sprintf("%s", entities)
	errStr := fmt.Sprintf("%s", err)
	if someEntitiesStr != entitiesStr || someErrStr != errStr {
		t.Fatalf("entities or err differs")
	}
}

func TestFilterInvalidMultiErrorWithSameLength(t *testing.T) {
	ctx, cancel, err := testServer()
	if err != nil {
		t.Fatalf("error in testServer: %+v", err)
	}
	defer cancel()

	someEntities := []interface{}{1}
	someEntitiesStr := fmt.Sprintf("%s", someEntities)
	someErr := appengine.MultiError([]error{errors.New("hoge error")})
	someErrStr := fmt.Sprintf("%s", someErr)

	entities, err := filter(ctx, someEntities, someErr)
	entitiesStr := fmt.Sprintf("%s", entities)
	errStr := fmt.Sprintf("%s", err)
	if someEntitiesStr != entitiesStr || someErrStr != errStr {
		t.Fatalf("entities or err differs")
	}
}

func TestGetMultiWithInvalidError(t *testing.T) {
	ctx, cancel, err := testServer()
	if err != nil {
		t.Fatalf("error in testServer: %+v", err)
	}
	defer cancel()

	in := make(chan Unit)
	out := getMulti(ctx, in, &Options{IgnoreErrFieldMismatch: true})

	in <- Unit{[]interface{}{1}, nil}
	u := <-out

	errStr := fmt.Sprintf("%s", errors.Cause(u.Err))
	if u.Entities != nil || !strings.Contains(errStr, "goon: Expected struct, got instead:") {
		t.Fatalf("entities or err differs")
	}
}

func TestQueryWithCancelled(t *testing.T) {
	ctx, cancel, err := testServer()
	if err != nil {
		t.Fatalf("error in testServer: %+v", err)
	}

	parentKey, err := createSampleHoge(ctx)
	if err != nil {
		t.Fatalf("error in createSampleHoge: %+v", err)
	}

	q := datastore.NewQuery("testHoge").Ancestor(parentKey)
	in := query(ctx, &Options{
		Query: q,
	})

	<-in
	cancel()

	if _, ok := <-in; ok {
		t.Fatalf("in has not been closed")
	}
}
