// Package generator can make a generator to yield entities from Datastore.
//
// It can manage any Entity by generator.Processor interface.
//
//  type SomeItem struct {
//    ID   int64  `datastore:"-" goon:"id"`
//    Name string `datastore:",noindex"`
//  }
//
//  func appender(ctx context.Context, entities []interface{}, i int, k *datastore.Key, parentKey *datastore.Key) []interface{} {
//    if k.IntID() == 0 {
//      log.Warningf(ctx, "SomeItem{} needs int64 key. But items[%d] has a string key: %v", i, k.StringID())
//    } else {
//      entities = append(entities, &SomeItem{ID: k.IntID()})
//    }
//  }
//
//  func someFunc(w http.ResponseWriter, r *http.Request) {
//    ctx := appengine.NewContext(r)
//    ctx, cancel := context.WithCancel(ctx)
//    defer cancel() // you should cancel before finishing.
//
//    ch := generator.New(ctx, &generator.Options{
//      Appender: appender,
//      Query:    datastore.NewQuery("SomeItem"),
//    })
//
//    for unit := range ch {
//      if unit.Err != nil {
//        panic(err)
//      }
//
//      for _, e := range unit.Entities {
//        if s, ok := e.(*SomeItem); ok {
//          // some nice handling
//        }
//      }
//    }
//  }
package generator

import (
	"sync"

	"github.com/mjibson/goon"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

// Options is options for Generator
type Options struct {
	// Appender is needed to create entity for real.
	Appender func(ctx context.Context, entities []interface{}, i int, k *datastore.Key, parentKey *datastore.Key) []interface{}
	// FetchLimit is a number of entities that a returned chunk has.  The
	// default value is 100.
	FetchLimit int
	// IgnoreErrFieldMismatch means it ignore ErrFieldMismatch error in
	// fetching.  And it logs that with log.Warnings() func.
	IgnoreErrFieldMismatch bool
	// ParentKey means the key of the parent entity that should be specified if
	// needed.
	ParentKey *datastore.Key
	// Query is the query to execute.
	Query *datastore.Query
}

// Unit will be returned by generator
type Unit struct {
	Entities []interface{}
	Err      error
}

const defaultFetchLimit = 100

// New returns a channel that does as a generator to yield a chunk of entities
// and an error if exists.  The number of entities in the chunk is specified by
// FetchLimit in Options.
func New(ctx context.Context, o *Options) <-chan Unit {
	if o == nil {
		o = &Options{
			FetchLimit: defaultFetchLimit,
		}
	} else if o.FetchLimit == 0 {
		o.FetchLimit = defaultFetchLimit
	}

	in := query(ctx, o)
	out := getMulti(ctx, in, o)

	return out
}

func query(ctx context.Context, o *Options) <-chan Unit {
	in := make(chan Unit)

	go func() {
		defer close(in)

		var cur *datastore.Cursor

		for {
			q := o.Query.KeysOnly()
			if cur != nil {
				q = q.Start(*cur)
			}

			g := goon.FromContext(ctx)
			t := g.Run(q)
			isDone := false
			entities := make([]interface{}, 0, o.FetchLimit)
			for i := 0; i < o.FetchLimit; i++ {
				k, err := t.Next(nil)
				if err == datastore.Done {
					isDone = true
					break
				} else if err != nil {
					in <- Unit{nil, errors.WithStack(err)}
					return
				}
				entities = o.Appender(ctx, entities, i, k, o.ParentKey)
			}

			if !isDone {
				c, err := t.Cursor()
				if err != nil {
					in <- Unit{nil, errors.WithStack(err)}
					return
				}
				cur = &c
			}

			select {
			case <-ctx.Done():
				return
			default:
				in <- Unit{entities, nil}
				if isDone {
					return
				}
			}
		}
	}()

	return in
}

func getMulti(ctx context.Context, in <-chan Unit, o *Options) <-chan Unit {
	out := make(chan Unit)

	go func() {
		var wg sync.WaitGroup
		defer func() {
			wg.Wait()
			close(out)
		}()

		for u := range in {
			if u.Err != nil {
				out <- Unit{nil, errors.WithStack(u.Err)}
				return
			}

			wg.Add(1)
			go func(u Unit) {
				defer wg.Done()

				g := goon.FromContext(ctx)
				if err := g.GetMulti(u.Entities); err != nil {
					if !o.IgnoreErrFieldMismatch {
						out <- Unit{nil, errors.WithStack(err)}
						return
					}

					filtered, err := filter(ctx, u.Entities, err)
					if err != nil {
						out <- Unit{nil, errors.WithStack(err)}
						return
					}

					out <- Unit{filtered, nil}
					return
				}

				out <- u
			}(u)
		}
	}()

	return out
}

func filter(ctx context.Context, entities []interface{}, err error) ([]interface{}, error) {
	if len(entities) == 0 || err == nil {
		return entities, err
	}

	filtered := make([]interface{}, 0, len(entities))

	mErr, ok := err.(appengine.MultiError)
	// non-MultiError error does not have ErrFieldMismatch,
	// ErrInvalidEntityType, and ErrNoSuchEntity, so we do not ignore.
	if !ok {
		return entities, err
	}

	if len(entities) != len(mErr) {
		log.Warningf(ctx, "MultiError has different length => len(entities): %d, len(mErr): %d", len(entities), len(mErr))
		return filtered, nil
	}

	for i := 0; i < len(entities); i++ {
		if mErr[i] == nil {
			filtered = append(filtered, entities[i])
			continue
		}
		if _, ok := mErr[i].(*datastore.ErrFieldMismatch); ok {
			log.Warningf(ctx, "mErr[%d] is ErrFieldMismatch, but ignore this: %v", i, err)
			continue
		}
		return entities, err
	}

	return filtered, nil
}
