// Package generator can make a generator to yield entities from Datastore.
//
// It can manage any Entity by generator.Processor interface.
//
//  type SomeItem struct {
//    ID   int64  `datastore:"-" goon:"id"`
//    Name string `datastore:",noindex"`
//  }
//
//  func appender(ctx context.Context, entities []interface{}, i int, k *datastore.Key) []interface{} {
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
//
//    ch := generator.New(ctx, &generator.Options{
//      Appender: appender,
//      Query:    datastore.NewQuery("SomeItem"),
//    })
//
//    for unit := range {
//      if unit.Err != nil {
//        cancel() // you should cancel before finishing.
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
	Appender func(ctx context.Context, entities []interface{}, i int, k *datastore.Key) []interface{}
	// FetchLimit is a number of entities that a returned chunk has.  The
	// default value is 100.
	FetchLimit int
	// IgnoreErrFieldMismatch means it ignore ErrFieldMismatch error in
	// fetching.  And it logs that with log.Warnings() func.
	IgnoreErrFieldMismatch bool
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
func New(ctx context.Context, o *Options) chan Unit {
	if o == nil {
		o = &Options{
			FetchLimit: defaultFetchLimit,
		}
	} else if o.FetchLimit == 0 {
		o.FetchLimit = defaultFetchLimit
	}

	ch := make(chan Unit)

	go func() {
		defer close(ch)

		var cur *datastore.Cursor

	loop:
		for {
			isDone, entities, err := process(ctx, o, cur)

			select {
			case <-ctx.Done():
				break loop
			default:
				ch <- Unit{entities, err}
				if err != nil || isDone {
					break loop
				}
			}
		}
	}()

	return ch
}

func process(ctx context.Context, o *Options, cur *datastore.Cursor) (bool, []interface{}, error) {
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
			return false, entities, errors.WithStack(err)
		}
		entities = o.Appender(ctx, entities, i, k)
	}

	if !isDone {
		c, err := t.Cursor()
		if err != nil {
			return false, entities, errors.WithStack(err)
		}
		*cur = c
	}

	if len(entities) == 0 {
		return true, entities, nil
	}

	if err := g.GetMulti(entities); err != nil {
		if !o.IgnoreErrFieldMismatch {
			return isDone, entities, errors.WithStack(err)
		}

		filtered, err := filter(ctx, entities, err)
		if err != nil {
			return isDone, entities, errors.WithStack(err)
		}
		entities = filtered
	}

	return isDone, entities, nil
}

func filter(ctx context.Context, entities []interface{}, err error) ([]interface{}, error) {
	if len(entities) == 0 || err == nil {
		return entities, err
	}

	filtered := make([]interface{}, 0, len(entities))

	mErr, ok := err.(appengine.MultiError)
	if !ok {
		if _, ok := err.(*datastore.ErrFieldMismatch); !ok {
			return entities, err
		}
		log.Warningf(ctx, "err is ErrFieldMismatch, but ignore this: %v", err)
		return filtered, nil
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
