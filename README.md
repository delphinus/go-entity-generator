# go-entity-generator

[![GoDoc](https://godoc.org/github.com/delphinus/go-entity-generator?status.svg)](https://godoc.org/github.com/delphinus/go-entity-generator)
[![CircleCI](https://circleci.com/gh/delphinus/go-entity-generator.svg?style=svg)](https://circleci.com/gh/delphinus/go-entity-generator)
[![Coverage Status](https://coveralls.io/repos/github/delphinus/go-entity-generator/badge.svg?branch=master)](https://coveralls.io/github/delphinus/go-entity-generator?branch=master)

Generator to yield all entities from Datastore.

docs in [godoc](https://godoc.org/github.com/delphinus/go-entity-generator).

## example

```go
type SomeItem struct {
  ID   int64  `datastore:"-" goon:"id"`
  Name string `datastore:",noindex"`
}

func appender(ctx context.Context, entities []interface{}, i int, k *datastore.Key, parentKey *datastore.Key) []interface{} {
  if k.IntID() == 0 {
    log.Warningf(ctx, "SomeItem{} needs int64 key. But items[%d] has a string key: %v", i, k.StringID())
  } else {
    entities = append(entities, &SomeItem{ID: k.IntID()})
  }
}

func someFunc(w http.ResponseWriter, r *http.Request) {
  ctx := appengine.NewContext(r)
  ctx, cancel := context.WithCancel(ctx)
  defer cancel() // you should cancel before finishing.

  ch := generator.New(ctx, &generator.Options{
    Appender: appender,
    Query:    datastore.NewQuery("SomeItem"),
  })

  for unit := range ch {
    if unit.Err != nil {
      panic(err)
    }

    for _, e := range unit.Entities {
      if s, ok := e.(*SomeItem); ok {
        // some nice handling
      }
    }
  }
}
```
