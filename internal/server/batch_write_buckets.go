package server

import (
	"sync"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func batchWriteBuckets(td types.TableDescriptor, buckets map[*pebble.DB][]pebble.BatchOp) error {
	switch len(buckets) {
	case 0:
		return nil
	case 1:
		for db, group := range buckets {
			return db.BatchWriteItem(td, group)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(buckets))
	for db, group := range buckets {
		db, group := db, group
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := db.BatchWriteItem(td, group); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}
