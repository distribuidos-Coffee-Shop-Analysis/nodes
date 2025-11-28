package common

import (
	"sync"
)

// Default number of workers for parallel processing
const DEFAULT_PARALLEL_WORKERS = 1000

// processInParallel distributes work across multiple workers and collects results
// T is the type of the result each worker produces
// - items: slice of data to process
// - numWorkers: number of concurrent workers (0 uses DEFAULT_PARALLEL_WORKERS)
// - processFunc: function that processes a single item and returns a result
// Returns a slice of all results from workers
func processInParallel[T any](
	items [][]byte,
	numWorkers int,
	processFunc func([]byte) T,
) []T {
	numItems := len(items)
	if numItems == 0 {
		return nil
	}

	if numWorkers <= 0 {
		numWorkers = DEFAULT_PARALLEL_WORKERS
	}
	if numItems < numWorkers {
		numWorkers = numItems
	}

	// Channels for work distribution and result collection
	workCh := make(chan []byte, numItems)
	resultsCh := make(chan T, numWorkers)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for data := range workCh {
				result := processFunc(data)
				resultsCh <- result
			}
		}()
	}

	// Send work to workers
	for _, item := range items {
		workCh <- item
	}
	close(workCh)

	// Wait for workers and close results channel
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Collect all results
	var results []T
	for result := range resultsCh {
		results = append(results, result)
	}

	return results
}

// ProcessAndMerge is a convenience wrapper that processes items and merges results
// - items: slice of data to process
// - numWorkers: number of concurrent workers (0 uses DEFAULT_PARALLEL_WORKERS)
// - processFunc: function that processes a single item and returns a result
// - mergeFunc: function that merges all results (called once with all worker results)
func ProcessAndMerge[T any](
	items [][]byte,
	numWorkers int,
	processFunc func([]byte) T,
	mergeFunc func([]T),
) {
	results := processInParallel(items, numWorkers, processFunc)
	if len(results) > 0 {
		mergeFunc(results)
	}
}
