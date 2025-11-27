package middleware

import (
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

// WorkerPool manages a pool of workers that consume from a queue
type WorkerPool struct {
	workers    []*Worker
	numWorkers int
	queueName  string
	connection *amqp.Connection
	callback   func(batch *protocol.BatchMessage, delivery amqp.Delivery)
	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

// NewWorkerPool creates a new worker pool for a specific queue
func NewWorkerPool(
	queueName string,
	connection *amqp.Connection,
	callback func(batch *protocol.BatchMessage, delivery amqp.Delivery),
	numWorkers int,
) *WorkerPool {
	return &WorkerPool{
		workers:    make([]*Worker, 0, numWorkers),
		numWorkers: numWorkers,
		queueName:  queueName,
		connection: connection,
		callback:   callback,
		shutdownCh: make(chan struct{}),
	}
}

// Start initializes and starts all workers in the pool
func (wp *WorkerPool) Start() error {
	for i := 0; i < wp.numWorkers; i++ {
		worker, err := NewWorker(i, wp.queueName, wp.connection, wp.callback, wp.shutdownCh)
		if err != nil {
			log.Printf("action: worker_pool_create_worker | worker_id: %d | queue: %s | result: fail | error: %v",
				i, wp.queueName, err)
			wp.Stop()
			return err
		}

		wp.workers = append(wp.workers, worker)
		wp.wg.Add(1)

		go func(w *Worker) {
			defer wp.wg.Done()
			w.Start()
		}(worker)
	}

	return nil
}

// Stop gracefully shuts down all workers in the pool
func (wp *WorkerPool) Stop() {
	log.Printf("action: worker_pool_stop | queue: %s | num_workers: %d", wp.queueName, len(wp.workers))

	// Signal all workers to stop
	close(wp.shutdownCh)

	// Wait for all workers to finish
	wp.wg.Wait()

	log.Printf("action: worker_pool_stop | queue: %s | result: success", wp.queueName)
}

// Wait blocks until all workers have finished
func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}

type WorkerPoolManager struct {
	pools []*WorkerPool
	mu    sync.Mutex
}

// NewWorkerPoolManager creates a new manager
// Useful for joiners with multiple queues
func NewWorkerPoolManager() *WorkerPoolManager {
	return &WorkerPoolManager{
		pools: make([]*WorkerPool, 0),
	}
}

// AddPool adds a worker pool to the manager
func (wpm *WorkerPoolManager) AddPool(pool *WorkerPool) {
	wpm.mu.Lock()
	defer wpm.mu.Unlock()
	wpm.pools = append(wpm.pools, pool)
}

// StartAll starts all worker pools
func (wpm *WorkerPoolManager) StartAll() error {
	wpm.mu.Lock()
	defer wpm.mu.Unlock()

	for _, pool := range wpm.pools {
		if err := pool.Start(); err != nil {
			wpm.stopAllLocked()
			return err
		}
	}
	return nil
}

// StopAll gracefully stops all worker pools
func (wpm *WorkerPoolManager) StopAll() {
	wpm.mu.Lock()
	defer wpm.mu.Unlock()
	wpm.stopAllLocked()
}

// stopAllLocked stops all pools (must be called with lock held)
func (wpm *WorkerPoolManager) stopAllLocked() {
	for _, pool := range wpm.pools {
		pool.Stop()
	}
}

// WaitAll blocks until all worker pools have finished
func (wpm *WorkerPoolManager) WaitAll() {
	wpm.mu.Lock()
	pools := wpm.pools
	wpm.mu.Unlock()

	for _, pool := range pools {
		pool.Wait()
	}
}
