#go-worker
A minimalistic workerQueue for Go.

##workerQueue
A simple queue in which every job in the queue will be executed
by an amount of workers. The count of the workers can be
preconfigured.
```
func main() {
    workerQueue := worker.NewWorkerQueue(2)
    workerQueue.Do(func() {
        println("executed 1")
    })
    workerQueue.Do(func() {
        println("executed 2")
    })
}

// executed 1
// executed 2
```

##distinctWorkerQueue

