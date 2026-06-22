package main

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type queue struct {
	messages []string
	waiters  []chan string
}

var (
	mu     sync.Mutex
	queues = map[string]*queue{}
)

func getOrCreate(name string) *queue {
	q, ok := queues[name]
	if !ok {
		q = &queue{}
		queues[name] = q
	}
	return q
}

func handlePut(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	v := r.URL.Query().Get("v")
	if v == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	mu.Lock()
	q := getOrCreate(name)
	if len(q.waiters) > 0 {
		ch := q.waiters[0]
		q.waiters = q.waiters[1:]
		mu.Unlock()
		ch <- v
	} else {
		q.messages = append(q.messages, v)
		mu.Unlock()
	}
	w.WriteHeader(http.StatusOK)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	timeout := 0
	if s := r.URL.Query().Get("timeout"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			timeout = n
		}
	}

	mu.Lock()
	q := getOrCreate(name)
	if len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages = q.messages[1:]
		mu.Unlock()
		w.Write([]byte(msg))
		return
	}
	if timeout == 0 {
		mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		return
	}

	ch := make(chan string, 1)
	q.waiters = append(q.waiters, ch)
	mu.Unlock()

	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(timeout)*time.Second)
	defer cancel()

	select {
	case msg := <-ch:
		w.Write([]byte(msg))
	case <-ctx.Done():
		mu.Lock()
		for i, wch := range q.waiters {
			if wch == ch {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				break
			}
		}
		mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
	}
}

func main() {
	if len(os.Args) < 2 {
		os.Exit(1)
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			handlePut(w, r)
		case http.MethodGet:
			handleGet(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	http.ListenAndServe(":"+os.Args[1], nil)
}
