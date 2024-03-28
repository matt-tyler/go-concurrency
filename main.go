package main

import (
	"context"
	"time"
	"encoding/json"
	"io"
	"os"

	"golang.org/x/sync/errgroup"

	"example.com/m/runner"
)

func makeChan[T any]() chan T {
	c := make(chan T, 10)
	return c
}

type fn func() error

// orDone returns a channel that will be closed when either the input channel is closed or the context is done
func OrDone[T any](ctx context.Context, ch <-chan T) <-chan T {
	done := ctx.Done()
	out := make(chan T)
	go func() {
		defer close(out)
		for {
			select {
			case <-done:
				return
			case v, ok := <-ch:
				if !ok {
					return
				}
				select {
				case out <- v:
				case <-done:
				}
			}
		}
	}()
	return out
}

// return a receiver channel and a function that starts a goroutine to write to the channel

// func source[T any](ctx context.Context) (chan<- T, fn) {
//
// }
//
func m[T, U any](ctx context.Context, ch <-chan T, fn func(t T) (U, error)) (<-chan U, fn) {
	out := make(chan U)
	return out, func() error {
		defer close(out)
		for v := range OrDone(ctx, ch) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			u, err := fn(v)
			if err != nil {
				return err
			}
			out <- u
		}
		return nil
	}
}

// ParallelMap returns a channel that will receive the results of the function
// fn applied to the values in the input channel. It takes a parameter n that
// specifies the number of goroutines to use.
func ParallelMap[T, U any](ctx context.Context, ch <-chan T, n int, fn func(t T) (U, error)) (<-chan U, func() error) {
	group, ctx := errgroup.WithContext(ctx)
	chs, f1 := FanOut(ctx, ch, n, fn)
	out, f2 := FanIn(ctx, chs)
	group.Go(f1)
	group.Go(f2)
	return out, group.Wait
}

// Identity returns the input value. This function will never return an error
func Identity[T any](t T) (T, error) {
	return t, nil
}

// ParallelChain return a channel that will receive the results of the function
// fn applied to the values in the input channel. It takes a parameter n that
// specifies the number of goroutines to use. ParallelChain differs from ParallelMap
// in that the function fn returns a channel of values rather than a single value.
func ParallelChain[T, U any](ctx context.Context, ch <-chan T, n int, fn func(ctx context.Context, t T) <-chan U) (<-chan U, func() error) {
	group, ctx := errgroup.WithContext(ctx)

	fanOut, f1 := FanOut(ctx, ch, n, Identity)
	group.Go(f1)

	us := make([]<-chan U, n)

	for i := range us {
		u, fn := Chain(ctx, fanOut[i], fn)
		group.Go(fn)
		us[i] = u
	}

	return FanIn(ctx, us)
}

// Chain returns a channel that will receive the results of the function
// fn applied to the values in the input channel. The function fn returns a channel
// of values rather than a single value.
func Chain[T, U any](ctx context.Context, ch <-chan T, fn func(ctx context.Context, t T) <-chan U) (<-chan U, func() error) {
	out := make(chan U)

	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		defer close(out)
		for v := range OrDone(ctx, ch) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			u := fn(ctx, v)
			for v := range OrDone(ctx, u) {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				out <- v
			}
		}
		return nil
	})
	return out, group.Wait
}

// FanOut returns n channels that will receive the results of the function
// fn applied to the values in the input channel.
func FanOut[T any, U any](ctx context.Context, ch <-chan T, n int, fn func(t T) (U, error)) ([]<-chan U, func() error) {
	cs := make([]chan U, n)
	result := make([]<-chan U, n)
	group, ctx := errgroup.WithContext(ctx)
	for i := range cs {
		cs[i] = make(chan U)
		result[i] = cs[i]

		group.Go(func() error {
			defer close(cs[i])
			for v := range OrDone(ctx, ch) {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				u, err := fn(v)
				if err != nil {
					return err
				}
				cs[i] <- u
			}
			return nil
		})
	}
	return result, group.Wait
}

func FanIn[T any](ctx context.Context, cs []<-chan T) (<-chan T, func() error) {
	out := make(chan T)

	group, ctx := errgroup.WithContext(ctx)
	fn := func() error {
		defer close(out)
		return group.Wait()
	}

	for i := range cs {
		group.Go(func() error {
			for v := range OrDone(ctx, cs[i]) {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				out <- v
			}
			return nil
		})
	}

	return out, fn
}


// convert to json and write to w
func sink[T any](ctx context.Context, ch <-chan T, w io.Writer) fn {
	return func() error {
		encoder := json.NewEncoder(w)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-ch:
				if !ok {
					return nil
				}
				err := encoder.Encode(v)
				if err != nil {
					return err
				}
			}
		}
	}
}


type x struct {
	ctx context.Context
	ch <-chan any
}

func (x *x) WriterTo(w io.Writer) (int64, error) {
	for {
		select {
		case <-x.ctx.Done():
			return 0, x.ctx.Err()
		case v, ok := <-x.ch:
			if !ok {
				return 0, io.EOF
			}
			bs, err := json.Marshal(v)
			if err != nil {
				return 0, err
			}
			w.Write(bs)
		}
	}
}

func moreNumbers(ctx context.Context, n int) <-chan int {
	out := make(chan int)

	go func() {
		defer close(out)
		x := n * 1000
		for i := 0; i < n; i++ {
			select {
			case <-ctx.Done():
			return
			case out <- x + i:
			}
		}
	}()
	return out
}

func main() {

	// group, ctx := errgroup.WithContext(context.Background())

	r := runner.WithContext(context.Background())

	ch := makeChan[int]()
	r.Go(func() error {
		defer close(ch)
		for i := range 5 {
			ch <- i
			time.Sleep(1 * time.Second)
		}
		return nil
	})

	// plusOne := func(i int) (int, error) {
	// 	return i + 10, nil
	// }

	// out, callback := ParallelMap(ctx, ch, 3, plusOne)
	out := runner.ParallelChain(r, ch, 3, runner.ProcessorFunc[int, int](moreNumbers))

	// group.Go(callback)
	r.Go(sink[int](r.Context(), out, os.Stdout))

	r.Wait()
}
