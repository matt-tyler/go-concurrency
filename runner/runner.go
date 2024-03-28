package runner

import (
	"context"
	"sync"
	"golang.org/x/sync/errgroup"
)


type Processor[T, U any] interface {
	Start(ctx context.Context, t T) <-chan U
}

type ProcessorFunc[T, U any] func(ctx context.Context, t T) <-chan U

func (f ProcessorFunc[T, U]) Start(ctx context.Context, t T) <-chan U {
	return f(ctx, t)
}

type Runner struct {
	ctx context.Context
	group *errgroup.Group
}

func WithContext(ctx context.Context) *Runner {
	group, ctx := errgroup.WithContext(ctx)
	return &Runner{ctx: ctx, group: group}
}

func (r *Runner) Go(fn func() error) {
	r.group.Go(fn)
}

func (r *Runner) Wait() error {
	return r.group.Wait()
}

func (r *Runner) Context() context.Context {
	return r.ctx
}

// ParallelMap returns a channel that will receive the results of the function
// fn applied to the values in the input channel. It takes a parameter n that
// specifies the number of goroutines to use.
func ParallelMap[T, U any](r *Runner, ch <-chan T, n int, fn func(t T) (U, error)) <-chan U {
	chs := FanOut(r, ch, n, fn)
	out := FanIn(r, chs)
	return out
}

// ParallelChain return a channel that will receive the results of the function
// fn applied to the values in the input channel. It takes a parameter n that
// specifies the number of goroutines to use. ParallelChain differs from ParallelMap
// in that the function fn returns a channel of values rather than a single value.
func ParallelChain[T, U any](r *Runner, ch <-chan T, n int, processor Processor[T, U]) <-chan U {
	fanOut := FanOut(r, ch, n, Identity)
	us := make([]<-chan U, n)

	for i := range us {
		u := Chain(r, fanOut[i], processor)
		us[i] = u
	}

	return FanIn(r, us)
}

// Identity returns the input value. This function will never return an error
func Identity[T any](t T) (T, error) {
	return t, nil
}

// Chain returns a channel that will receive the results of the function
// fn applied to the values in the input channel. The function fn returns a channel
// of values rather than a single value.
func Chain[T, U any](r *Runner, ch <-chan T, processor Processor[T, U]) <-chan U  {
	out := make(chan U)
	ctx := r.Context()
	r.Go(func() error {
		defer close(out)
		for v := range OrDone(ctx, ch) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			u := processor.Start(ctx, v)
			for v := range OrDone(ctx, u) {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				out <- v
			}
		}
		return nil
	})
	return out
}

// FanOut returns n channels that will receive the results of the function
// fn applied to the values in the input channel.
func FanOut[T any, U any](r *Runner, ch <-chan T, n int, fn func(t T) (U, error)) []<-chan U {
	cs := make([]chan U, n)
	result := make([]<-chan U, n)

	ctx := r.Context()
	for i := range cs {
		cs[i] = make(chan U)
		result[i] = cs[i]

		r.Go(func() error {
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
	return result
}

func FanIn[T any](r *Runner, cs []<-chan T) <-chan T {
	out := make(chan T)

	wg := sync.WaitGroup{}
	wg.Add(len(cs))

	r.Go(func() error {
		defer close(out)
		wg.Wait()
		return nil
	})

	ctx := r.Context()
	for i := range cs {
		r.Go(func() error {
			defer wg.Done()
			for v := range OrDone(ctx, cs[i]) {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				out <- v
			}
			return nil
		})
	}
	return out
}

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
