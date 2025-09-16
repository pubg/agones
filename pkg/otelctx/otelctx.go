package otelctx

import (
	"context"

	routine "github.com/timandy/routine"
)

// HACK: 몇몇 라이브러리에서 context 제대로 못받을 경우 대충 우회해서 넘길려고 씀, js 에서 context API 비슷한 거라고 생각하면 됨
// 데이터 추출 용도로만 써야 하고 기존의 context 를 유지해야 함
var current = routine.NewInheritableThreadLocal[context.Context]()

func WithContext(ctx context.Context, fn func() error) error {
	old := current.Get()
	defer current.Set(old)

	current.Set(ctx)
	return fn()
}

func WithContext2[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	old := current.Get()
	defer current.Set(old)

	current.Set(ctx)
	return fn()
}

func GetContext() (context.Context, bool) {
	ctx := current.Get()
	if ctx == nil {
		return context.Background(), false
	}

	return ctx, true
}
