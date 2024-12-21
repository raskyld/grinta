package gorounette

type gopts struct {
	name string
}

type GOptions func(*gopts)

func WithName(name string) GOptions {
	return func(o *gopts) {
		o.name = name
	}
}
