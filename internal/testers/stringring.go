package testers

type stringRing struct {
	items   []string
	pointer int
}

func newStringRing(cap int) *stringRing {
	return &stringRing{
		items: make([]string, cap),
	}
}

func (sr *stringRing) Contains(item string) bool {
	for _, i := range sr.items {
		if i == item {
			return true
		}
	}
	return false
}

func (sr *stringRing) Add(item string) {
	sr.items[sr.pointer] = item
	sr.pointer++
	if sr.pointer >= len(sr.items) {
		sr.pointer = 0
	}
}
