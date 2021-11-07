// +build !enterprise

package index

func (o *Optimizer) optmizeHook(sql string) (string, error) {
	return "", nil
}
