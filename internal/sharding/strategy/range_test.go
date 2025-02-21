package strategy

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"testing"
)

func TestRange_Sharding(t *testing.T) {
	r := NewRange("test_db", "test_tab_%d", 1)
	var eg errgroup.Group
	eg.Go(func() error {
		_, err := r.Sharding([]string{"1"})
		return err
	})
	eg.Go(func() error {
		_, err := r.Sharding([]string{"2"})
		return err
	})
	eg.Go(func() error {
		_, err := r.Sharding([]string{"3"})
		return err
	})
	err := eg.Wait()
	require.NoError(t, err)
	assert.Equal(t, 3, r.maxSeq.Load())
}
