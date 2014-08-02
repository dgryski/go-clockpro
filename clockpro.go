// Package clockpro implements the CLOCK-Pro caching algorithm.
/*

It is based on the python implementation from https://bitbucket.org/SamiLehtinen/pyclockpro .

Slides describing the algorithm: http://fr.slideshare.net/huliang64/clockpro

The original paper: http://static.usenix.org/event/usenix05/tech/general/full_papers/jiang/jiang_html/html.html

This version  uses a linked-list instead of an array to make the inserts and
deletes O(1) instead of O(n) as in the original python implementation.

It is MIT licensed, like the original.
*/
package clockpro

import (
	"container/ring"
	"fmt"
)

type pageType int

const (
	ptTest pageType = iota
	ptCold
	ptHot
)

func (p pageType) String() string {

	switch p {
	case ptTest:
		return "Test"
	case ptCold:
		return "Cold"
	case ptHot:
		return "Hot"
	}

	return "unknown"
}

type metaEntry struct {
	ptype pageType
	key   string
	val   interface{}
	ref   bool
}

type Cache struct {
	mem_max  int
	mem_cold int
	meta     *ring.Ring
	metaKeys map[string]*ring.Ring

	hand_pos_hot  *ring.Ring
	hand_pos_cold *ring.Ring
	hand_pos_test *ring.Ring

	count_hot  int
	count_cold int
	count_test int
}

func New(size int) *Cache {
	return &Cache{
		mem_max:  size,
		mem_cold: size,
		metaKeys: make(map[string]*ring.Ring),
	}

}

func (c *Cache) Get(key string) interface{} {

	v := c.metaKeys[key]

	if v == nil {
		return nil
	}

	val := v.Value.(*metaEntry)

	if val.val == nil {
		return nil
	}

	val.ref = true
	return val.val
}

func (c *Cache) Set(key string, value interface{}) {
	v := c.metaKeys[key]

	if v != nil {

		val := v.Value.(*metaEntry)

		if val.val == nil {
			if c.mem_cold < c.mem_max {
				c.mem_cold++
			}
			val.ref = false
			val.val = value
			val.ptype = ptHot
			c.count_test--
			c.meta_del(val.key)
			c.meta_add(val)
			c.count_hot++
		} else {
			val.val = value
			val.ref = true
		}
	} else {
		e := &metaEntry{ref: false, val: value, ptype: ptCold, key: key}
		c.meta_add(e)
		c.count_cold++
	}
}

func (c *Cache) meta_add(mentry *metaEntry) {

	c.evict()

	if c.meta == nil {
		// first element
		elt := &ring.Ring{Value: mentry}
		c.meta = elt
		c.metaKeys[mentry.key] = elt
		c.hand_pos_hot = elt
		c.hand_pos_cold = elt
		c.hand_pos_test = elt
	} else {
		elt := &ring.Ring{Value: mentry}
		c.metaKeys[mentry.key] = elt
		elt.Link(c.hand_pos_hot)

		if c.hand_pos_cold == c.hand_pos_hot {
			c.hand_pos_cold = c.hand_pos_cold.Prev()
		}
	}
}

func (c *Cache) meta_del(key string) {

	elt, ok := c.metaKeys[key]

	if !ok {
		panic("key " + key + " not present in remove!")
	}

	delete(c.metaKeys, key)

	if elt == c.hand_pos_hot {
		c.hand_pos_hot = c.hand_pos_hot.Prev()
	}

	if elt == c.hand_pos_cold {
		c.hand_pos_cold = c.hand_pos_cold.Prev()
	}

	if elt == c.hand_pos_test {
		c.hand_pos_test = c.hand_pos_test.Prev()
	}

	elt.Prev().Unlink(1)
}

func (c *Cache) evict() {

	for c.mem_max <= c.count_hot+c.count_cold {
		c.hand_cold()
	}
}

func (c *Cache) hand_cold() {

	meta := c.hand_pos_cold.Value.(*metaEntry)

	if meta.ptype == ptCold {

		if meta.ref {
			meta.ptype = ptHot
			meta.ref = false
			c.count_cold--
			c.count_hot++
		} else {
			meta.ptype = ptTest
			meta.val = nil
			c.count_cold--
			c.count_test++
			for c.mem_max < c.count_test {
				c.hand_test()
			}
		}
	}

	c.hand_pos_cold = c.hand_pos_cold.Next()

	for c.mem_max-c.mem_cold < c.count_hot {
		c.hand_hot()
	}
}

func (c *Cache) hand_hot() {

	if c.hand_pos_hot == c.hand_pos_test {
		c.hand_test()
	}

	meta := c.hand_pos_hot.Value.(*metaEntry)

	if meta.ptype == ptHot {

		if meta.ref {
			meta.ref = false
		} else {
			meta.ptype = ptCold
			c.count_hot--
			c.count_cold++
		}
	}

	c.hand_pos_hot = c.hand_pos_hot.Next()
}

func (c *Cache) hand_test() {

	if c.hand_pos_test == c.hand_pos_cold {
		c.hand_cold()
	}

	meta := c.hand_pos_test.Value.(*metaEntry)

	if meta.ptype == ptTest {

		prev := c.hand_pos_test.Prev()
		c.meta_del(meta.key)
		c.hand_pos_test = prev

		c.count_test--
		if c.mem_cold > 1 {
			c.mem_cold--
		}
	}

	c.hand_pos_test = c.hand_pos_test.Next()
}

func (c *Cache) dump() {

	var b []byte

	var end *ring.Ring = nil
	for elt := c.meta; elt != end; elt = elt.Next() {
		end = c.meta
		m := elt.Value.(*metaEntry)

		if c.hand_pos_hot == elt {
			b = append(b, '2')
		}

		if c.hand_pos_test == elt {
			b = append(b, '0')
		}

		if c.hand_pos_cold == elt {
			b = append(b, '1')
		}

		switch m.ptype {
		case ptHot:
			if m.ref {
				b = append(b, 'H')
			} else {

				b = append(b, 'h')
			}
		case ptCold:
			if m.ref {
				b = append(b, 'C')
			} else {
				b = append(b, 'c')
			}
		case ptTest:
			b = append(b, 'n')

		}
	}

	fmt.Println(string(b))
}
