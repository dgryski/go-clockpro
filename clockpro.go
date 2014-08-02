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
	"container/list"
	"fmt"
)

type cacheEntry struct {
	ref bool
	val interface{}
}

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
}

// TODO(dgryski): combine data and metaKeys map
// TODO(dgryski): advance(elt)
// TODO(dgryski): container/list -> container/ring

type Cache struct {
	mem_max  int
	mem_cold int
	data     map[string]*cacheEntry
	meta     *list.List
	metaKeys map[string]*list.Element

	hand_pos_hot  *list.Element
	hand_pos_cold *list.Element
	hand_pos_test *list.Element

	hand_idx_hot  int
	hand_idx_cold int
	hand_idx_test int

	count_hot  int
	count_cold int
	count_test int
}

func New(size int) *Cache {
	return &Cache{
		mem_max:  size,
		mem_cold: size,
		data:     make(map[string]*cacheEntry),
		metaKeys: make(map[string]*list.Element),
		meta:     list.New(),
	}

}

func (c *Cache) Get(key string) interface{} {

	if v, ok := c.data[key]; ok && v != nil {
		v.ref = true
		return v.val
	}

	return nil
}

var DEBUG = false

func TRACE(what string) func() {
	if !DEBUG {
		return func() {}
	}
	fmt.Println("enter: ", what)
	return func() {
		fmt.Println("leave: ", what)
	}
}

func (c *Cache) Set(key string, value interface{}) {
	//	c.FullDump()
	defer TRACE("set")()

	if v, ok := c.data[key]; ok {
		if v == nil {
			if c.mem_cold < c.mem_max {
				c.mem_cold++
			}
			c.meta_del(key)
			c.data[key] = &cacheEntry{ref: false, val: value}
			c.count_test--
			c.meta_add(ptHot, key)
			c.count_hot++
		} else {
			v.val = value
			v.ref = true
		}
	} else {
		c.data[key] = &cacheEntry{ref: false, val: value}
		c.meta_add(ptCold, key)
		c.count_cold++
	}

	c.VerifyIdxs()
}

func (c *Cache) meta_add(ptype pageType, key string) {

	defer TRACE("meta_add")()

	c.evict()

	mentry := &metaEntry{
		ptype: ptype,
		key:   key,
	}

	if c.hand_pos_hot == nil {
		// first element
		elt := c.meta.PushFront(mentry)
		c.metaKeys[key] = elt
		c.hand_pos_hot = elt
		c.hand_pos_cold = elt
		c.hand_pos_test = elt
	} else {
		c.VerifyIdxs()
		c.metaKeys[key] = c.meta.InsertBefore(mentry, c.hand_pos_hot)

		if c.hand_idx_cold >= c.hand_idx_hot {
			c.hand_pos_cold = c.hand_pos_cold.Prev()
		}

		if c.hand_idx_test >= c.hand_idx_hot {
			c.hand_pos_test = c.hand_pos_test.Prev()
		}

		c.hand_pos_hot = c.hand_pos_hot.Prev()
	}

	if c.hand_idx_cold > c.hand_idx_hot {
		c.hand_idx_cold += 1
		c.hand_pos_cold = c.hand_pos_cold.Next()

		if c.hand_pos_cold == nil {
			c.hand_idx_cold = 0
			c.hand_pos_cold = c.meta.Front()
		}
	}

	c.VerifyIdxs()

	if c.hand_idx_test >= c.hand_idx_hot {
		c.hand_idx_test += 1
		c.hand_pos_test = c.hand_pos_test.Next()
		if c.hand_pos_test == nil {
			c.hand_idx_test = 0
			c.hand_pos_test = c.meta.Front()
		}
	}
	c.hand_idx_hot += 1
	c.hand_pos_hot = c.hand_pos_hot.Next()
	if c.hand_pos_hot == nil {
		c.hand_idx_hot = 0
		c.hand_pos_hot = c.meta.Front()
	}

	c.VerifyIdxs()
}

func (c *Cache) meta_del(key string) {

	defer TRACE("meta_del")()

	elt, ok := c.metaKeys[key]

	if !ok {
		panic("key " + key + " not present in remove!")
	}

	delete(c.metaKeys, key)

	c.VerifyIdxs()

	var idx int

	// FIXME(dgryski): get rid of this O(n) loop

	for e := c.meta.Front(); e != nil; e = e.Next() {
		if e == elt {
			break
		}
		idx++
	}

	if elt == c.hand_pos_hot {
		c.hand_pos_hot = c.hand_pos_hot.Prev()
		if c.hand_pos_hot == nil {
			c.hand_pos_hot = c.meta.Back()
		}
	}

	if elt == c.hand_pos_cold {
		c.hand_pos_cold = c.hand_pos_cold.Prev()
		if c.hand_pos_cold == nil {
			c.hand_pos_cold = c.meta.Back()
		}
	}

	if elt == c.hand_pos_test {
		c.hand_pos_test = c.hand_pos_test.Prev()
		if c.hand_pos_test == nil {
			c.hand_pos_test = c.meta.Back()
		}
	}

	c.meta.Remove(elt)

	max_pos := c.meta.Len() - 1

	if c.hand_idx_hot >= idx {
		c.hand_idx_hot--
		if c.hand_idx_hot < 0 {
			c.hand_idx_hot = max_pos
		}
	}

	if c.hand_idx_cold >= idx {
		c.hand_idx_cold--
		if c.hand_idx_cold < 0 {
			c.hand_idx_cold = max_pos
		}
	}

	if c.hand_idx_test >= idx {
		c.hand_idx_test--
		if c.hand_idx_test < 0 {
			c.hand_idx_test = max_pos
		}
	}

	c.VerifyIdxs()

}

func (c *Cache) evict() {

	defer TRACE("evict")()

	for c.mem_max <= c.count_hot+c.count_cold {
		c.hand_cold()
	}
}

func (c *Cache) hand_cold() {

	defer TRACE("hand_cold")()

	meta := c.hand_pos_cold.Value.(*metaEntry)

	if meta.ptype == ptCold {
		data := c.data[meta.key]

		if data.ref {
			meta.ptype = ptHot
			data.ref = false
			c.count_cold--
			c.count_hot++
		} else {
			meta.ptype = ptTest
			c.data[meta.key] = nil
			c.count_cold--
			c.count_test++
			for c.mem_max < c.count_test {
				c.hand_test()
			}
		}
	}

	c.hand_idx_cold++
	c.hand_pos_cold = c.hand_pos_cold.Next()
	if c.hand_pos_cold == nil {
		c.hand_pos_cold = c.meta.Front()
		c.hand_idx_cold = 0
	}

	for c.mem_max-c.mem_cold < c.count_hot {
		c.hand_hot()
	}
}

func (c *Cache) hand_hot() {

	defer TRACE("hand_hot")()

	if c.hand_pos_hot == c.hand_pos_test {
		c.hand_test()
	}

	meta := c.hand_pos_hot.Value.(*metaEntry)

	if meta.ptype == ptHot {
		data := c.data[meta.key]

		if data.ref {
			data.ref = false
		} else {
			meta.ptype = ptCold
			c.count_hot--
			c.count_cold++
		}
	}

	c.hand_idx_hot++
	c.hand_pos_hot = c.hand_pos_hot.Next()
	if c.hand_pos_hot == nil {
		c.hand_pos_hot = c.meta.Front()
		c.hand_idx_hot = 0
	}
}

func (c *Cache) hand_test() {

	defer TRACE("hand_test")()

	if c.hand_pos_test == c.hand_pos_cold {
		c.hand_cold()
	}

	meta := c.hand_pos_test.Value.(*metaEntry)

	if meta.ptype == ptTest {

		delete(c.data, meta.key)

		prev := c.hand_pos_test.Prev()
		pidx := c.hand_idx_test - 1
		if prev == nil {
			prev = c.meta.Back()
			pidx = c.meta.Len()
		}
		c.meta_del(meta.key)
		c.hand_pos_test = prev
		c.hand_idx_test = pidx

		c.count_test--
		if c.mem_cold > 1 {
			c.mem_cold--
		}
	}

	c.hand_idx_test++
	c.hand_pos_test = c.hand_pos_test.Next()
	if c.hand_pos_test == nil {
		c.hand_pos_test = c.meta.Front()
		c.hand_idx_test = 0
	}
}

func (c *Cache) Dump() {

	if !DEBUG {
		return

	}

	var b []byte

	for elt := c.meta.Front(); elt != nil; elt = elt.Next() {
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
			if c.data[m.key].ref {
				b = append(b, 'H')
			} else {

				b = append(b, 'h')
			}
		case ptCold:
			if v, ok := c.data[m.key]; ok && v != nil {
				if v.ref {
					b = append(b, 'C')
				} else {
					b = append(b, 'c')
				}
			} else {
				panic("no cached key for cold data " + m.key)
			}
		case ptTest:
			b = append(b, 'n')

		}
	}

	fmt.Println(string(b))
}

func (c *Cache) FullDump() {
	/*

		var keys []string
		for k := range c.data {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		fmt.Println("-data-")
		for _, k := range keys {
			if c.data[k] == nil {
				fmt.Println("k=", k, "v=", c.data[k])
			} else {
				fmt.Printf("k=%s v=%+v\n", k, *(c.data[k]))
			}
		}

	*/

	fmt.Println("-list-")
	var idx int
	for elt := c.meta.Front(); elt != nil; elt = elt.Next() {
		m := elt.Value.(*metaEntry)

		if elt == c.hand_pos_hot {
			fmt.Println("HOT pos")
		}
		if elt == c.hand_pos_cold {
			fmt.Println("COLD pos")
		}
		if elt == c.hand_pos_test {
			fmt.Println("TEST pos")
		}

		if idx == c.hand_idx_hot {
			fmt.Println("HOT idx")
		}
		if idx == c.hand_idx_cold {
			fmt.Println("COLD idx")
		}
		if idx == c.hand_idx_test {
			fmt.Println("TEST idx")
		}

		fmt.Printf("%+v\n", m)
		idx++
	}
}

func (c *Cache) VerifyIdxs() {

	if !DEBUG {
		return
	}

	if c.meta.Len() == 0 {
		return
	}

	hotidx := -1
	coldidx := -1
	testidx := -1

	idx := 0
	for e := c.meta.Front(); e != nil; e = e.Next() {
		if e == c.hand_pos_hot {
			hotidx = idx
		}
		if e == c.hand_pos_cold {
			coldidx = idx
		}
		if e == c.hand_pos_test {
			testidx = idx
		}
		idx++
	}

	if hotidx != c.hand_idx_hot || coldidx != c.hand_idx_cold || testidx != c.hand_idx_test {
		fmt.Println(c.meta.Len(), hotidx, c.hand_idx_hot, coldidx, c.hand_idx_cold, testidx, c.hand_idx_test)
		panic("index mismatch")
	}
}
