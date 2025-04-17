package ristretto

import "time"

// Item is a full representation of what's stored in the cache for each key-value pair.
type Item[V any] struct {
	flag       itemFlag
	Key        uint64
	Conflict   uint64
	Value      V
	Cost       int64
	Expiration time.Time
	wait       chan struct{}
	onSet      func(bool)
}

func (c *Cache[K, V]) SetWithTTLAndCallback(key K, value V, cost int64, ttl time.Duration, cb func(success bool)) {
	if c == nil || c.isClosed.Load() {
		cb(false)
		return
	}

	var expiration time.Time
	switch {
	case ttl == 0:
		// No expiration.
		break
	case ttl < 0:
		// Treat this a no-op.
		cb(false)
		return
	default:
		expiration = time.Now().Add(ttl)
	}

	keyHash, conflictHash := c.keyToHash(key)
	i := &Item[V]{
		flag:       itemNew,
		Key:        keyHash,
		Conflict:   conflictHash,
		Value:      value,
		Cost:       cost,
		Expiration: expiration,
		onSet: func(ok bool) {
			if cb != nil {
				cb(ok)
			}
		},
	}
	// cost is eventually updated. The expiration must also be immediately updated
	// to prevent items from being prematurely removed from the map.
	if prev, ok := c.storedItems.Update(i); ok {
		c.onExit(prev)
		i.flag = itemUpdate
	}
	// Attempt to send item to cachePolicy.
	select {
	case c.setBuf <- i:
		// sucesso: callback será chamado no processItems
		return
	default:
		// falha: buffer cheio
		if i.flag == itemUpdate {
			// Mesmo se for update, a política não garantiu admissão.
			return
		}
		c.Metrics.add(dropSets, keyHash, 1)
		cb(false)
	}
}

// processItems is ran by goroutines processing the Set buffer.
func (c *Cache[K, V]) processItems() {
	startTs := make(map[uint64]time.Time)
	numToKeep := 100000 // TODO: Make this configurable via options.

	trackAdmission := func(key uint64) {
		if c.Metrics == nil {
			return
		}
		startTs[key] = time.Now()
		if len(startTs) > numToKeep {
			for k := range startTs {
				if len(startTs) <= numToKeep {
					break
				}
				delete(startTs, k)
			}
		}
	}
	onEvict := func(i *Item[V]) {
		if ts, has := startTs[i.Key]; has {
			c.Metrics.trackEviction(int64(time.Since(ts) / time.Second))
			delete(startTs, i.Key)
		}
		if c.onEvict != nil {
			c.onEvict(i)
		}
	}

	for {
		select {
		case i := <-c.setBuf:
			if i.wait != nil {
				close(i.wait)
				continue
			}
			// Calculate item cost value if new or update.
			if i.Cost == 0 && c.cost != nil && i.flag != itemDelete {
				i.Cost = c.cost(i.Value)
			}
			if !c.ignoreInternalCost {
				// Add the cost of internally storing the object.
				i.Cost += itemSize
			}

			switch i.flag {
			case itemNew:
				victims, added := c.cachePolicy.Add(i.Key, i.Cost)
				if added {
					c.storedItems.Set(i)
					c.Metrics.add(keyAdd, i.Key, 1)
					trackAdmission(i.Key)
					if i.onSet != nil {
						i.onSet(true)
					}
				} else {
					if i.onSet != nil {
						i.onSet(false) // mesmo que seja para marcar o fim
					}
					c.onReject(i)
				}
				for _, victim := range victims {
					victim.Conflict, victim.Value = c.storedItems.Del(victim.Key, 0)
					onEvict(victim)
				}

			case itemUpdate:
				c.cachePolicy.Update(i.Key, i.Cost)
				if i.onSet != nil {
					i.onSet(true)
				}

			case itemDelete:
				c.cachePolicy.Del(i.Key) // Deals with metrics updates.
				_, val := c.storedItems.Del(i.Key, i.Conflict)
				c.onExit(val)
			}
		case <-c.cleanupTicker.C:
			c.storedItems.Cleanup(c.cachePolicy, onEvict)
		case <-c.stop:
			c.done <- struct{}{}
			return
		}
	}
}
