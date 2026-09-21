package mdb

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	bolt "go.etcd.io/bbolt"
)

const (
	scoreMin         uint64 = 0
	scoreMax                = ^uint64(0)
	uint64EncodedLen        = 8
)

var (
	bucketHash      = []byte("h")
	bucketZetMember = []byte("zm")
	bucketZetScore  = []byte("zc")

	ErrNilBucket       = errors.New("bucket is nil")
	ErrBufferTooShort  = errors.New("buffer too short")
	ErrNameTooLong     = errors.New("name length exceeds 255 bytes")
	ErrKeyTooLong      = errors.New("key length exceeds 255 bytes")
	ErrInvalidBuf      = errors.New("invalid buffer length")
	ErrKeyValuePairLen = errors.New("kvs len must be an even number")
	ErrKeyNotFound     = errors.New("key not found")

	keyBufPool = sync.Pool{
		New: func() interface{} {
			return new(make([]byte, 520))
		},
	}
)

type (
	DB struct {
		db *bolt.DB
		mu sync.RWMutex
	}

	DBStats struct {
		FilePath   string
		FileSize   int64
		DataSize   int64
		UsageRatio float64
		FragRatio  float64
	}
)

func Open(path string) (*DB, error) {
	return OpenWithMode(path, 0o600)
}

func OpenWithMode(path string, mode os.FileMode) (*DB, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("empty database path")
	}
	database, err := bolt.Open(path, mode, &bolt.Options{
		Timeout:      time.Second,
		FreelistType: bolt.FreelistMapType,
	})
	if err != nil {
		return nil, err
	}
	err = database.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketHash)
		_, err = tx.CreateBucketIfNotExists(bucketZetMember)
		_, err = tx.CreateBucketIfNotExists(bucketZetScore)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &DB{db: database}, nil
}

func (d *DB) View(fn func(*bolt.Tx) error) error {
	if d == nil || fn == nil {
		return errors.New("nil database or func")
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.db == nil {
		return errors.New("nil database")
	}
	return d.db.View(fn)
}

func (d *DB) Update(fn func(*bolt.Tx) error) error {
	if d == nil || fn == nil {
		return errors.New("nil database or func")
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.db == nil {
		return errors.New("nil database")
	}
	return d.db.Update(fn)
}

func (d *DB) Close() error {
	if d == nil {
		return errors.New("nil database")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.db == nil {
		return nil
	}
	err := d.db.Close()
	d.db = nil
	return err
}

// -------------------
// Hash 功能函数
// -------------------

func (d *DB) HSet(tx *bolt.Tx, name string, key, val []byte) error {
	b := tx.Bucket(bucketHash)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	reallyKey, err := encodeHashKeyToBuf(name, key, bufPtr)
	if err != nil {
		return err
	}
	return b.Put(reallyKey, val)
}

func (d *DB) HMSet(tx *bolt.Tx, name string, kvs ...[]byte) error {
	if len(kvs) == 0 || len(kvs)%2 != 0 {
		return ErrKeyValuePairLen
	}
	b := tx.Bucket(bucketHash)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	for i := 0; i < len(kvs)-1; i += 2 {
		reallyKey, err := encodeHashKeyToBuf(name, kvs[i], bufPtr)
		if err != nil {
			return err
		}
		if err = b.Put(reallyKey, kvs[i+1]); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) HIncr(tx *bolt.Tx, name string, key []byte, step int64) (uint64, error) {
	b := tx.Bucket(bucketHash)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	reallyKey, _ := encodeHashKeyToBuf(name, key, bufPtr)

	var current uint64
	v := b.Get(reallyKey)
	if len(v) == uint64EncodedLen {
		current = B2i(v)
	} else if len(v) > 0 {
		if parsedUint, err := parseUintBytes(v); err == nil {
			current = parsedUint
		}
	}
	newVal := uint64(int64(current) + step)
	return newVal, b.Put(reallyKey, I2b(newVal))
}

// HKeyExist 判断 Hash 中是否存在指定的 key
func (d *DB) HKeyExist(tx *bolt.Tx, name string, key []byte) bool {
	b := tx.Bucket(bucketHash)
	if b == nil {
		return false
	}

	// 从对象池获取 buffer，减少内存分配
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	// 编码真实的存储键
	reallyKey, err := encodeHashKeyToBuf(name, key, bufPtr)
	if err != nil {
		return false // 编码失败（如 name 或 key 超长）则视为不存在
	}

	// 在 bbolt 中，如果键不存在，Get() 将返回 nil
	return b.Get(reallyKey) != nil
}

func (d *DB) HGet(tx *bolt.Tx, name string, key []byte) []byte {
	b := tx.Bucket(bucketHash)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	reallyKey, _ := encodeHashKeyToBuf(name, key, bufPtr)
	return b.Get(reallyKey)
}

func (d *DB) HGetInt(tx *bolt.Tx, name string, key []byte) uint64 {
	b := tx.Bucket(bucketHash)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	reallyKey, _ := encodeHashKeyToBuf(name, key, bufPtr)
	v := b.Get(reallyKey)
	if v == nil {
		return 0 // 不存在返回 0
	}
	return B2i(v)
}

func (d *DB) HGetFunc(tx *bolt.Tx, name string, key []byte, fn func(val []byte) error) error {
	b := tx.Bucket(bucketHash)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	reallyKey, _ := encodeHashKeyToBuf(name, key, bufPtr)
	v := b.Get(reallyKey)
	if v == nil {
		return ErrKeyNotFound
	}
	return fn(v)
}

func (d *DB) HMGetFunc(tx *bolt.Tx, name string, keys [][]byte, fn func(key, val []byte) error) error {
	b := tx.Bucket(bucketHash)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	for _, key := range keys {
		reallyKey, _ := encodeHashKeyToBuf(name, key, bufPtr)
		if err := fn(key, b.Get(reallyKey)); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) HScanFunc(tx *bolt.Tx, name string, keyStart []byte, limit int, fn func(key, val []byte) bool) error {
	if limit <= 0 {
		return nil
	}
	b := tx.Bucket(bucketHash)
	bufPtr1 := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr1)

	reallyKeyStart, _ := encodeHashKeyToBuf(name, keyStart, bufPtr1)
	prefixLen := 1 + len(name)
	prefix := reallyKeyStart[:prefixLen] // Zero alloc prefix extraction

	c := b.Cursor()
	n := 0
	for k, v := c.Seek(reallyKeyStart); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if len(keyStart) > 0 && bytes.Compare(k, reallyKeyStart) <= 0 {
			continue
		}
		if !fn(k[prefixLen:], v) {
			break
		}
		n++
		if n == limit {
			break
		}
	}
	return nil
}
func (d *DB) HRScanFunc(tx *bolt.Tx, name string, keyStart []byte, limit int, fn func(key, val []byte) bool) error {
	if limit <= 0 {
		return nil
	}
	b := tx.Bucket(bucketHash)
	bufPtr1 := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr1) // 1st buffer for start key
	bufPtr2 := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr2) // 2nd buffer for upper bound

	var seekKey, prefix []byte
	prefixLen := 1 + len(name)
	isStartEmpty := len(keyStart) == 0

	if isStartEmpty {
		prefix, _ = encodeHashKeyToBuf(name, nil, bufPtr1)
	} else {
		seekKey, _ = encodeHashKeyToBuf(name, keyStart, bufPtr1)
		prefix = seekKey[:prefixLen]
	}

	upper := keyUpperBoundToBuf(prefix, bufPtr2)
	c := b.Cursor()
	var k, v []byte

	if isStartEmpty {
		if upper != nil {
			k, v = c.Seek(upper)
			if k == nil {
				k, v = c.Last()
			} else {
				k, v = c.Prev()
			}
		} else {
			k, v = c.Last()
		}
	} else {
		k, v = c.Seek(seekKey)
		if k == nil {
			k, v = c.Last()
		}
	}

	n := 0
	for k != nil {
		if bytes.Compare(k, prefix) < 0 {
			break
		}
		if bytes.HasPrefix(k, prefix) {
			if !isStartEmpty && bytes.Compare(k, seekKey) >= 0 {
				k, v = c.Prev()
				continue
			}
			if !fn(k[prefixLen:], v) {
				break
			}
			n++
			if n == limit {
				break
			}
		}
		k, v = c.Prev()
	}
	return nil
}
func (d *DB) HDel(tx *bolt.Tx, name string, key []byte) error {
	b := tx.Bucket(bucketHash)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	reallyKey, _ := encodeHashKeyToBuf(name, key, bufPtr)
	return b.Delete(reallyKey)
}

func (d *DB) HMDel(tx *bolt.Tx, name string, keys [][]byte) error {
	b := tx.Bucket(bucketHash)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	for _, key := range keys {
		reallyKey, _ := encodeHashKeyToBuf(name, key, bufPtr)
		if err := b.Delete(reallyKey); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) HDelBucket(tx *bolt.Tx, name string) error {
	b := tx.Bucket(bucketHash)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	prefix, _ := encodeHashKeyToBuf(name, nil, bufPtr)
	c := b.Cursor()
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Seek(prefix) {
		if err := c.Delete(); err != nil {
			return err
		}
	}
	return nil
}

// -------------------
// Zet 功能函数
// -------------------

func (d *DB) ZSet(tx *bolt.Tx, name string, key []byte, score uint64) error {
	b1 := tx.Bucket(bucketZetMember)
	b2 := tx.Bucket(bucketZetScore)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	reallyKey, _ := encodeHashKeyToBuf(name, key, bufPtr)
	scoreByte := I2b(score)
	oldScoreByte := b1.Get(reallyKey)
	if bytes.Equal(oldScoreByte, scoreByte) {
		return nil
	}

	if err := b1.Put(reallyKey, scoreByte); err != nil {
		return err
	}
	reallyScoreKey, _ := encodeZsetScoreKeyToBuf(name, key, score, bufPtr)
	if err := b2.Put(reallyScoreKey, []byte{}); err != nil {
		return err
	}

	if len(oldScoreByte) == uint64EncodedLen {
		reallyScoreKey, err := encodeZsetScoreKeyToBuf(name, key, B2i(oldScoreByte), bufPtr)
		if err == nil {
			_ = b2.Delete(reallyScoreKey)
		}
	}
	return nil
}

func (d *DB) ZSetF(tx *bolt.Tx, name string, key []byte, score float64) error {
	return d.ZSet(tx, name, key, Float64ToSortableUint64(score))
}

func (d *DB) ZMSet(tx *bolt.Tx, name string, kvs ...[]byte) error {
	for i := 0; i < len(kvs)-1; i += 2 {
		scoreBuf := kvs[i+1]
		var score uint64
		if len(scoreBuf) == uint64EncodedLen {
			score = B2i(scoreBuf)
		} else {
			score, _ = parseUintBytes(scoreBuf)
		}
		if err := d.ZSet(tx, name, kvs[i], score); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) ZIncr(tx *bolt.Tx, name string, key []byte, step int64) (uint64, error) {
	b1 := tx.Bucket(bucketZetMember)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	reallyKey, _ := encodeHashKeyToBuf(name, key, bufPtr)

	var current uint64
	v := b1.Get(reallyKey)
	if len(v) == uint64EncodedLen {
		current = B2i(v)
	} else if len(v) > 0 {
		current, _ = parseUintBytes(v)
	}
	newScore := uint64(int64(current) + step)
	return newScore, d.ZSet(tx, name, key, newScore)
}

func (d *DB) ZGetInt(tx *bolt.Tx, name string, key []byte) uint64 {
	b := tx.Bucket(bucketZetMember)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	reallyKey, _ := encodeHashKeyToBuf(name, key, bufPtr)
	v := b.Get(reallyKey)
	if v == nil {
		return 0
	}
	score := B2i(v)
	if len(v) != uint64EncodedLen {
		score, _ = parseUintBytes(v)
	}
	return score
}

func (d *DB) ZGetFunc(tx *bolt.Tx, name string, key []byte, fn func(score uint64) error) error {
	b := tx.Bucket(bucketZetMember)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	reallyKey, _ := encodeHashKeyToBuf(name, key, bufPtr)
	v := b.Get(reallyKey)
	if v == nil {
		return ErrKeyNotFound
	}
	score := B2i(v)
	if len(v) != uint64EncodedLen {
		score, _ = parseUintBytes(v)
	}
	return fn(score)
}

func (d *DB) ZMGetFunc(tx *bolt.Tx, name string, keys [][]byte, fn func(key []byte, score uint64, exists bool) error) error {
	b := tx.Bucket(bucketZetMember)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	for _, key := range keys {
		reallyKey, _ := encodeHashKeyToBuf(name, key, bufPtr)
		v := b.Get(reallyKey)
		if v == nil {
			if err := fn(key, 0, false); err != nil {
				return err
			}
			continue
		}
		var score uint64
		if len(v) == uint64EncodedLen {
			score = B2i(v)
		} else {
			score, _ = parseUintBytes(v)
		}
		if err := fn(key, score, true); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) ZGetFuncF(tx *bolt.Tx, name string, key []byte, fn func(score float64) error) error {
	return d.ZGetFunc(tx, name, key, func(score uint64) error {
		return fn(SortableUint64ToFloat64(score))
	})
}

func (d *DB) ZScanFunc(tx *bolt.Tx, name string, keyStart []byte, scoreStart, scoreEnd uint64, limit int, fn func(key []byte, score uint64) bool) error {
	if limit <= 0 {
		return nil
	}
	b := tx.Bucket(bucketZetScore)
	if scoreEnd == 0 || scoreEnd < scoreStart {
		scoreEnd = scoreMax
	}
	bufPtr1 := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr1)

	reallyKeyStart, _ := encodeZsetScoreKeyToBuf(name, keyStart, scoreStart, bufPtr1)
	prefixLen := 1 + len(name)
	prefix := reallyKeyStart[:prefixLen]

	c := b.Cursor()
	n := 0
	for k, _ := c.Seek(reallyKeyStart); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		_, key, score, err := DecodeZsetScoreKey(k)
		if err != nil || score > scoreEnd {
			if score > scoreEnd {
				break
			}
			continue
		}
		if len(keyStart) > 0 && bytes.Compare(k, reallyKeyStart) <= 0 {
			continue
		}
		if !fn(key, score) {
			break
		}
		n++
		if n == limit {
			break
		}
	}
	return nil
}

func (d *DB) ZScanFuncF(tx *bolt.Tx, name string, keyStart []byte, scoreStart, scoreEnd float64, limit int, fn func(key []byte, score float64) bool) error {
	// 【核心修复】识别 0, 0, nil 全表扫描占位符，直接透传给底层 uint64 引擎
	if scoreStart == 0 && scoreEnd == 0 && len(keyStart) == 0 {
		return d.ZScanFunc(tx, name, keyStart, 0, 0, limit, func(key []byte, score uint64) bool {
			return fn(key, SortableUint64ToFloat64(score))
		})
	}

	sStart := Float64ToSortableUint64(scoreStart)
	sEnd := Float64ToSortableUint64(scoreEnd)
	if scoreEnd < scoreStart {
		sEnd = scoreMax
	}
	return d.ZScanFunc(tx, name, keyStart, sStart, sEnd, limit, func(key []byte, score uint64) bool {
		return fn(key, SortableUint64ToFloat64(score))
	})
}

func (d *DB) ZRScanFunc(tx *bolt.Tx, name string, keyStart []byte, scoreStart, scoreEnd uint64, limit int, fn func(key []byte, score uint64) bool) error {
	if limit <= 0 {
		return nil
	}
	b := tx.Bucket(bucketZetScore)
	bufPtr1 := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr1)
	bufPtr2 := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr2)

	isStartEmpty := scoreStart == 0 && len(keyStart) == 0
	if isStartEmpty {
		scoreStart = scoreMax
	}
	if scoreEnd > scoreStart {
		scoreEnd = scoreMin
	}

	prefixLen := 1 + len(name)
	var seekKey, prefix []byte

	if isStartEmpty {
		prefix, _ = encodeHashKeyToBuf(name, nil, bufPtr1)
	} else {
		seekKey, _ = encodeZsetScoreKeyToBuf(name, keyStart, scoreStart, bufPtr1)
		prefix = seekKey[:prefixLen]
	}

	upper := keyUpperBoundToBuf(prefix, bufPtr2)
	c := b.Cursor()
	var k []byte

	if isStartEmpty {
		if upper != nil {
			k, _ = c.Seek(upper)
			if k == nil {
				k, _ = c.Last()
			} else {
				k, _ = c.Prev()
			}
		} else {
			k, _ = c.Last()
		}
	} else {
		k, _ = c.Seek(seekKey)
		if k == nil {
			k, _ = c.Last()
		}
	}

	n := 0
	for k != nil && bytes.Compare(k, prefix) >= 0 {
		if bytes.HasPrefix(k, prefix) {
			_, key, score, err := DecodeZsetScoreKey(k)
			if err != nil {
				k, _ = c.Prev()
				continue
			}
			if score < scoreEnd {
				break
			}
			if !isStartEmpty && bytes.Compare(k, seekKey) >= 0 {
				k, _ = c.Prev()
				continue
			}
			if !fn(key, score) {
				break
			}
			n++
			if n == limit {
				break
			}
		}
		k, _ = c.Prev()
	}
	return nil
}

func (d *DB) ZRScanFuncF(tx *bolt.Tx, name string, keyStart []byte, scoreStart, scoreEnd float64, limit int, fn func(key []byte, score float64) bool) error {
	// 【核心修复】识别 0, 0, nil 全表扫描占位符，直接透传给底层 uint64 引擎
	if scoreStart == 0 && scoreEnd == 0 && len(keyStart) == 0 {
		return d.ZRScanFunc(tx, name, keyStart, 0, 0, limit, func(key []byte, score uint64) bool {
			return fn(key, SortableUint64ToFloat64(score))
		})
	}

	sStart := Float64ToSortableUint64(scoreStart)
	sEnd := Float64ToSortableUint64(scoreEnd)
	if scoreEnd > scoreStart {
		sEnd = scoreMin
	}
	return d.ZRScanFunc(tx, name, keyStart, sStart, sEnd, limit, func(key []byte, score uint64) bool {
		return fn(key, SortableUint64ToFloat64(score))
	})
}

func (d *DB) ZDel(tx *bolt.Tx, name string, key []byte) error {
	b1 := tx.Bucket(bucketZetMember)
	b2 := tx.Bucket(bucketZetScore)
	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	reallyKey, _ := encodeHashKeyToBuf(name, key, bufPtr)
	oldScoreByte := b1.Get(reallyKey)
	if oldScoreByte == nil {
		return nil
	}
	_ = b1.Delete(reallyKey)

	var oldScore uint64
	if len(oldScoreByte) == uint64EncodedLen {
		oldScore = B2i(oldScoreByte)
	} else {
		oldScore, _ = parseUintBytes(oldScoreByte)
	}
	reallyScoreKey, _ := encodeZsetScoreKeyToBuf(name, key, oldScore, bufPtr)
	return b2.Delete(reallyScoreKey)
}

func (d *DB) ZMDel(tx *bolt.Tx, name string, keys [][]byte) error {
	for _, key := range keys {
		if err := d.ZDel(tx, name, key); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) ZDelBucket(tx *bolt.Tx, name string) error {
	b1 := tx.Bucket(bucketZetMember)
	b2 := tx.Bucket(bucketZetScore)
	if b1 == nil || b2 == nil {
		return ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	prefix, err := encodeHashKeyToBuf(name, nil, bufPtr)
	if err != nil {
		return err
	}

	c1 := b1.Cursor()
	for k, _ := c1.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c1.Seek(prefix) {
		if err := c1.Delete(); err != nil {
			return err
		}
	}

	c2 := b2.Cursor()
	for k, _ := c2.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c2.Seek(prefix) {
		if err := c2.Delete(); err != nil {
			return err
		}
	}

	return nil
}

// -----------------------
// 统计与压缩功能函数
// -----------------------

func (d *DB) Stats() (*DBStats, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("nil database")
	}
	d.mu.RLock()
	defer d.mu.RUnlock()

	path := d.db.Path()
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	fileSize := fi.Size()

	var dataSize int64
	err = d.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return b.ForEach(func(k, v []byte) error {
				dataSize += int64(len(k) + len(v))
				return nil
			})
		})
	})

	usageRatio := float64(0)
	if fileSize > 0 {
		usageRatio = float64(dataSize) / float64(fileSize)
	}
	fragRatio := math.Max(0, 1.0-usageRatio)

	return &DBStats{FilePath: path, FileSize: fileSize, DataSize: dataSize, UsageRatio: usageRatio, FragRatio: fragRatio}, err
}

func (d *DB) ShouldCompact(minFileSize int64, minFragRatio float64) (bool, *DBStats, error) {
	stats, err := d.Stats()
	if err != nil {
		return false, nil, err
	}
	if stats.FileSize < minFileSize || stats.FragRatio < minFragRatio {
		return false, stats, nil
	}
	return true, stats, nil
}

func (d *DB) Compact(dstPath string) error {
	if d == nil {
		return errors.New("nil database")
	}

	d.mu.RLock()
	if d.db == nil {
		d.mu.RUnlock()
		return errors.New("nil database")
	}
	origPath := d.db.Path()
	d.mu.RUnlock()

	isInPlace := dstPath == "" || dstPath == origPath
	targetPath := dstPath
	if isInPlace {
		targetPath = origPath + ".compact.tmp"
	}

	d.mu.RLock()
	err := compactDB(d.db, targetPath, 0o600)
	d.mu.RUnlock()
	if err != nil {
		if isInPlace {
			_ = os.Remove(targetPath)
		}
		return err
	}

	if !isInPlace {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.db.Close(); err != nil {
		_ = os.Remove(targetPath)
		return err
	}
	if err := os.Rename(targetPath, origPath); err != nil {
		_ = os.Remove(targetPath)
		return err
	}
	newDB, err := bolt.Open(origPath, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return err
	}
	d.db = newDB
	return nil
}

// CompactZip 执行在线压缩并将结果打包为 ZIP 文件。
//
// - dstPath: 压缩后的中间 bbolt 数据库文件路径。如果传入空字符串，将自动创建临时文件并在打包完成后删除。
// - zipPath: 最终生成的 .zip 文件路径。
func (d *DB) CompactZip(dstPath, zipPath string) error {
	if d == nil {
		return errors.New("nil database")
	}

	d.mu.RLock()
	if d.db == nil {
		d.mu.RUnlock()
		return errors.New("nil database")
	}
	origPath := d.db.Path()
	d.mu.RUnlock()

	// 判断是否需要创建临时文件
	targetPath := dstPath
	isTemp := false
	if targetPath == "" {
		targetPath = origPath + ".compact.tmp"
		isTemp = true
	}

	// 1. 在读锁保护下执行原生的 compactDB
	d.mu.RLock()
	err := compactDB(d.db, targetPath, 0o600)
	d.mu.RUnlock()

	if err != nil {
		if isTemp {
			_ = os.Remove(targetPath)
		}
		return err
	}

	// 2. 将压缩后的 db 文件打包为 zip
	err = createZipArchive(targetPath, zipPath)

	// 3. 清理临时文件（若指定了 dstPath 则保留该中间文件）
	if isTemp {
		_ = os.Remove(targetPath)
	}

	// 如果打包失败，清理掉生成的损坏 zip 文件
	if err != nil {
		_ = os.Remove(zipPath)
		return err
	}

	return nil
}

func compactDB(srcDB *bolt.DB, dstPath string, mode os.FileMode) error {
	dstDB, err := bolt.Open(dstPath, mode, &bolt.Options{
		Timeout: 5 * time.Second,
		NoSync:  true,
	})
	if err != nil {
		return err
	}
	defer dstDB.Close()

	const batchSize = 10000

	err = srcDB.View(func(srcTx *bolt.Tx) error {
		return srcTx.ForEach(func(name []byte, b *bolt.Bucket) error {
			err := dstDB.Update(func(dstTx *bolt.Tx) error {
				dstBucket, err := dstTx.CreateBucketIfNotExists(name)
				if err == nil {
					dstBucket.FillPercent = 0.9
				}
				return err
			})
			if err != nil {
				return err
			}

			c := b.Cursor()
			k, v := c.First()
			for k != nil {
				err := dstDB.Update(func(dstTx *bolt.Tx) error {
					dstBucket := dstTx.Bucket(name)
					dstBucket.FillPercent = 0.9
					count := 0
					for k != nil && count < batchSize {
						if err := dstBucket.Put(k, v); err != nil {
							return err
						}
						k, v = c.Next()
						count++
					}
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
	})

	if err != nil {
		return err
	}
	return dstDB.Sync()
}

// -----------------------
// 辅助函数
// -----------------------

// B2s converts byte slice to a string without memory allocation (Go 1.20+ safe).
func B2s(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// S2b converts string to a byte slice without memory allocation (Go 1.20+ safe).
func S2b(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func Float64ToSortableUint64(f float64) uint64 {
	u := math.Float64bits(f)
	if u&(1<<63) != 0 {
		return ^u
	}
	return u | (1 << 63)
}

func SortableUint64ToFloat64(u uint64) float64 {
	if u&(1<<63) != 0 {
		u = u &^ (1 << 63)
	} else {
		u = ^u
	}
	return math.Float64frombits(u)
}

func encodeHashKeyToBuf(name string, key []byte, bufPtr *[]byte) ([]byte, error) {
	if len(name) > 255 {
		return nil, ErrNameTooLong
	}
	if len(key) > 255 {
		return nil, ErrKeyTooLong
	}
	reqLen := 1 + len(name) + len(key)

	var buf []byte
	if bufPtr == nil {
		buf = make([]byte, reqLen)
	} else {
		if cap(*bufPtr) < reqLen {
			*bufPtr = make([]byte, reqLen)
		}
		buf = (*bufPtr)[:reqLen]
	}

	buf[0] = byte(len(name))
	copy(buf[1:], name)
	copy(buf[1+len(name):], key)
	return buf, nil
}

func EncodeHashKey(name string, key []byte) ([]byte, error) {
	return encodeHashKeyToBuf(name, key, nil)
}

func DecodeHashKey(buf []byte) (name string, key []byte, err error) {
	if len(buf) < 1 {
		return "", nil, ErrBufferTooShort
	}
	nameLen := int(buf[0])
	if len(buf) < 1+nameLen {
		return "", nil, ErrBufferTooShort
	}
	name = string(buf[1 : 1+nameLen])
	key = buf[1+nameLen:]
	if len(key) > 255 {
		return "", nil, ErrKeyTooLong
	}
	return name, key, nil
}

func encodeZsetScoreKeyToBuf(name string, key []byte, score uint64, bufPtr *[]byte) ([]byte, error) {
	if len(name) > 255 {
		return nil, ErrNameTooLong
	}
	if len(key) > 255 {
		return nil, ErrKeyTooLong
	}
	reqLen := 1 + len(name) + uint64EncodedLen + len(key)

	var buf []byte
	if bufPtr == nil {
		buf = make([]byte, reqLen)
	} else {
		if cap(*bufPtr) < reqLen {
			*bufPtr = make([]byte, reqLen)
		}
		buf = (*bufPtr)[:reqLen]
	}

	buf[0] = byte(len(name))
	copy(buf[1:], name)
	binary.BigEndian.PutUint64(buf[1+len(name):], score)
	copy(buf[1+len(name)+uint64EncodedLen:], key)
	return buf, nil
}

func EncodeZsetScoreKey(name string, key []byte, score uint64) ([]byte, error) {
	return encodeZsetScoreKeyToBuf(name, key, score, nil)
}

func DecodeZsetScoreKey(buf []byte) (name string, key []byte, score uint64, err error) {
	if len(buf) < 1+uint64EncodedLen {
		return "", nil, 0, ErrInvalidBuf
	}
	nameLen := int(buf[0])
	if len(buf) < 1+nameLen+uint64EncodedLen {
		return "", nil, 0, ErrInvalidBuf
	}
	name = string(buf[1 : 1+nameLen])
	scoreIndex := 1 + nameLen
	score = binary.BigEndian.Uint64(buf[scoreIndex : scoreIndex+uint64EncodedLen])
	keyIndex := scoreIndex + uint64EncodedLen
	key = make([]byte, len(buf)-keyIndex)
	copy(key, buf[keyIndex:])
	return name, key, score, nil
}

func I2b(v uint64) []byte {
	b := make([]byte, uint64EncodedLen)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func B2i(v []byte) uint64 {
	if len(v) < uint64EncodedLen {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

// DS2b returns an 8-byte big endian representation of Digit string
// v ("123456") -> uint64(123456) -> 8-byte big endian.
func DS2b(v string) []byte {
	i, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return []byte("")
	}
	return I2b(i)
}

// DS2i returns uint64 of Digit string
// v ("123456") -> uint64(123456).
func DS2i(v string) uint64 {
	i, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return uint64(0)
	}
	return i
}

func parseUintBytes(b []byte) (uint64, error) {
	if len(b) == 0 {
		return 0, errors.New("empty bytes")
	}
	var n uint64
	for _, ch := range b {
		if ch < '0' || ch > '9' {
			return 0, errors.New("invalid char")
		}
		n = n*10 + uint64(ch-'0')
	}
	return n, nil
}

func parseIntBytes(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, errors.New("empty bytes")
	}
	neg := false
	if b[0] == '-' {
		neg = true
		b = b[1:]
	}
	if len(b) == 0 {
		return 0, errors.New("empty bytes")
	}
	var n int64
	for _, ch := range b {
		if ch < '0' || ch > '9' {
			return 0, errors.New("invalid char")
		}
		n = n*10 + int64(ch-'0')
	}
	if neg {
		return -n, nil
	}
	return n, nil
}

func BConcat(slices ...[]byte) []byte {
	var totalLen int
	for _, s := range slices {
		totalLen += len(s)
	}
	tmp := make([]byte, 0, totalLen)
	for _, s := range slices {
		tmp = append(tmp, s...)
	}
	return tmp
}

func keyUpperBound(b []byte) []byte {
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			end = end[:i+1]
			return end
		}
	}
	return nil
}

// 优化后：零分配求上界
func keyUpperBoundToBuf(b []byte, bufPtr *[]byte) []byte {
	if len(b) == 0 {
		return nil
	}

	reqLen := len(b)
	var end []byte

	if bufPtr == nil {
		end = make([]byte, reqLen)
	} else {
		if cap(*bufPtr) < reqLen {
			*bufPtr = make([]byte, reqLen)
		}
		end = (*bufPtr)[:reqLen]
	}

	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			return end[:i+1]
		}
	}
	return nil
}

// createZipArchive 辅助函数：将指定的源文件打包成 zip 格式
func createZipArchive(sourcePath, zipPath string) (err error) {
	// 创建 zip 输出文件
	outFile, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	// 确保发生错误时文件描述符能被正确关闭
	defer func() {
		if cerr := outFile.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	zipWriter := zip.NewWriter(outFile)
	defer func() {
		if cerr := zipWriter.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	// 打开要打包的源文件
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	// 获取文件信息，用于生成 zip 文件头
	info, err := sourceFile.Stat()
	if err != nil {
		return err
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}
	// 将文件内部名称设定为其自身的名字，去除绝对路径
	header.Name = filepath.Base(sourcePath)
	// 指定使用 Deflate 压缩算法
	header.Method = zip.Deflate

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return err
	}

	// 将源文件内容拷贝到 zip 写入器中
	_, err = io.Copy(writer, sourceFile)
	return err
}
