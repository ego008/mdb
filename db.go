package mdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
	"unsafe"

	bolt "go.etcd.io/bbolt"
)

const (
	kvpLen                  = "kvs len must is an even number"
	scoreMin         uint64 = 0
	scoreMax         uint64 = ^uint64(0)
	uint64EncodedLen        = 8
)

var (
	bucketHash      = []byte("h")  // 存储 k: v , 格式 k = EncodeHashKey(name, key)
	bucketZetMember = []byte("zm") // 存储 k: score , 格式 k = EncodeHashKey(name, key), score = I2b(uint64)
	bucketZetScore  = []byte("zc") // 存储 k: nil , 格式 k = EncodeZsetScoreKey(name, key, score),

	ErrNilBucket       = errors.New("bucket is nil")
	ErrBufferTooShort  = errors.New("buffer too short")
	ErrNameTooLong     = errors.New("name length exceeds 255 bytes")
	ErrKeyTooLong      = errors.New("key length exceeds 255 bytes")
	ErrInvalidBuf      = errors.New("invalid buffer length")
	ErrKeyValuePairLen = errors.New(kvpLen)
	ErrKeyNotFound     = errors.New("key not found") // 统一未找到数据的错误提示

	// keyBufPool 预分配对象池，消除高频读写时的 slice 内存分配
	keyBufPool = sync.Pool{
		New: func() interface{} {
			// Max Name(255) + Max Key(255) + 9 Bytes overhead
			b := make([]byte, 520)
			return &b
		},
	}
)

type (
	DB struct {
		db *bolt.DB
		mu sync.RWMutex // 仅使用读写锁来保护在线 Compact 时的句柄热替换
	}

	DBStats struct {
		FilePath   string  `json:"file_path"`
		FileSize   int64   `json:"file_size"`
		DataSize   int64   `json:"data_size"`
		UsageRatio float64 `json:"usage_ratio"`
		FragRatio  float64 `json:"frag_ratio"`
	}
)

func Open(path string) (*DB, error) {
	return OpenWithMode(path, 0o600)
}

func OpenWithMode(path string, mode os.FileMode) (*DB, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("empty database path")
	}
	database, err := bolt.Open(path, mode, &bolt.Options{Timeout: time.Second})
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

// View 执行只读事务 (去除了 WaitGroup，轻装上阵)
func (d *DB) View(fn func(*bolt.Tx) error) error {
	if d == nil {
		return errors.New("nil database")
	}
	if fn == nil {
		return errors.New("nil function callback")
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.db == nil {
		return errors.New("nil database")
	}
	return d.db.View(fn)
}

// Update 执行读写事务
func (d *DB) Update(fn func(*bolt.Tx) error) error {
	if d == nil {
		return errors.New("nil database")
	}
	if fn == nil {
		return errors.New("nil function callback")
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
	if b == nil {
		return ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	reallyKey, err := encodeHashKeyToBuf(name, key, *bufPtr)
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
	if b == nil {
		return ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	buf := *bufPtr

	for i := 0; i < len(kvs)-1; i += 2 {
		reallyKey, err := encodeHashKeyToBuf(name, kvs[i], buf)
		if err != nil {
			return err
		}
		if err = b.Put(reallyKey, kvs[i+1]); err != nil {
			return err
		}
	}

	return nil
}

func (d *DB) Hincr(tx *bolt.Tx, name string, key []byte, step int64) (uint64, error) {
	b := tx.Bucket(bucketHash)
	if b == nil {
		return 0, ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	reallyKey, err := encodeHashKeyToBuf(name, key, *bufPtr)
	if err != nil {
		return 0, err
	}

	var current uint64
	v := b.Get(reallyKey)
	if len(v) == uint64EncodedLen {
		current = B2i(v)
	} else if len(v) > 0 {
		// 零拷贝解析数值
		parsedUint, parseErr := parseUintBytes(v)
		if parseErr != nil {
			parsedInt, parseErr2 := parseIntBytes(v)
			if parseErr2 != nil {
				return 0, fmt.Errorf("hash value is not a valid integer")
			}
			current = uint64(parsedInt)
		} else {
			current = parsedUint
		}
	}

	newVal := uint64(int64(current) + step)
	newValBytes := I2b(newVal)

	if err := b.Put(reallyKey, newValBytes); err != nil {
		return 0, err
	}

	return newVal, nil
}

// HGetFunc 安全回调式读取，消除内存分配与 Mmap 访问越界隐患
func (d *DB) HGetFunc(tx *bolt.Tx, name string, key []byte, fn func(val []byte) error) error {
	b := tx.Bucket(bucketHash)
	if b == nil {
		return ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	reallyKey, err := encodeHashKeyToBuf(name, key, *bufPtr)
	if err != nil {
		return err
	}

	v := b.Get(reallyKey)
	if v == nil {
		return ErrKeyNotFound
	}
	return fn(v)
}

// HMGetFunc 批量回调读取
// fn 的第二个参数 val 如果是 nil，代表该 key 不存在
func (d *DB) HMGetFunc(tx *bolt.Tx, name string, keys [][]byte, fn func(key, val []byte) error) error {
	b := tx.Bucket(bucketHash)
	if b == nil {
		return ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	buf := *bufPtr

	for _, key := range keys {
		reallyKey, err := encodeHashKeyToBuf(name, key, buf)
		if err != nil {
			continue
		}
		v := b.Get(reallyKey)
		if err := fn(key, v); err != nil {
			return err
		}
	}
	return nil
}

// HScanFunc 正序扫描。若回调 fn 返回 false，则终止遍历。
func (d *DB) HScanFunc(tx *bolt.Tx, name string, keyStart []byte, limit int, fn func(key, val []byte) bool) error {
	if limit <= 0 {
		return nil
	}
	b := tx.Bucket(bucketHash)
	if b == nil {
		return ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	buf := *bufPtr

	reallyKeyStart, err := encodeHashKeyToBuf(name, keyStart, buf)
	if err != nil {
		return err
	}

	prefix, err := encodeHashKeyToBuf(name, nil, buf)
	if err != nil {
		return err
	}
	// 为了不被迭代修改覆盖，克隆 prefix
	prefixCopy := append([]byte(nil), prefix...)
	prefixLen := len(prefixCopy)

	c := b.Cursor()
	n := 0

	for k, v := c.Seek(reallyKeyStart); k != nil; k, v = c.Next() {
		if !bytes.HasPrefix(k, prefixCopy) {
			break
		}
		if bytes.Compare(k, reallyKeyStart) <= 0 {
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

// HRScanFunc 逆序扫描
func (d *DB) HRScanFunc(tx *bolt.Tx, name string, keyStart []byte, limit int, fn func(key, val []byte) bool) error {
	if limit <= 0 {
		return nil
	}
	b := tx.Bucket(bucketHash)
	if b == nil {
		return ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	buf := *bufPtr

	prefix, err := encodeHashKeyToBuf(name, nil, buf)
	if err != nil {
		return err
	}
	prefixCopy := append([]byte(nil), prefix...)
	prefixLen := len(prefixCopy)

	var seekKey []byte
	isStartEmpty := len(keyStart) == 0

	if isStartEmpty {
		seekKey, err = EncodeHashKey(name, bytes.Repeat([]byte{0xFF}, 255))
	} else {
		seekKey, err = EncodeHashKey(name, keyStart)
	}
	if err != nil {
		return err
	}

	c := b.Cursor()
	k, v := c.Seek(seekKey)
	if k == nil {
		k, v = c.Last()
	}

	n := 0
	for k != nil {
		if bytes.Compare(k, prefixCopy) < 0 {
			break
		}

		if bytes.HasPrefix(k, prefixCopy) {
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
	if b == nil {
		return ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	reallyKey, err := encodeHashKeyToBuf(name, key, *bufPtr)
	if err != nil {
		return err
	}

	return b.Delete(reallyKey)
}

func (d *DB) HMDel(tx *bolt.Tx, name string, keys [][]byte) error {
	b := tx.Bucket(bucketHash)
	if b == nil {
		return ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	buf := *bufPtr

	for _, key := range keys {
		reallyKey, err := encodeHashKeyToBuf(name, key, buf)
		if err != nil {
			return err
		}
		if err := b.Delete(reallyKey); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) HDelBucket(tx *bolt.Tx, name string) error {
	b := tx.Bucket(bucketHash)
	if b == nil {
		return ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	prefix, err := encodeHashKeyToBuf(name, nil, *bufPtr)
	if err != nil {
		return err
	}

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
	if b1 == nil {
		return ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	reallyKey, err := encodeHashKeyToBuf(name, key, *bufPtr)
	if err != nil {
		return err
	}

	scoreByte := I2b(score)
	oldScoreByte := b1.Get(reallyKey)
	if bytes.Equal(oldScoreByte, scoreByte) {
		return nil
	}

	if err = b1.Put(reallyKey, scoreByte); err != nil {
		return err
	}

	reallyScoreKey, err := encodeZsetScoreKeyToBuf(name, key, score, *bufPtr)
	if err != nil {
		return err
	}

	b2 := tx.Bucket(bucketZetScore)
	if b2 == nil {
		return ErrNilBucket
	}

	if err = b2.Put(reallyScoreKey, []byte{}); err != nil {
		return err
	}

	if len(oldScoreByte) == uint64EncodedLen {
		reallyScoreKey, err = encodeZsetScoreKeyToBuf(name, key, B2i(oldScoreByte), *bufPtr)
		if err != nil {
			return err
		}
		if err = b2.Delete(reallyScoreKey); err != nil {
			return err
		}
	}

	return nil
}

func (d *DB) ZMSet(tx *bolt.Tx, name string, kvs ...[]byte) error {
	if len(kvs) == 0 || len(kvs)%2 != 0 {
		return ErrKeyValuePairLen
	}

	for i := 0; i < len(kvs)-1; i += 2 {
		key := kvs[i]
		scoreBuf := kvs[i+1]
		var score uint64
		if len(scoreBuf) == uint64EncodedLen {
			score = B2i(scoreBuf)
		} else {
			parsedUint, parseErr := parseUintBytes(scoreBuf)
			if parseErr != nil {
				return fmt.Errorf("zset score is not a valid uint: %v", parseErr)
			}
			score = parsedUint
		}
		if err := d.ZSet(tx, name, key, score); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) Zincr(tx *bolt.Tx, name string, key []byte, step int64) (uint64, error) {
	b1 := tx.Bucket(bucketZetMember)
	if b1 == nil {
		return 0, ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	reallyKey, err := encodeHashKeyToBuf(name, key, *bufPtr)
	if err != nil {
		return 0, err
	}

	var current uint64
	v := b1.Get(reallyKey)
	if len(v) == uint64EncodedLen {
		current = B2i(v)
	} else if len(v) > 0 {
		parsedUint, parseErr := parseUintBytes(v)
		if parseErr == nil {
			current = parsedUint
		}
	}

	newScore := uint64(int64(current) + step)

	if err := d.ZSet(tx, name, key, newScore); err != nil {
		return 0, err
	}

	return newScore, nil
}

// ZGetFunc 读取并回调成员 score
func (d *DB) ZGetFunc(tx *bolt.Tx, name string, key []byte, fn func(score uint64) error) error {
	b := tx.Bucket(bucketZetMember)
	if b == nil {
		return ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	reallyKey, err := encodeHashKeyToBuf(name, key, *bufPtr)
	if err != nil {
		return err
	}

	v := b.Get(reallyKey)
	if v == nil {
		return ErrKeyNotFound
	}

	var score uint64
	if len(v) == uint64EncodedLen {
		score = B2i(v)
	} else {
		score, _ = parseUintBytes(v)
	}

	return fn(score)
}

// ZMGetFunc 批量获取 member。回调参数 includesExists 用来标识是否命中
func (d *DB) ZMGetFunc(tx *bolt.Tx, name string, keys [][]byte, fn func(key []byte, score uint64, exists bool) error) error {
	b := tx.Bucket(bucketZetMember)
	if b == nil {
		return ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	buf := *bufPtr

	for _, key := range keys {
		reallyKey, err := encodeHashKeyToBuf(name, key, buf)
		if err != nil {
			continue
		}

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

// ZScanFunc 正序扫描 Score 及 Key。若 fn 返回 false 则提前终止。
func (d *DB) ZScanFunc(tx *bolt.Tx, name string, keyStart []byte, scoreStart, scoreEnd uint64, limit int, fn func(key []byte, score uint64) bool) error {
	if limit <= 0 {
		return nil
	}
	b := tx.Bucket(bucketZetScore)
	if b == nil {
		return ErrNilBucket
	}

	if scoreEnd == 0 || scoreEnd < scoreStart {
		scoreEnd = scoreMax
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	buf := *bufPtr

	prefix, err := encodeHashKeyToBuf(name, nil, buf)
	if err != nil {
		return err
	}
	prefixCopy := append([]byte(nil), prefix...)

	reallyKeyStart, err := EncodeZsetScoreKey(name, keyStart, scoreStart)
	if err != nil {
		return err
	}

	c := b.Cursor()
	n := 0

	for k, _ := c.Seek(reallyKeyStart); k != nil; k, _ = c.Next() {
		if !bytes.HasPrefix(k, prefixCopy) {
			break
		}

		_, key, score, err := DecodeZsetScoreKey(k)
		if err != nil {
			continue
		}
		if score > scoreEnd {
			break
		}

		if bytes.Compare(k, reallyKeyStart) <= 0 {
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

// ZRScanFunc 逆序扫描 Score 及 Key。
func (d *DB) ZRScanFunc(tx *bolt.Tx, name string, keyStart []byte, scoreStart, scoreEnd uint64, limit int, fn func(key []byte, score uint64) bool) error {
	if limit <= 0 {
		return nil
	}
	b := tx.Bucket(bucketZetScore)
	if b == nil {
		return ErrNilBucket
	}

	isStartEmpty := scoreStart == 0 && len(keyStart) == 0
	if isStartEmpty {
		scoreStart = scoreMax
	}
	if scoreEnd > scoreStart {
		scoreEnd = scoreMin
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)
	buf := *bufPtr

	prefix, err := encodeHashKeyToBuf(name, nil, buf)
	if err != nil {
		return err
	}
	prefixCopy := append([]byte(nil), prefix...)

	var seekKey []byte
	if isStartEmpty {
		seekKey, err = EncodeZsetScoreKey(name, bytes.Repeat([]byte{0xFF}, 255), scoreMax)
	} else {
		seekKey, err = EncodeZsetScoreKey(name, keyStart, scoreStart)
	}
	if err != nil {
		return err
	}

	c := b.Cursor()
	k, _ := c.Seek(seekKey)
	if k == nil {
		k, _ = c.Last()
	}

	n := 0
	for k != nil {
		if bytes.Compare(k, prefixCopy) < 0 {
			break
		}

		if bytes.HasPrefix(k, prefixCopy) {
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

func (d *DB) ZDel(tx *bolt.Tx, name string, key []byte) error {
	b1 := tx.Bucket(bucketZetMember)
	b2 := tx.Bucket(bucketZetScore)
	if b1 == nil || b2 == nil {
		return ErrNilBucket
	}

	bufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(bufPtr)

	reallyKey, err := encodeHashKeyToBuf(name, key, *bufPtr)
	if err != nil {
		return err
	}

	oldScoreByte := b1.Get(reallyKey)
	if oldScoreByte == nil {
		return nil
	}

	if err := b1.Delete(reallyKey); err != nil {
		return err
	}

	var oldScore uint64
	if len(oldScoreByte) == uint64EncodedLen {
		oldScore = B2i(oldScoreByte)
	} else if len(oldScoreByte) > 0 {
		oldScore, _ = parseUintBytes(oldScoreByte)
	}

	reallyScoreKey, err := encodeZsetScoreKeyToBuf(name, key, oldScore, *bufPtr)
	if err != nil {
		return err
	}

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

	prefix, err := encodeHashKeyToBuf(name, nil, *bufPtr)
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
	if d == nil {
		return nil, errors.New("nil database")
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.db == nil {
		return nil, errors.New("nil database")
	}

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
	if err != nil {
		return nil, err
	}

	var usageRatio float64
	if fileSize > 0 {
		usageRatio = float64(dataSize) / float64(fileSize)
	}
	fragRatio := 1.0 - usageRatio
	if fragRatio < 0 {
		fragRatio = 0
	}

	return &DBStats{
		FilePath:   path,
		FileSize:   fileSize,
		DataSize:   dataSize,
		UsageRatio: usageRatio,
		FragRatio:  fragRatio,
	}, nil
}

func (d *DB) ShouldCompact(minFileSize int64, minFragRatio float64) (bool, *DBStats, error) {
	stats, err := d.Stats()
	if err != nil {
		return false, nil, err
	}

	if stats.FileSize < minFileSize {
		return false, stats, nil
	}

	if stats.FragRatio < minFragRatio {
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
		return fmt.Errorf("failed to close old db for compaction: %w", err)
	}

	if err := os.Rename(targetPath, origPath); err != nil {
		_ = os.Remove(targetPath)
		return fmt.Errorf("failed to replace db file: %w", err)
	}

	newDB, err := bolt.Open(origPath, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return fmt.Errorf("failed to reopen db after compaction: %w", err)
	}
	d.db = newDB

	return nil
}

func compactDB(srcDB *bolt.DB, dstPath string, mode os.FileMode) error {
	dstDB, err := bolt.Open(dstPath, mode, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return err
	}
	defer dstDB.Close()

	const batchSize = 10000

	return srcDB.View(func(srcTx *bolt.Tx) error {
		return srcTx.ForEach(func(name []byte, b *bolt.Bucket) error {
			err := dstDB.Update(func(dstTx *bolt.Tx) error {
				_, err := dstTx.CreateBucketIfNotExists(name)
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
}

// -----------------------
// 辅助函数
// -----------------------

func encodeHashKeyToBuf(name string, key []byte, buf []byte) ([]byte, error) {
	if len(name) > 255 {
		return nil, ErrNameTooLong
	}
	if len(key) > 255 {
		return nil, ErrKeyTooLong
	}
	reqLen := 1 + len(name) + len(key)
	if len(buf) < reqLen {
		buf = make([]byte, reqLen)
	}
	buf[0] = byte(len(name))
	copy(buf[1:], name)
	copy(buf[1+len(name):], key)
	return buf[:reqLen], nil
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

func encodeZsetScoreKeyToBuf(name string, key []byte, score uint64, buf []byte) ([]byte, error) {
	if len(name) > 255 {
		return nil, ErrNameTooLong
	}
	if len(key) > 255 {
		return nil, ErrKeyTooLong
	}
	reqLen := 1 + len(name) + uint64EncodedLen + len(key)
	if len(buf) < reqLen {
		buf = make([]byte, reqLen)
	}
	buf[0] = byte(len(name))
	copy(buf[1:], name)
	binary.BigEndian.PutUint64(buf[1+len(name):], score)
	copy(buf[1+len(name)+uint64EncodedLen:], key)
	return buf[:reqLen], nil
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

func B2s(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
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
