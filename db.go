package mdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	bolt "go.etcd.io/bbolt"
)

const (
	replyOK                 = "ok"
	replyNotFound           = "not_found"
	replyError              = "error"
	bucketNotFound          = "bucket_not_found"
	keyNotFound             = "key_not_found"
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
)

type (
	BS []byte
	DB struct {
		db *bolt.DB
		wg sync.WaitGroup
	}

	Reply struct {
		State string
		Data  []BS
	}

	// Entry a key-value pair.
	Entry struct {
		Key, Value BS
	}
)

func Open(path string) (*DB, error) {
	return OpenWithMode(path, 0o600)
}

// OpenWithMode opens a database using the supplied file mode.
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

// View executes a read-only transaction.
func (d *DB) View(fn func(*bolt.Tx) error) error {
	if d == nil || d.db == nil {
		return errors.New("nil database")
	}
	if fn == nil {
		return errors.New("nil transaction callback")
	}
	d.wg.Add(1)
	defer d.wg.Done()
	return d.db.View(fn)
}

// Update executes a read-write transaction.
func (d *DB) Update(fn func(*bolt.Tx) error) error {
	if d == nil || d.db == nil {
		return errors.New("nil database")
	}
	if fn == nil {
		return errors.New("nil transaction callback")
	}
	d.wg.Add(1)
	defer d.wg.Done()
	return d.db.Update(fn)
}

func (d *DB) Close() error {
	d.wg.Wait()
	return d.db.Close()
}

// -------------------
// Hash 功能函数
// -------------------

func (d *DB) HSet(tx *bolt.Tx, name string, key, val []byte) error {
	b := tx.Bucket(bucketHash)
	if b == nil {
		return ErrNilBucket
	}
	reallyKey, err := EncodeHashKey(name, key)
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

	for i := 0; i < len(kvs)-1; i += 2 {
		reallyKey, err := EncodeHashKey(name, kvs[i])
		if err != nil {
			return err
		}
		if err = b.Put(reallyKey, kvs[i+1]); err != nil {
			return err
		}
	}

	return nil
}

// Hincr 对 Hash 中指定 key 的数值进行自增/自减操作，并返回更新后的值。
// 如果 key 不存在，初始值视为 0。
func (d *DB) Hincr(tx *bolt.Tx, name string, key []byte, step int64) (uint64, error) {
	b := tx.Bucket(bucketHash)
	if b == nil {
		return 0, ErrNilBucket
	}

	reallyKey, err := EncodeHashKey(name, key)
	if err != nil {
		return 0, err
	}

	var current uint64
	v := b.Get(reallyKey)
	if len(v) == uint64EncodedLen {
		current = B2i(v)
	} else if len(v) > 0 {
		var parseErr error
		current, parseErr = strconv.ParseUint(string(v), 10, 64)
		if parseErr != nil {
			// 若按 uint64 解析失败，尝试按 int64 解析
			iVal, parseErr2 := strconv.ParseInt(string(v), 10, 64)
			if parseErr2 != nil {
				return 0, fmt.Errorf("hash value is not a valid integer: %v", parseErr)
			}
			current = uint64(iVal)
		}
	}

	newVal := uint64(int64(current) + step)
	newValBytes := I2b(newVal)

	if err := b.Put(reallyKey, newValBytes); err != nil {
		return 0, err
	}

	return newVal, nil
}

func (d *DB) HGet(tx *bolt.Tx, name string, key []byte) *Reply {
	r := &Reply{State: replyError, Data: []BS{}}
	b := tx.Bucket(bucketHash)
	if b == nil {
		r.State = bucketNotFound
		return r
	}
	reallyKey, err := EncodeHashKey(name, key)
	if err != nil {
		r.State = err.Error()
		return r
	}
	v := b.Get(reallyKey)
	if v == nil {
		r.State = keyNotFound
		return r
	}
	r.State = replyOK
	r.Data = append(r.Data, v)
	return r
}

// HMGet 批量获取 Hash 中多个 key 的 value。
// 返回 Reply.Data 格式为 [k1, v1, k2, v2, ...]
func (d *DB) HMGet(tx *bolt.Tx, name string, keys [][]byte) *Reply {
	r := &Reply{
		State: replyError,
		Data:  make([]BS, 0, len(keys)*2),
	}

	b := tx.Bucket(bucketHash)
	if b == nil {
		r.State = bucketNotFound
		return r
	}

	for _, key := range keys {
		reallyKey, err := EncodeHashKey(name, key)
		if err != nil {
			r.Data = append(r.Data, key, nil)
			continue
		}
		v := b.Get(reallyKey)
		r.Data = append(r.Data, key, v)
	}

	r.State = replyOK
	return r
}

func (d *DB) HScan(tx *bolt.Tx, name string, keyStart []byte, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []BS{},
	}

	b := tx.Bucket(bucketHash)
	if b == nil {
		r.State = bucketNotFound
		return r
	}

	// 计算遍历起始的完整 Key
	reallyKeyStart, err := EncodeHashKey(name, keyStart)
	if err != nil {
		r.State = err.Error()
		return r
	}

	// 计算当前 Hash 表(name)的前缀
	prefix, err := EncodeHashKey(name, nil)
	if err != nil {
		r.State = err.Error()
		return r
	}

	prefixLen := len(prefix)
	c := b.Cursor()
	n := 0

	for k, v := c.Seek(reallyKeyStart); k != nil; k, v = c.Next() {
		// 1. 前缀校验：一旦 Key 不再包含当前 name 的前缀，说明已遍历完该 Hash 表，立即退出
		if !bytes.HasPrefix(k, prefix) {
			break
		}

		// 2. 跳过 keyStart 本身（HScan 语义为遍历大于 keyStart 的元素）
		if bytes.Compare(k, reallyKeyStart) <= 0 {
			continue
		}

		r.Data = append(r.Data, k[prefixLen:], v)
		n++
		if n == limit {
			break
		}
	}

	r.State = replyOK
	return r
}

// HRScan 逆序遍历 Hash 数据 (Reverse Scan)
// keyStart 为空时，从当前 Hash 表的最大 Key 开始逆序遍历；
// keyStart 非空时，遍历小于 keyStart 的元素。
func (d *DB) HRScan(tx *bolt.Tx, name string, keyStart []byte, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []BS{},
	}

	if limit <= 0 {
		r.State = replyOK
		return r
	}

	b := tx.Bucket(bucketHash)
	if b == nil {
		r.State = bucketNotFound
		return r
	}

	// 计算当前 Hash 表的前缀
	prefix, err := EncodeHashKey(name, nil)
	if err != nil {
		r.State = err.Error()
		return r
	}
	prefixLen := len(prefix)

	var seekKey []byte
	isStartEmpty := len(keyStart) == 0

	if isStartEmpty {
		// 当 keyStart 为空时，生成当前 name 可能的最大 Key（Key 最大 255 字节），用于定位到 name 的末尾
		maxKey, err := EncodeHashKey(name, bytes.Repeat([]byte{0xFF}, 255))
		if err != nil {
			r.State = err.Error()
			return r
		}
		seekKey = maxKey
	} else {
		reallyKeyStart, err := EncodeHashKey(name, keyStart)
		if err != nil {
			r.State = err.Error()
			return r
		}
		seekKey = reallyKeyStart
	}

	c := b.Cursor()
	k, v := c.Seek(seekKey)
	if k == nil {
		// 如果 seekKey 超出了整张 bucket 的末尾，从 bucket 的最后一个 key 开始逆序查找
		k, v = c.Last()
	}

	n := 0
	for k != nil {
		// 1. 下界检查：如果当前 Key 的字节序已经小于当前 name 的前缀，说明已遍历完该 Hash 表，立即退出
		if bytes.Compare(k, prefix) < 0 {
			break
		}

		// 2. 属于当前 name 前缀的合法数据
		if bytes.HasPrefix(k, prefix) {
			// 如果指定了 keyStart，过滤掉大于等于 keyStart 的元素（保证只获取 strictly小于 keyStart 的元素）
			if !isStartEmpty && bytes.Compare(k, seekKey) >= 0 {
				k, v = c.Prev()
				continue
			}

			r.Data = append(r.Data, k[prefixLen:], v)
			n++
			if n == limit {
				break
			}
		}

		// 游标向上（逆序）移动
		k, v = c.Prev()
	}

	r.State = replyOK
	return r
}

// -------------------
// Zet 功能函数
// -------------------

func (d *DB) ZSet(tx *bolt.Tx, name string, key []byte, score uint64) error {
	b1 := tx.Bucket(bucketZetMember)
	if b1 == nil {
		return ErrNilBucket
	}
	reallyKey, err := EncodeHashKey(name, key)
	if err != nil {
		return err
	}

	scoreByte := I2b(score)

	// get old score
	oldScoreByte := b1.Get(reallyKey)
	if bytes.Equal(oldScoreByte, scoreByte) {
		return nil
	}

	// set new score kv
	if err = b1.Put(reallyKey, scoreByte); err != nil {
		return err
	}
	// set new score k
	reallyScoreKey, err := EncodeZsetScoreKey(name, key, score)
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
		reallyScoreKey, err = EncodeZsetScoreKey(name, key, B2i(oldScoreByte))
		if err != nil {
			return err
		}
		if err = b2.Delete(reallyScoreKey); err != nil {
			return err
		}
	}

	return nil
}

// ZMSet 批量设置 Zet 成员及其 score。
// kvs 格式为 [key1, score1, key2, score2, ...]，score 支持 8 字节 BigEndian 或数字字符串。
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
			score = DS2i(B2s(scoreBuf))
		}
		if err := d.ZSet(tx, name, key, score); err != nil {
			return err
		}
	}

	return nil
}

// Zincr 对 Zet 中指定 member 的 score 进行自增/自减操作，并返回更新后的 score。
// 如果 member 不存在，初始 score 视为 0。
func (d *DB) Zincr(tx *bolt.Tx, name string, key []byte, step int64) (uint64, error) {
	b1 := tx.Bucket(bucketZetMember)
	if b1 == nil {
		return 0, ErrNilBucket
	}

	reallyKey, err := EncodeHashKey(name, key)
	if err != nil {
		return 0, err
	}

	var current uint64
	v := b1.Get(reallyKey)
	if len(v) == uint64EncodedLen {
		current = B2i(v)
	} else if len(v) > 0 {
		current = DS2i(B2s(v))
	}

	newScore := uint64(int64(current) + step)

	// 复用 ZSet 更新成员 Score 并重置 ZSetScore 索引
	if err := d.ZSet(tx, name, key, newScore); err != nil {
		return 0, err
	}

	return newScore, nil
}

func (d *DB) ZGet(tx *bolt.Tx, name string, key []byte) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []BS{},
	}

	reallyKey, err := EncodeHashKey(name, key)
	if err != nil {
		r.State = err.Error()
		return r
	}

	b := tx.Bucket(bucketZetMember)
	if b == nil {
		r.State = ErrNilBucket.Error()
		return r
	}

	v := b.Get(reallyKey)
	if v == nil {
		r.State = keyNotFound
		return r
	}

	r.State = replyOK
	r.Data = append(r.Data, v)

	return r
}

// ZMGet 批量获取 Zet 中多个 member 的 score。
// 返回 Reply.Data 格式为 [k1, score1, k2, score2, ...]
func (d *DB) ZMGet(tx *bolt.Tx, name string, keys [][]byte) *Reply {
	r := &Reply{
		State: replyError,
		Data:  make([]BS, 0, len(keys)*2),
	}

	b := tx.Bucket(bucketZetMember)
	if b == nil {
		r.State = bucketNotFound
		return r
	}

	for _, key := range keys {
		reallyKey, err := EncodeHashKey(name, key)
		if err != nil {
			r.Data = append(r.Data, key, nil)
			continue
		}
		v := b.Get(reallyKey)
		r.Data = append(r.Data, key, v)
	}

	r.State = replyOK
	return r
}

// ZScan 正序遍历 Zet 数据 (Scan by Score & Key)
// 按 score 升序及 key 字典序升序遍历。
// keyStart 与 scoreStart 指定起始点（结果不包含起始点本身）；
// keyStart 为空且 scoreStart 为 0 时从最小值开始遍历；
// scoreEnd 为 0 或小于 scoreStart 时默认遍历至最大 score。
func (d *DB) ZScan(tx *bolt.Tx, name string, keyStart []byte, scoreStart, scoreEnd uint64, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []BS{},
	}

	if limit <= 0 {
		r.State = replyOK
		return r
	}

	b := tx.Bucket(bucketZetScore)
	if b == nil {
		r.State = bucketNotFound
		return r
	}

	if scoreEnd == 0 || scoreEnd < scoreStart {
		scoreEnd = scoreMax
	}

	prefix, err := EncodeHashKey(name, nil)
	if err != nil {
		r.State = err.Error()
		return r
	}

	reallyKeyStart, err := EncodeZsetScoreKey(name, keyStart, scoreStart)
	if err != nil {
		r.State = err.Error()
		return r
	}

	c := b.Cursor()
	n := 0

	for k, _ := c.Seek(reallyKeyStart); k != nil; k, _ = c.Next() {
		// 1. 前缀检查：超出当前 name 的范围则退出
		if !bytes.HasPrefix(k, prefix) {
			break
		}

		// 2. 解码并检查 score 上限
		_, key, score, err := DecodeZsetScoreKey(k)
		if err != nil {
			continue
		}
		if score > scoreEnd {
			break
		}

		// 3. 跳过小于等于 (scoreStart, keyStart) 的节点
		if bytes.Compare(k, reallyKeyStart) <= 0 {
			continue
		}

		r.Data = append(r.Data, key, I2b(score))
		n++
		if n == limit {
			break
		}
	}

	r.State = replyOK
	return r
}

// ZRScan 逆序遍历 Zet 数据 (Reverse Scan by Score & Key)
// 按 score 降序及 key 字典序降序遍历。
// scoreStart 与 keyStart 指定起始点（结果不包含起始点本身）；
// scoreStart 为 0 且 keyStart 为空时从最大值开始遍历；
// scoreEnd 大于 scoreStart 时默认下界为 0 (scoreMin)。
func (d *DB) ZRScan(tx *bolt.Tx, name string, keyStart []byte, scoreStart, scoreEnd uint64, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []BS{},
	}

	if limit <= 0 {
		r.State = replyOK
		return r
	}

	b := tx.Bucket(bucketZetScore)
	if b == nil {
		r.State = bucketNotFound
		return r
	}

	isStartEmpty := scoreStart == 0 && len(keyStart) == 0
	if isStartEmpty {
		scoreStart = scoreMax
	}
	if scoreEnd > scoreStart {
		scoreEnd = scoreMin
	}

	prefix, err := EncodeHashKey(name, nil)
	if err != nil {
		r.State = err.Error()
		return r
	}

	var seekKey []byte
	if isStartEmpty {
		// 定位到当前 name 在 scoreMax 下最大的 key
		seekKey, err = EncodeZsetScoreKey(name, bytes.Repeat([]byte{0xFF}, 255), scoreMax)
	} else {
		seekKey, err = EncodeZsetScoreKey(name, keyStart, scoreStart)
	}
	if err != nil {
		r.State = err.Error()
		return r
	}

	c := b.Cursor()
	k, _ := c.Seek(seekKey)
	if k == nil {
		k, _ = c.Last()
	}

	n := 0
	for k != nil {
		// 1. 下界检查：游标移出当前 name 的范围，立即退出
		if bytes.Compare(k, prefix) < 0 {
			break
		}

		// 2. 落在当前 name 范围的数据处理
		if bytes.HasPrefix(k, prefix) {
			_, key, score, err := DecodeZsetScoreKey(k)
			if err != nil {
				k, _ = c.Prev()
				continue
			}

			// 检查 score 下限
			if score < scoreEnd {
				break
			}

			// 过滤大于等于起始位置的数据（非全表起点时）
			if !isStartEmpty && bytes.Compare(k, seekKey) >= 0 {
				k, _ = c.Prev()
				continue
			}

			r.Data = append(r.Data, key, I2b(score))
			n++
			if n == limit {
				break
			}
		}

		k, _ = c.Prev()
	}

	r.State = replyOK
	return r
}

// -----------
// Reply 辅助函数
// -----------

func (r *Reply) OK() bool {
	return r.State == replyOK
}

func (r *Reply) NotFound() bool {
	return r.State == replyNotFound
}

// Clone 深度拷贝 Reply 中的 mmap 数据。
// 重要：如果在 bolt.Tx 事务闭包外使用 Reply 数据，必须调用此方法以防止段错误。
func (r *Reply) Clone() *Reply {
	if len(r.Data) == 0 {
		return r
	}
	newReply := &Reply{
		State: r.State,
		Data:  make([]BS, len(r.Data)),
	}
	for i, b := range r.Data {
		copied := make([]byte, len(b))
		copy(copied, b)
		newReply.Data[i] = copied
	}
	return newReply
}

func (r *Reply) Bytes() []byte {
	if len(r.Data) > 0 {
		return r.Data[0]
	}
	return nil
}

// String is a convenience wrapper over Get for string value.
func (r *Reply) String() string {
	if len(r.Data) > 0 {
		return B2s(r.Data[0])
	}
	return ""
}

// Int is a convenience wrapper over Get for int value of a hashmap.
func (r *Reply) Int() int {
	return int(r.Uint64())
}

// Int64 is a convenience wrapper over Get for int64 value of a hashmap.
func (r *Reply) Int64() int64 {
	if len(r.Data) < 1 {
		return 0
	}
	return int64(r.Uint64())
}

// Uint is a convenience wrapper over Get for uint value of a hashmap.
func (r *Reply) Uint() uint {
	return uint(r.Uint64())
}

// Uint64 is a convenience wrapper over Get for uint64 value of a hashmap.
func (r *Reply) Uint64() uint64 {
	if len(r.Data) < 1 {
		return 0
	}
	if len(r.Data[0]) < uint64EncodedLen {
		return 0
	}
	return binary.BigEndian.Uint64(r.Data[0])
}

// List retrieves the key/value pairs from reply of a hashmap.
func (r *Reply) List() []Entry {
	if len(r.Data) < 1 {
		return []Entry{}
	}
	list := make([]Entry, len(r.Data)/2)
	j := 0
	for i := 0; i < (len(r.Data) - 1); i += 2 {
		list[j] = Entry{r.Data[i], r.Data[i+1]}
		j++
	}
	return list
}

// Dict retrieves the key/value pairs from reply of a hashmap.
func (r *Reply) Dict() map[string][]byte {
	if len(r.Data) < 1 {
		return map[string][]byte{}
	}
	dict := make(map[string][]byte, len(r.Data)/2)
	for i := 0; i < (len(r.Data) - 1); i += 2 {
		dict[B2s(r.Data[i])] = r.Data[i+1]
	}
	return dict
}

func (r *Reply) KvLen() int {
	return len(r.Data) / 2
}

func (r *Reply) KvEach(fn func(key, value BS)) int {
	for i := 0; i < (len(r.Data) - 1); i += 2 {
		fn(r.Data[i], r.Data[i+1])
	}
	return r.KvLen()
}

// JSON parses the JSON-encoded Reply Entry value and stores the result
// in the value pointed to by v.
func (r *Reply) JSON(v interface{}) error {
	if r == nil || len(r.Data) == 0 {
		return errors.New("empty reply")
	}
	if v == nil {
		return errors.New("nil json destination")
	}
	return json.Unmarshal(r.Data[0], v)
}

// -----------
// BS 辅助函数
// -----------

func (b BS) Bytes() []byte {
	return b
}

func (b BS) String() string {
	return B2s(b)
}

// Int is a convenience wrapper over Get for int value of a hashmap.
func (b BS) Int() int {
	return int(b.Uint64())
}

// Int64 is a convenience wrapper over Get for int64 value of a hashmap.
func (b BS) Int64() int64 {
	return int64(b.Uint64())
}

// Uint is a convenience wrapper over Get for uint value of a hashmap.
func (b BS) Uint() uint {
	return uint(b.Uint64())
}

// Uint64 is a convenience wrapper over Get for uint64 value of a hashmap.
func (b BS) Uint64() uint64 {
	if len(b) < uint64EncodedLen {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// JSON parses the JSON-encoded Reply Entry value and stores the result
// in the value pointed to by v.
func (b BS) JSON(v interface{}) error {
	if v == nil {
		return errors.New("nil json destination")
	}
	return json.Unmarshal(b, v)
}

// -----------------------
// 辅助函数
// -----------------------

// EncodeHashKey 编码 Hash 数据 Key: [NameLen (1B)][Name][Key]
func EncodeHashKey(name string, key []byte) ([]byte, error) {
	if len(name) > 255 {
		return nil, ErrNameTooLong
	}
	if len(key) > 255 {
		return nil, ErrKeyTooLong
	}

	buf := make([]byte, 1+len(name)+len(key))
	buf[0] = byte(len(name))
	copy(buf[1:], name)
	copy(buf[1+len(name):], key)
	return buf, nil
}

// DecodeHashKey 解码 Hash 数据 Key
func DecodeHashKey(buf []byte) (name string, key []byte, err error) {
	// 1. 至少需要 1 字节读取 NameLen
	if len(buf) < 1 {
		return "", nil, ErrBufferTooShort
	}

	// 2. 解析 Name 长度
	nameLen := int(buf[0])

	// 3. 校验 Buffer 长度是否足够容纳 Name
	if len(buf) < 1+nameLen {
		return "", nil, ErrBufferTooShort
	}

	// 4. 提取 Name 和 Key
	name = string(buf[1 : 1+nameLen])
	key = buf[1+nameLen:]

	// 5. 校验提取出的 Key 是否符合约定（防止传入非法损坏的数据）
	if len(key) > 255 {
		return "", nil, ErrKeyTooLong
	}

	return name, key, nil
}

// EncodeZsetScoreKey 编码 Zet 数据 Key: [NameLen (1B)][Name][EncodedScore (8B)][Key]
func EncodeZsetScoreKey(name string, key []byte, score uint64) ([]byte, error) {
	if len(name) > 255 {
		return nil, ErrNameTooLong
	}
	if len(key) > 255 {
		return nil, ErrKeyTooLong
	}

	buf := make([]byte, 1+len(name)+uint64EncodedLen+len(key))
	buf[0] = byte(len(name))
	copy(buf[1:], name)

	// 优化：直接在 buf 上进行 BigEndian 编码，省去 I2b 的内存分配和 copy
	binary.BigEndian.PutUint64(buf[1+len(name):], score)

	copy(buf[1+len(name)+uint64EncodedLen:], key)
	return buf, nil
}

// DecodeZsetScoreKey 解码 Zet 数据 Key
func DecodeZsetScoreKey(buf []byte) (name string, key []byte, score uint64, err error) {
	// 基础长度校验：至少需要 1B(NameLen) + 8B(Score) = 9 字节
	if len(buf) < 1+uint64EncodedLen {
		return "", nil, 0, ErrInvalidBuf
	}

	nameLen := int(buf[0])

	// 严格长度校验：buffer 总长度必须 >= 1 + nameLen + 8
	if len(buf) < 1+nameLen+uint64EncodedLen {
		return "", nil, 0, ErrInvalidBuf
	}

	name = string(buf[1 : 1+nameLen])

	// 读取 Score
	scoreIndex := 1 + nameLen
	score = binary.BigEndian.Uint64(buf[scoreIndex : scoreIndex+uint64EncodedLen])

	// 读取 Key
	keyIndex := scoreIndex + uint64EncodedLen
	// 拷贝 key 以防止与原 buf 共享底层数组，避免潜在的内存泄漏
	key = make([]byte, len(buf)-keyIndex)
	copy(key, buf[keyIndex:])

	return name, key, score, nil
}

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

// I2b returns an 8-byte big endian representation of v
// v uint64(123456) -> 8-byte big endian.
func I2b(v uint64) []byte {
	b := make([]byte, uint64EncodedLen)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// B2i return an int64 of v
// v (8-byte big endian) -> uint64(123456).
func B2i(v []byte) uint64 {
	if len(v) < uint64EncodedLen {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

// B2ds return a Digit string of v
// v (8-byte big endian) -> uint64(123456) -> "123456".
func B2ds(v []byte) string {
	return strconv.FormatUint(binary.BigEndian.Uint64(v), 10)
}
