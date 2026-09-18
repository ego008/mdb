package mdb

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

// helperOpenDB 创建临时测试数据库并在测试结束时自动清理
func helperOpenDB(t *testing.T) *DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open database failed: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

// -----------------------------------------------------------------------------
// Key 编解码测试
// -----------------------------------------------------------------------------

func TestEncodeDecodeHashKey_Success(t *testing.T) {
	tests := []struct {
		name     string
		key      []byte
		hashName string
	}{
		{hashName: "user", key: []byte("1001"), name: "normal string and key"},
		{hashName: "a", key: []byte("b"), name: "single char"},
		{hashName: "", key: []byte(""), name: "empty name and empty key"},
		{hashName: strings.Repeat("x", 255), key: bytes.Repeat([]byte("y"), 255), name: "max length 255 bytes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf, err := EncodeHashKey(tt.hashName, tt.key)
			if err != nil {
				t.Fatalf("EncodeHashKey unexpected error: %v", err)
			}

			gotName, gotKey, err := DecodeHashKey(buf)
			if err != nil {
				t.Fatalf("DecodeHashKey unexpected error: %v", err)
			}

			if gotName != tt.hashName {
				t.Errorf("DecodeName mismatch: got %q, want %q", gotName, tt.hashName)
			}
			if !bytes.Equal(gotKey, tt.key) {
				t.Errorf("DecodeKey mismatch: got %q, want %q", gotKey, tt.key)
			}
		})
	}
}

func TestEncodeHashKey_LengthExceeded(t *testing.T) {
	t.Run("Name length > 255", func(t *testing.T) {
		longName := strings.Repeat("a", 256)
		_, err := EncodeHashKey(longName, []byte("key"))
		if !errors.Is(err, ErrNameTooLong) {
			t.Errorf("expected ErrNameTooLong, got %v", err)
		}
	})

	t.Run("Key length > 255", func(t *testing.T) {
		longKey := bytes.Repeat([]byte("k"), 256)
		_, err := EncodeHashKey("name", longKey)
		if !errors.Is(err, ErrKeyTooLong) {
			t.Errorf("expected ErrKeyTooLong, got %v", err)
		}
	})
}

func TestDecodeHashKey_InvalidBuffer(t *testing.T) {
	t.Run("Empty buffer", func(t *testing.T) {
		_, _, err := DecodeHashKey([]byte{})
		if !errors.Is(err, ErrBufferTooShort) {
			t.Errorf("expected ErrBufferTooShort, got %v", err)
		}
	})

	t.Run("Buffer shorter than declared NameLen", func(t *testing.T) {
		// 声明 nameLen = 10，但整体长度只有 3 字节
		buf := []byte{10, 'a', 'b'}
		_, _, err := DecodeHashKey(buf)
		if !errors.Is(err, ErrBufferTooShort) {
			t.Errorf("expected ErrBufferTooShort, got %v", err)
		}
	})

	t.Run("Key exceeds 255 bytes on decode", func(t *testing.T) {
		// NameLen = 1 ('a')，后跟 256 字节的 Key
		buf := make([]byte, 1+1+256)
		buf[0] = 1
		buf[1] = 'a'
		_, _, err := DecodeHashKey(buf)
		if !errors.Is(err, ErrKeyTooLong) {
			t.Errorf("expected ErrKeyTooLong, got %v", err)
		}
	})
}

func TestEncodeAndDecodeZSetScoreKey(t *testing.T) {
	tests := []struct {
		name    string
		zName   string
		key     []byte
		score   uint64
		wantErr error
	}{
		{
			name:    "Normal case",
			zName:   "my_zset",
			key:     []byte("member1"),
			score:   123456,
			wantErr: nil,
		},
		{
			name:    "Empty key",
			zName:   "my_zset",
			key:     []byte(""),
			score:   0,
			wantErr: nil,
		},
		{
			name:    "Empty name",
			zName:   "",
			key:     []byte("member2"),
			score:   999999999999,
			wantErr: nil,
		},
		{
			name:    "Max uint64 score",
			zName:   "max_score_set",
			key:     []byte("max_member"),
			score:   ^uint64(0), // 18446744073709551615
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 1. 测试编码
			encodedBuf, err := EncodeZsetScoreKey(tt.zName, tt.key, tt.score)
			if err != tt.wantErr {
				t.Fatalf("EncodeZSetScoreKey() error = %v, wantErr %v", err, tt.wantErr)
			}

			// 2. 测试解码
			decName, decKey, decScore, decErr := DecodeZsetScoreKey(encodedBuf)
			if decErr != nil {
				t.Fatalf("DecodeZSetScoreKey() unexpected error = %v", decErr)
			}

			// 3. 校验数据一致性
			if decName != tt.zName {
				t.Errorf("Name mismatch: got %v, want %v", decName, tt.zName)
			}
			if decScore != tt.score {
				t.Errorf("Score mismatch: got %v, want %v", decScore, tt.score)
			}
			if !bytes.Equal(decKey, tt.key) {
				t.Errorf("Key mismatch: got %v, want %v", decKey, tt.key)
			}
		})
	}
}

func TestDecodeInvalidBuffer(t *testing.T) {
	invalidBufs := [][]byte{
		nil,
		{},
		{0},                       // 只有 nameLen
		{5, 'a', 'b'},             // nameLen=5，但总长度不够
		{2, 'a', 'b', 0, 0, 0, 0}, // score 部分不足 8 字节
	}

	for i, buf := range invalidBufs {
		_, _, _, err := DecodeZsetScoreKey(buf)
		if err != ErrInvalidBuf {
			t.Errorf("Test case %d expected ErrInvalidBuf, got %v", i, err)
		}
	}
}

// -----------------------------------------------------------------------------
// 数据库 Open / Close / View / Update 测试
// -----------------------------------------------------------------------------

func TestDB_Open_Errors(t *testing.T) {
	t.Run("Empty path", func(t *testing.T) {
		_, err := Open("   ")
		if err == nil {
			t.Error("expected error when opening empty path, got nil")
		}
	})
}

func TestDB_NilSafety(t *testing.T) {
	var nilDB *DB

	t.Run("Nil View", func(t *testing.T) {
		err := nilDB.View(func(tx *bbolt.Tx) error { return nil })
		if err == nil {
			t.Error("expected error on nil db View")
		}
	})

	t.Run("Nil Update", func(t *testing.T) {
		err := nilDB.Update(func(tx *bbolt.Tx) error { return nil })
		if err == nil {
			t.Error("expected error on nil db Update")
		}
	})

	db := helperOpenDB(t)
	t.Run("Nil callback View", func(t *testing.T) {
		err := db.View(nil)
		if err == nil {
			t.Error("expected error on nil callback View")
		}
	})

	t.Run("Nil callback Update", func(t *testing.T) {
		err := db.Update(nil)
		if err == nil {
			t.Error("expected error on nil callback Update")
		}
	})
}

func TestDB_Close_WaitGroup(t *testing.T) {
	db := helperOpenDB(t)

	var wg sync.WaitGroup
	wg.Add(1)

	// 模拟长耗时事务，测试 Close 是否会阻塞等待事务完成
	go func() {
		_ = db.View(func(tx *bbolt.Tx) error {
			wg.Done()
			time.Sleep(50 * time.Millisecond)
			return nil
		})
	}()

	wg.Wait() // 确保 Goroutine 已进入 View 内部
	start := time.Now()
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close failed: %v", err)
	}

	if time.Since(start) < 40*time.Millisecond {
		t.Error("db.Close returned too early, did not wait for active transaction")
	}
}

// -----------------------------------------------------------------------------
// HSet 和 HGet 测试
// -----------------------------------------------------------------------------

func TestDB_HSet_HGet(t *testing.T) {
	db := helperOpenDB(t)

	hashName := "user_info"
	fieldKey := []byte("email")
	fieldVal := []byte("foo@example.com")

	// 1. 写入数据
	err := db.Update(func(tx *bbolt.Tx) error {
		return db.HSet(tx, hashName, fieldKey, fieldVal)
	})
	if err != nil {
		t.Fatalf("HSet failed: %v", err)
	}

	// 2. 读取数据并验证
	err = db.View(func(tx *bbolt.Tx) error {
		r := db.HGet(tx, hashName, fieldKey)
		if !r.OK() {
			return nil
		}
		if !bytes.Equal(r.Bytes(), fieldVal) {
			t.Errorf("HGet mismatch: got %s, want %s", r.Bytes(), fieldVal)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}

	// 3. 读取不存在的 key
	err = db.View(func(tx *bbolt.Tx) error {
		r := db.HGet(tx, hashName, []byte("not_exist"))
		if r.OK() {
			t.Errorf("expected nil for non-existent key, got %v", r.OK())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}

	// 4. 覆盖写入
	newVal := []byte("bar@example.com")
	err = db.Update(func(tx *bbolt.Tx) error {
		return db.HSet(tx, hashName, fieldKey, newVal)
	})
	if err != nil {
		t.Fatalf("HSet update failed: %v", err)
	}

	err = db.View(func(tx *bbolt.Tx) error {
		r := db.HGet(tx, hashName, fieldKey)
		if !bytes.Equal(r.Bytes(), newVal) {
			t.Errorf("HGet updated value mismatch: got %s, want %s", r.Bytes(), newVal)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}

	// 5. 写入 key 为 nil 数据
	err = db.Update(func(tx *bbolt.Tx) error {
		return db.HSet(tx, hashName, fieldKey, nil)
	})
	if err != nil {
		t.Fatalf("HSet failed: %v", err)
	}

	// 6. 读取数据并验证
	err = db.View(func(tx *bbolt.Tx) error {
		r := db.HGet(tx, hashName, fieldKey)
		if !r.OK() {
			t.Errorf("HGet mismatch: got %t, want %t", r.OK(), true)
		}
		if !bytes.Equal(r.Bytes(), nil) {
			t.Errorf("HGet mismatch: got %s, want %v", r.Bytes(), nil)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}
}

func TestDB_HScan(t *testing.T) {
	db := helperOpenDB(t)

	hashName := "user_info"
	kvs := [][]byte{
		[]byte("a"), []byte("av"),
		[]byte("b"), []byte("bv"),
		[]byte("c"), []byte("cv"),
		[]byte("d"), []byte("dv"),
		[]byte("e"), []byte("ev"),
		[]byte("f"), []byte("fv"),
		[]byte("g"), []byte("gv"),
		[]byte("h"), []byte("hv"),
	}

	// 1. 写入数据
	err := db.Update(func(tx *bbolt.Tx) error {
		// 前后加
		db.HMSet(tx, "user_infi", kvs...)
		db.HMSet(tx, "user_infq", kvs...)
		return db.HMSet(tx, hashName, kvs...)
	})
	if err != nil {
		t.Fatalf("HMSet failed: %v", err)
	}

	// 2. 读取数据并验证
	err = db.View(func(tx *bbolt.Tx) error {
		for i := 0; i < len(kvs)-1; i += 2 {
			r := db.HGet(tx, hashName, kvs[i])
			if !r.OK() {
				return nil
			}
			if !bytes.Equal(r.Bytes(), kvs[i+1]) {
				t.Errorf("HGet mismatch: got %s, want %s", r.Bytes(), kvs[i+1])
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}

	// 3. HScan
	err = db.View(func(tx *bbolt.Tx) error {
		r := db.HScan(tx, hashName, nil, 100)
		if !r.OK() {
			t.Errorf("HScan mismatch: got %t, want %t", r.OK(), true)
		}
		if r.KvLen() != len(kvs)/2 {
			t.Errorf("HScan mismatch: got %d, want %d", r.KvLen(), len(kvs)/2)
		}
		i := 0
		r.KvEach(func(key, value BS) {
			t.Logf("HScan key=%q value=%q\n", key, value)
			if !bytes.Equal(key, kvs[i]) || !bytes.Equal(value, kvs[i+1]) {
				t.Errorf("HScan mismatch: got %s, want %s", value, kvs[i+1])
			}
			i += 2
		})

		//
		r = db.HScan(tx, hashName, kvs[4], 1)
		t.Logf("HScan key=%q value=%q\n", r.Data[0], r.Data[1])
		if r.KvLen() != 1 {
			t.Errorf("HScan mismatch: got %d, want %d", r.KvLen(), 1)
		}
		if !bytes.Equal(r.Data[0], kvs[6]) || !bytes.Equal(r.Data[1], kvs[7]) {
			t.Errorf("HScan mismatch: got %s => %s, want %s => %s", r.Data[0], r.Data[1], kvs[6], kvs[7])
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}

}

func TestDB_HRScan(t *testing.T) {
	db := helperOpenDB(t)

	hashName := "user_info"
	kvs := [][]byte{
		[]byte("a"), []byte("av"),
		[]byte("b"), []byte("bv"),
		[]byte("c"), []byte("cv"),
		[]byte("d"), []byte("dv"),
		[]byte("e"), []byte("ev"),
		[]byte("f"), []byte("fv"),
		[]byte("g"), []byte("gv"),
		[]byte("h"), []byte("hv"),
	}

	// 1. 写入数据（同时在 user_info 前后分别写入 user_infi 和 user_infq，测试前缀隔离）
	err := db.Update(func(tx *bbolt.Tx) error {
		_ = db.HMSet(tx, "user_infi", kvs...)
		_ = db.HMSet(tx, "user_infq", kvs...)
		return db.HMSet(tx, hashName, kvs...)
	})
	if err != nil {
		t.Fatalf("HMSet failed: %v", err)
	}

	err = db.View(func(tx *bbolt.Tx) error {
		// 测试1：全表逆序扫描 (keyStart == nil)
		// 期望返回顺序: h, g, f, e, d, c, b, a
		r := db.HRScan(tx, hashName, nil, 100)
		if !r.OK() {
			t.Fatalf("HRScan failed, state: %s", r.State)
		}
		if r.KvLen() != len(kvs)/2 {
			t.Errorf("HRScan length mismatch: got %d, want %d", r.KvLen(), len(kvs)/2)
		}

		expectedIdx := len(kvs) - 2 // 从最后一个 Key 'h' 开始
		r.KvEach(func(key, value BS) {
			if !bytes.Equal(key, kvs[expectedIdx]) || !bytes.Equal(value, kvs[expectedIdx+1]) {
				t.Errorf("HRScan content mismatch: got key=%s val=%s, want key=%s val=%s",
					key, value, kvs[expectedIdx], kvs[expectedIdx+1])
			}
			expectedIdx -= 2
		})

		// 测试2：指定存在的 keyStart ("e") 逆序扫描 limit=1，预期获取 "d"
		r = db.HRScan(tx, hashName, []byte("e"), 1)
		if !r.OK() {
			t.Fatalf("HRScan failed, state: %s", r.State)
		}
		if r.KvLen() != 1 {
			t.Fatalf("HRScan limit mismatch: got %d, want %d", r.KvLen(), 1)
		}
		if !bytes.Equal(r.Data[0], []byte("d")) || !bytes.Equal(r.Data[1], []byte("dv")) {
			t.Errorf("HRScan value mismatch: got key=%s val=%s, want key=d val=dv", r.Data[0], r.Data[1])
		}

		// 测试3：指定不存在的 keyStart ("e1") 逆序扫描 limit=3，预期获取 "e", "d", "c"
		r = db.HRScan(tx, hashName, []byte("e1"), 3)
		if !r.OK() || r.KvLen() != 3 {
			t.Fatalf("HRScan mismatch for 'e1': got len=%d, want 3", r.KvLen())
		}
		if !bytes.Equal(r.Data[0], []byte("e")) || !bytes.Equal(r.Data[2], []byte("d")) || !bytes.Equal(r.Data[4], []byte("c")) {
			t.Errorf("HRScan result mismatch for keyStart 'e1': got [%s, %s, %s], want [e, d, c]",
				r.Data[0], r.Data[2], r.Data[4])
		}

		// 测试4：当 keyStart 为最小 key ("a") 时，没有比 "a" 更小的 Key，应返回空列表
		r = db.HRScan(tx, hashName, []byte("a"), 10)
		if !r.OK() || r.KvLen() != 0 {
			t.Errorf("HRScan for min key mismatch: got len=%d, want 0", r.KvLen())
		}

		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}
}

func TestDB_HSet_HGet_Errors(t *testing.T) {
	db := helperOpenDB(t)

	t.Run("Name length limit in HSet/HGet", func(t *testing.T) {
		longName := strings.Repeat("n", 256)
		err := db.Update(func(tx *bbolt.Tx) error {
			return db.HSet(tx, longName, []byte("key"), []byte("val"))
		})
		if !errors.Is(err, ErrNameTooLong) {
			t.Errorf("expected ErrNameTooLong, got %v", err)
		}

	})

	t.Run("Nil Bucket Handling", func(t *testing.T) {
		// 删掉 bucketHash 模拟极端受损场景
		err := db.Update(func(tx *bbolt.Tx) error {
			return tx.DeleteBucket(bucketHash)
		})
		if err != nil {
			t.Fatalf("DeleteBucket failed: %v", err)
		}

		err = db.Update(func(tx *bbolt.Tx) error {
			return db.HSet(tx, "name", []byte("key"), []byte("val"))
		})
		if !errors.Is(err, ErrNilBucket) {
			t.Errorf("expected ErrNilBucket on HSet, got %v", err)
		}
	})
}

// -----------------------------------------------------------------------------
// ZSet 和 ZGet 测试
// -----------------------------------------------------------------------------

func TestDB_ZSet_ZGet(t *testing.T) {
	db := helperOpenDB(t)

	hashName := "user_info"
	fieldKey := []byte("email")
	fieldVal := uint64(69)

	// 1. 写入数据
	err := db.Update(func(tx *bbolt.Tx) error {
		return db.ZSet(tx, hashName, fieldKey, fieldVal)
	})
	if err != nil {
		t.Fatalf("ZSet failed: %v", err)
	}

	// 2. 读取数据并验证
	err = db.View(func(tx *bbolt.Tx) error {
		r := db.ZGet(tx, hashName, fieldKey)
		if !r.OK() {
			return nil
		}
		if r.Uint64() != fieldVal {
			t.Errorf("HGet mismatch: got %d, want %d", r.Uint64(), fieldVal)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}

	// 3. 读取不存在的 key
	err = db.View(func(tx *bbolt.Tx) error {
		r := db.ZGet(tx, hashName, []byte("not_exist"))
		if r.OK() {
			t.Errorf("expected nil for non-existent key, got %v", r.OK())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}

	// 4. 覆盖写入
	newVal := uint64(99)
	err = db.Update(func(tx *bbolt.Tx) error {
		return db.ZSet(tx, hashName, fieldKey, newVal)
	})
	if err != nil {
		t.Fatalf("HSet update failed: %v", err)
	}

	err = db.View(func(tx *bbolt.Tx) error {
		r := db.ZGet(tx, hashName, fieldKey)
		if r.Uint64() != newVal {
			t.Errorf("HGet updated value mismatch: got %d, want %d", r.Uint64(), newVal)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}

	// 5. 写入 key 为 0 数据
	err = db.Update(func(tx *bbolt.Tx) error {
		return db.ZSet(tx, hashName, fieldKey, 0)
	})
	if err != nil {
		t.Fatalf("ZSet failed: %v", err)
	}

	// 6. 读取数据并验证
	err = db.View(func(tx *bbolt.Tx) error {
		r := db.ZGet(tx, hashName, fieldKey)
		if !r.OK() {
			t.Errorf("ZGet mismatch: got %t, want %t", r.OK(), true)
		}
		if r.Uint64() != 0 {
			t.Errorf("ZGet mismatch: got %d, want %d", r.Uint64(), 0)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}
}

func TestDB_ZMSet_ZScan_ZRScan(t *testing.T) {
	db := helperOpenDB(t)

	zName := "user_scores"
	kvs := [][]byte{
		[]byte("user_a"), I2b(10),
		[]byte("user_b"), I2b(20),
		[]byte("user_c"), I2b(20), // 相同 score，验证字典序
		[]byte("user_d"), I2b(30),
		[]byte("user_e"), []byte("40"), // 验证字符串数字兼容性
	}

	// 1. 测试 ZMSet 写入
	err := db.Update(func(tx *bbolt.Tx) error {
		// 在前后写入相邻集合名称，测试隔离性
		_ = db.ZMSet(tx, "user_scorea", kvs...)
		_ = db.ZMSet(tx, "user_scores_z", kvs...)
		return db.ZMSet(tx, zName, kvs...)
	})
	if err != nil {
		t.Fatalf("ZMSet failed: %v", err)
	}

	// 2. 验证写入结果
	err = db.View(func(tx *bbolt.Tx) error {
		r := db.ZGet(tx, zName, []byte("user_e"))
		if !r.OK() || r.Uint64() != 40 {
			t.Errorf("ZGet mismatch: got %d, want 40", r.Uint64())
		}

		// 测试错误传参：奇数个参数
		err := db.Update(func(tx *bbolt.Tx) error {
			return db.ZMSet(tx, zName, []byte("only_key"))
		})
		if !errors.Is(err, ErrKeyValuePairLen) {
			t.Errorf("expected ErrKeyValuePairLen, got %v", err)
		}

		// 3. 测试 ZScan 正序扫描 (Score 升序 + Key 字典序升序)
		// 全表扫描，期望顺序：user_a(10) -> user_b(20) -> user_c(20) -> user_d(30) -> user_e(40)
		r = db.ZScan(tx, zName, nil, 0, 0, 100)
		if !r.OK() || r.KvLen() != 5 {
			t.Fatalf("ZScan full scan failed: got len=%d, want 5", r.KvLen())
		}
		expectedKeys := []string{"user_a", "user_b", "user_c", "user_d", "user_e"}
		expectedScores := []uint64{10, 20, 20, 30, 40}
		idx := 0
		r.KvEach(func(key, score BS) {
			if key.String() != expectedKeys[idx] || score.Uint64() != expectedScores[idx] {
				t.Errorf("ZScan mismatch at %d: got %s:%d, want %s:%d",
					idx, key.String(), score.Uint64(), expectedKeys[idx], expectedScores[idx])
			}
			idx++
		})

		// 指定起始点 user_b(20)，期望结果：user_c(20) -> user_d(30)
		r = db.ZScan(tx, zName, []byte("user_b"), 20, 30, 10)
		if !r.OK() || r.KvLen() != 2 {
			t.Fatalf("ZScan seek test failed: got len=%d, want 2", r.KvLen())
		}
		if r.Data[0].String() != "user_c" || r.Data[2].String() != "user_d" {
			t.Errorf("ZScan range mismatch: got [%s, %s], want [user_c, user_d]", r.Data[0], r.Data[2])
		}

		// 4. 测试 ZRScan 逆序扫描 (Score 降序 + Key 字典序降序)
		// 全表逆序扫描，期望顺序：user_e(40) -> user_d(30) -> user_c(20) -> user_b(20) -> user_a(10)
		r = db.ZRScan(tx, zName, nil, 0, 0, 100)
		if !r.OK() || r.KvLen() != 5 {
			t.Fatalf("ZRScan full scan failed: got len=%d, want 5", r.KvLen())
		}
		idx = 4
		r.KvEach(func(key, score BS) {
			if key.String() != expectedKeys[idx] || score.Uint64() != expectedScores[idx] {
				t.Errorf("ZRScan mismatch at index %d: got %s:%d, want %s:%d",
					idx, key.String(), score.Uint64(), expectedKeys[idx], expectedScores[idx])
			}
			idx--
		})

		// 指定起始点 user_d(30)，期望结果：user_c(20)
		r = db.ZRScan(tx, zName, []byte("user_d"), 30, 0, 1)
		if !r.OK() || r.KvLen() != 1 {
			t.Fatalf("ZRScan seek test failed: got len=%d, want 1", r.KvLen())
		}
		if r.Data[0].String() != "user_c" || r.Data[1].Uint64() != 20 {
			t.Errorf("ZRScan seek mismatch: got %s:%d, want user_c:20", r.Data[0].String(), r.Data[1].Uint64())
		}

		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}
}

func TestDB_Hincr_Zincr(t *testing.T) {
	db := helperOpenDB(t)

	// -------------------------------------------------------------------------
	// 1. Hincr 测试
	// -------------------------------------------------------------------------
	t.Run("Hincr Operations", func(t *testing.T) {
		hashName := "user_counter"
		field := []byte("views")

		err := db.Update(func(tx *bbolt.Tx) error {
			// 1.1 不存在 key 时自增
			val, err := db.Hincr(tx, hashName, field, 10)
			if err != nil || val != 10 {
				t.Fatalf("Hincr initial step failed: got val=%d, err=%v", val, err)
			}

			// 1.2 再次自增
			val, err = db.Hincr(tx, hashName, field, 5)
			if err != nil || val != 15 {
				t.Fatalf("Hincr increment failed: got val=%d, err=%v", val, err)
			}

			// 1.3 负数步长 (自减)
			val, err = db.Hincr(tx, hashName, field, -3)
			if err != nil || val != 12 {
				t.Fatalf("Hincr decrement failed: got val=%d, err=%v", val, err)
			}

			// 1.4 HGet 验证值正确
			r := db.HGet(tx, hashName, field)
			if !r.OK() || r.Uint64() != 12 {
				t.Fatalf("HGet verification failed: got %d, want 12", r.Uint64())
			}

			// 1.5 测试对字符串数字进行 Hincr
			strField := []byte("str_views")
			_ = db.HSet(tx, hashName, strField, []byte("100"))
			val, err = db.Hincr(tx, hashName, strField, 20)
			if err != nil || val != 120 {
				t.Fatalf("Hincr on string number failed: got val=%d, err=%v", val, err)
			}

			return nil
		})
		if err != nil {
			t.Fatalf("Hincr test transaction failed: %v", err)
		}
	})

	// -------------------------------------------------------------------------
	// 2. Zincr 测试
	// -------------------------------------------------------------------------
	t.Run("Zincr Operations", func(t *testing.T) {
		zName := "leaderboard"
		playerA := []byte("player_a")
		playerB := []byte("player_b")

		err := db.Update(func(tx *bbolt.Tx) error {
			// 2.1 新成员 Zincr
			score, err := db.Zincr(tx, zName, playerA, 100)
			if err != nil || score != 100 {
				t.Fatalf("Zincr initial failed: got score=%d, err=%v", score, err)
			}

			_ = db.ZSet(tx, zName, playerB, 150)

			// 2.2 增加 Score 使 playerA 超越 playerB
			score, err = db.Zincr(tx, zName, playerA, 100) // score 变为 200
			if err != nil || score != 200 {
				t.Fatalf("Zincr increment failed: got score=%d, err=%v", score, err)
			}

			// 2.3 验证 ZGet
			r := db.ZGet(tx, zName, playerA)
			if !r.OK() || r.Uint64() != 200 {
				t.Fatalf("ZGet verification failed: got %d, want 200", r.Uint64())
			}

			// 2.4 验证 ZScan 顺序（playerB(150) -> playerA(200)）
			rScan := db.ZScan(tx, zName, nil, 0, 0, 10)
			if !rScan.OK() || rScan.KvLen() != 2 {
				t.Fatalf("ZScan count mismatch: got len=%d, want 2", rScan.KvLen())
			}
			if rScan.Data[0].String() != "player_b" || rScan.Data[2].String() != "player_a" {
				t.Fatalf("ZScan ranking mismatch: got [%s, %s], want [player_b, player_a]",
					rScan.Data[0].String(), rScan.Data[2].String())
			}

			// 2.5 负数步长自减 (playerA score 200 - 80 = 120，排名降到 playerB 之后)
			score, err = db.Zincr(tx, zName, playerA, -80)
			if err != nil || score != 120 {
				t.Fatalf("Zincr decrement failed: got score=%d, err=%v", score, err)
			}

			// 2.6 再次验证 ZScan 顺序（playerA(120) -> playerB(150)）
			rScan = db.ZScan(tx, zName, nil, 0, 0, 10)
			if rScan.Data[0].String() != "player_a" || rScan.Data[2].String() != "player_b" {
				t.Fatalf("ZScan ranking update failed: got [%s, %s], want [player_a, player_b]",
					rScan.Data[0].String(), rScan.Data[2].String())
			}

			return nil
		})
		if err != nil {
			t.Fatalf("Zincr test transaction failed: %v", err)
		}
	})
}

func TestDB_HMGet_ZMGet(t *testing.T) {
	db := helperOpenDB(t)

	// -------------------------------------------------------------------------
	// 1. HMGet 测试
	// -------------------------------------------------------------------------
	t.Run("HMGet Operations", func(t *testing.T) {
		hashName := "user_info"
		kvs := [][]byte{
			[]byte("name"), []byte("alice"),
			[]byte("age"), []byte("18"),
			[]byte("city"), []byte("beijing"),
		}

		err := db.Update(func(tx *bbolt.Tx) error {
			return db.HMSet(tx, hashName, kvs...)
		})
		if err != nil {
			t.Fatalf("HMSet failed: %v", err)
		}

		err = db.View(func(tx *bbolt.Tx) error {
			// 1.1 获取存在的多个 key
			queryKeys := [][]byte{[]byte("name"), []byte("city")}
			r := db.HMGet(tx, hashName, queryKeys)
			if !r.OK() || r.KvLen() != 2 {
				t.Fatalf("HMGet failed: got len=%d, want 2", r.KvLen())
			}

			dict := r.Dict()
			if string(dict["name"]) != "alice" || string(dict["city"]) != "beijing" {
				t.Errorf("HMGet dict mismatch: %v", dict)
			}

			// 1.2 包含不存在的 key
			queryKeysWithNotExist := [][]byte{[]byte("name"), []byte("not_exist"), []byte("age")}
			r = db.HMGet(tx, hashName, queryKeysWithNotExist)
			if !r.OK() || r.KvLen() != 3 {
				t.Fatalf("HMGet mixed failed: got len=%d, want 3", r.KvLen())
			}

			// 验证顺序与位置对应关系 [k1, v1, k2, v2, k3, v3]
			if !bytes.Equal(r.Data[0], []byte("name")) || string(r.Data[1]) != "alice" {
				t.Errorf("HMGet key1 mismatch: got %s => %s", r.Data[0], r.Data[1])
			}
			if !bytes.Equal(r.Data[2], []byte("not_exist")) || r.Data[3] != nil {
				t.Errorf("HMGet key2 (not exist) mismatch: got %s => %v", r.Data[2], r.Data[3])
			}
			if !bytes.Equal(r.Data[4], []byte("age")) || string(r.Data[5]) != "18" {
				t.Errorf("HMGet key3 mismatch: got %s => %s", r.Data[4], r.Data[5])
			}

			return nil
		})
		if err != nil {
			t.Fatalf("HMGet test view failed: %v", err)
		}
	})

	// -------------------------------------------------------------------------
	// 2. ZMGet 测试
	// -------------------------------------------------------------------------
	t.Run("ZMGet Operations", func(t *testing.T) {
		zName := "game_rank"
		kvs := [][]byte{
			[]byte("player1"), I2b(100),
			[]byte("player2"), I2b(200),
			[]byte("player3"), I2b(300),
		}

		err := db.Update(func(tx *bbolt.Tx) error {
			return db.ZMSet(tx, zName, kvs...)
		})
		if err != nil {
			t.Fatalf("ZMSet failed: %v", err)
		}

		err = db.View(func(tx *bbolt.Tx) error {
			// 2.1 批量获取存在的成员 score
			queryKeys := [][]byte{[]byte("player1"), []byte("player3")}
			r := db.ZMGet(tx, zName, queryKeys)
			if !r.OK() || r.KvLen() != 2 {
				t.Fatalf("ZMGet failed: got len=%d, want 2", r.KvLen())
			}

			if r.Data[1].Uint64() != 100 || r.Data[3].Uint64() != 300 {
				t.Errorf("ZMGet score mismatch: got %d and %d", r.Data[1].Uint64(), r.Data[3].Uint64())
			}

			// 2.2 批量获取包含不存在成员的 score
			queryKeysWithNotExist := [][]byte{[]byte("player2"), []byte("nobody")}
			r = db.ZMGet(tx, zName, queryKeysWithNotExist)
			if !r.OK() || r.KvLen() != 2 {
				t.Fatalf("ZMGet mixed failed: got len=%d, want 2", r.KvLen())
			}

			if r.Data[1].Uint64() != 200 {
				t.Errorf("ZMGet player2 score mismatch: got %d, want 200", r.Data[1].Uint64())
			}
			if r.Data[3] != nil || r.Data[3].Uint64() != 0 {
				t.Errorf("ZMGet nobody score mismatch: got %v (uint64: %d), want nil/0", r.Data[3], r.Data[3].Uint64())
			}

			return nil
		})
		if err != nil {
			t.Fatalf("ZMGet test view failed: %v", err)
		}
	})
}

func TestDB_HDel_HMDel_HDelBucket(t *testing.T) {
	db := helperOpenDB(t)

	hashName := "user_info"
	hashNeighbor := "user_info_other"

	kvs := [][]byte{
		[]byte("k1"), []byte("v1"),
		[]byte("k2"), []byte("v2"),
		[]byte("k3"), []byte("v3"),
		[]byte("k4"), []byte("v4"),
	}

	err := db.Update(func(tx *bbolt.Tx) error {
		_ = db.HMSet(tx, hashNeighbor, kvs...)
		return db.HMSet(tx, hashName, kvs...)
	})
	if err != nil {
		t.Fatalf("HMSet setup failed: %v", err)
	}

	// 1. 测试 HDel 单个删除
	err = db.Update(func(tx *bbolt.Tx) error {
		if err := db.HDel(tx, hashName, []byte("k1")); err != nil {
			return err
		}
		r := db.HGet(tx, hashName, []byte("k1"))
		if r.OK() {
			t.Errorf("expected k1 to be deleted, but still found")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("HDel test failed: %v", err)
	}

	// 2. 测试 HMDel 批量删除
	err = db.Update(func(tx *bbolt.Tx) error {
		delKeys := [][]byte{[]byte("k2"), []byte("k3")}
		if err := db.HMDel(tx, hashName, delKeys); err != nil {
			return err
		}

		r := db.HMGet(tx, hashName, delKeys)
		if r.Data[1] != nil || r.Data[3] != nil {
			t.Errorf("HMDel failed: k2 or k3 still exists")
		}

		// 验证未被删除的 k4 依然正常
		rK4 := db.HGet(tx, hashName, []byte("k4"))
		if !rK4.OK() || !bytes.Equal(rK4.Bytes(), []byte("v4")) {
			t.Errorf("HMDel accidentally deleted k4")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("HMDel test failed: %v", err)
	}

	// 3. 测试 HDelBucket 清空整个 Hash 表
	err = db.Update(func(tx *bbolt.Tx) error {
		if err := db.HDelBucket(tx, hashName); err != nil {
			return err
		}

		// 验证目标 Hash 表已空
		r := db.HScan(tx, hashName, nil, 100)
		if r.KvLen() != 0 {
			t.Errorf("HDelBucket failed: target hash still has %d items", r.KvLen())
		}

		// 验证相邻 Hash 表未受干扰
		rNeighbor := db.HScan(tx, hashNeighbor, nil, 100)
		if rNeighbor.KvLen() != 4 {
			t.Errorf("HDelBucket affected neighbor hash: got len=%d, want 4", rNeighbor.KvLen())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("HDelBucket test failed: %v", err)
	}
}

func TestDB_ZDel_ZMDel_ZDelBucket(t *testing.T) {
	db := helperOpenDB(t)

	zName := "game_rank"
	zNeighbor := "game_rank_other"

	kvs := [][]byte{
		[]byte("p1"), I2b(100),
		[]byte("p2"), I2b(200),
		[]byte("p3"), I2b(300),
		[]byte("p4"), I2b(400),
	}

	err := db.Update(func(tx *bbolt.Tx) error {
		_ = db.ZMSet(tx, zNeighbor, kvs...)
		return db.ZMSet(tx, zName, kvs...)
	})
	if err != nil {
		t.Fatalf("ZMSet setup failed: %v", err)
	}

	// 1. 测试 ZDel 单个删除（同时验证 Member 和 Score 桶索引清理）
	err = db.Update(func(tx *bbolt.Tx) error {
		if err := db.ZDel(tx, zName, []byte("p1")); err != nil {
			return err
		}

		rGet := db.ZGet(tx, zName, []byte("p1"))
		if rGet.OK() {
			t.Errorf("ZDel failed: p1 still exists in Member bucket")
		}

		rScan := db.ZScan(tx, zName, nil, 0, 0, 100)
		if rScan.KvLen() != 3 {
			t.Errorf("ZDel failed: ZScan length mismatch, got %d, want 3", rScan.KvLen())
		}
		if rScan.Data[0].String() == "p1" {
			t.Errorf("ZDel failed: p1 still exists in Score index bucket")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ZDel test failed: %v", err)
	}

	// 2. 测试 ZMDel 批量删除
	err = db.Update(func(tx *bbolt.Tx) error {
		delKeys := [][]byte{[]byte("p2"), []byte("p3")}
		if err := db.ZMDel(tx, zName, delKeys); err != nil {
			return err
		}

		rScan := db.ZScan(tx, zName, nil, 0, 0, 100)
		if rScan.KvLen() != 1 || rScan.Data[0].String() != "p4" {
			t.Errorf("ZMDel failed: expected only p4 left, got %v", rScan.Dict())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ZMDel test failed: %v", err)
	}

	// 3. 测试 ZDelBucket 清空整个 Zet 表
	err = db.Update(func(tx *bbolt.Tx) error {
		if err := db.ZDelBucket(tx, zName); err != nil {
			return err
		}

		// 验证目标 ZSet 已空
		rScan := db.ZScan(tx, zName, nil, 0, 0, 100)
		if rScan.KvLen() != 0 {
			t.Errorf("ZDelBucket failed: target ZSet still has %d items", rScan.KvLen())
		}

		// 验证相邻 ZSet 未受干扰
		rNeighbor := db.ZScan(tx, zNeighbor, nil, 0, 0, 100)
		if rNeighbor.KvLen() != 4 {
			t.Errorf("ZDelBucket affected neighbor ZSet: got len=%d, want 4", rNeighbor.KvLen())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ZDelBucket test failed: %v", err)
	}
}
