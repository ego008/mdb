package mdb

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
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
// Key 编解码及辅助解析器测试
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
	longName := strings.Repeat("a", 256)
	_, err := EncodeHashKey(longName, []byte("key"))
	if !errors.Is(err, ErrNameTooLong) {
		t.Errorf("expected ErrNameTooLong, got %v", err)
	}

	longKey := bytes.Repeat([]byte("k"), 256)
	_, err = EncodeHashKey("name", longKey)
	if !errors.Is(err, ErrKeyTooLong) {
		t.Errorf("expected ErrKeyTooLong, got %v", err)
	}
}

func TestDecodeInvalidBuffer(t *testing.T) {
	_, _, err := DecodeHashKey([]byte{})
	if !errors.Is(err, ErrBufferTooShort) {
		t.Errorf("expected ErrBufferTooShort, got %v", err)
	}

	buf := []byte{10, 'a', 'b'}
	_, _, err = DecodeHashKey(buf)
	if !errors.Is(err, ErrBufferTooShort) {
		t.Errorf("expected ErrBufferTooShort, got %v", err)
	}

	invalidZBufs := [][]byte{
		nil,
		{},
		{0},
		{5, 'a', 'b'},
		{2, 'a', 'b', 0, 0, 0, 0},
	}
	for i, zb := range invalidZBufs {
		_, _, _, err := DecodeZsetScoreKey(zb)
		if err != ErrInvalidBuf {
			t.Errorf("Test case %d expected ErrInvalidBuf, got %v", i, err)
		}
	}
}

func TestZeroCopyParsers(t *testing.T) {
	// parseUintBytes
	u, err := parseUintBytes([]byte("123456"))
	if err != nil || u != 123456 {
		t.Errorf("parseUintBytes failed: got %d, err %v", u, err)
	}
	_, err = parseUintBytes([]byte("12a3"))
	if err == nil {
		t.Errorf("parseUintBytes expected error for invalid string")
	}

	// parseIntBytes
	i, err := parseIntBytes([]byte("-9876"))
	if err != nil || i != -9876 {
		t.Errorf("parseIntBytes failed: got %d, err %v", i, err)
	}
	i2, err := parseIntBytes([]byte("9876"))
	if err != nil || i2 != 9876 {
		t.Errorf("parseIntBytes failed: got %d, err %v", i2, err)
	}
}

// -----------------------------------------------------------------------------
// 数据库基础与并发生命周期测试
// -----------------------------------------------------------------------------

func TestDB_NilSafety(t *testing.T) {
	var nilDB *DB

	if err := nilDB.View(func(tx *bolt.Tx) error { return nil }); err == nil {
		t.Error("expected error on nil db View")
	}
	if err := nilDB.Update(func(tx *bolt.Tx) error { return nil }); err == nil {
		t.Error("expected error on nil db Update")
	}

	db := helperOpenDB(t)
	if err := db.View(nil); err == nil {
		t.Error("expected error on nil callback View")
	}
	if err := db.Update(nil); err == nil {
		t.Error("expected error on nil callback Update")
	}
}

func TestDB_Close_Concurrency(t *testing.T) {
	db := helperOpenDB(t)

	var wg sync.WaitGroup
	wg.Add(1)

	// 模拟事务正在运行中被并发 Close 调用的场景（利用 RWMutex 保护）
	go func() {
		_ = db.View(func(tx *bolt.Tx) error {
			wg.Done()
			time.Sleep(50 * time.Millisecond) // 占有读锁
			return nil
		})
	}()

	wg.Wait()
	start := time.Now()
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close failed: %v", err)
	}

	// Close 的写锁必须等待 View 的读锁释放才能获取
	if time.Since(start) < 40*time.Millisecond {
		t.Error("db.Close returned too early, did not wait for active transaction")
	}
}

// -----------------------------------------------------------------------------
// Hash: Set, GetFunc, ScanFunc 测试
// -----------------------------------------------------------------------------

func TestDB_Hash_GetFunc_ScanFunc(t *testing.T) {
	db := helperOpenDB(t)

	hashName := "user_info"
	kvs := [][]byte{
		[]byte("a"), []byte("av"),
		[]byte("b"), []byte("bv"),
		[]byte("c"), []byte("cv"),
		[]byte("d"), []byte("dv"),
	}

	err := db.Update(func(tx *bolt.Tx) error {
		// 写入相邻命名空间验证隔离性
		_ = db.HMSet(tx, "user_infi", kvs...)
		_ = db.HMSet(tx, "user_infq", kvs...)
		return db.HMSet(tx, hashName, kvs...)
	})
	if err != nil {
		t.Fatalf("HMSet failed: %v", err)
	}

	err = db.View(func(tx *bolt.Tx) error {
		// 1. 测试 HGetFunc
		var outVal []byte
		err := db.HGetFunc(tx, hashName, []byte("c"), func(val []byte) error {
			outVal = append([]byte(nil), val...)
			return nil
		})
		if err != nil || string(outVal) != "cv" {
			t.Errorf("HGetFunc failed: got %s, err %v", outVal, err)
		}

		// 测试 ErrKeyNotFound
		err = db.HGetFunc(tx, hashName, []byte("not_exist"), func(val []byte) error { return nil })
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("HGetFunc expected ErrKeyNotFound, got %v", err)
		}

		// 2. 测试 HMGetFunc
		queryKeys := [][]byte{[]byte("a"), []byte("c"), []byte("not_exist")}
		results := make(map[string]string)
		err = db.HMGetFunc(tx, hashName, queryKeys, func(key, val []byte) error {
			if val != nil {
				results[string(key)] = string(val)
			}
			return nil
		})
		if err != nil || results["a"] != "av" || results["c"] != "cv" || len(results) != 2 {
			t.Errorf("HMGetFunc mismatch: %v, err: %v", results, err)
		}

		// 3. 测试 HScanFunc (正序 + 提前终止)
		var scanned []string
		err = db.HScanFunc(tx, hashName, nil, 100, func(key, val []byte) bool {
			scanned = append(scanned, string(key))
			// 测试提前终止 (Early Break)，在扫描到 b 时退出
			return string(key) != "b"
		})
		if err != nil {
			t.Fatalf("HScanFunc failed: %v", err)
		}
		if len(scanned) != 2 || scanned[0] != "a" || scanned[1] != "b" {
			t.Errorf("HScanFunc early break failed: %v", scanned)
		}

		// 4. 测试 HRScanFunc (逆序)
		scanned = nil
		err = db.HRScanFunc(tx, hashName, nil, 2, func(key, val []byte) bool {
			scanned = append(scanned, string(key))
			return true
		})
		if len(scanned) != 2 || scanned[0] != "d" || scanned[1] != "c" {
			t.Errorf("HRScanFunc limit failed: %v", scanned)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}
}

func TestDB_Hash_Hincr_Del(t *testing.T) {
	db := helperOpenDB(t)

	hashName := "counters"
	field := []byte("clicks")

	err := db.Update(func(tx *bolt.Tx) error {
		// 1. 测试 Hincr
		v, err := db.Hincr(tx, hashName, field, 10)
		if err != nil || v != 10 {
			t.Errorf("Hincr initial failed: got %d", v)
		}

		// 测试对字符数字 Hincr (触发零拷贝解析)
		_ = db.HSet(tx, hashName, []byte("str_num"), []byte("100"))
		v, err = db.Hincr(tx, hashName, []byte("str_num"), 50)
		if err != nil || v != 150 {
			t.Errorf("Hincr string num failed: got %d", v)
		}

		// 2. 测试 HDel 批量删除与 Bucket 清空
		_ = db.HSet(tx, hashName, []byte("k1"), []byte("v1"))
		_ = db.HSet(tx, hashName, []byte("k2"), []byte("v2"))
		_ = db.HMDel(tx, hashName, [][]byte{[]byte("k1"), []byte("k2")})

		err = db.HGetFunc(tx, hashName, []byte("k1"), func(v []byte) error { return nil })
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("HMDel failed to delete")
		}

		_ = db.HDelBucket(tx, hashName)
		count := 0
		_ = db.HScanFunc(tx, hashName, nil, 10, func(k, v []byte) bool {
			count++
			return true
		})
		if count != 0 {
			t.Errorf("HDelBucket failed, remaining %d", count)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Zet: Set, GetFunc, ScanFunc, Incr 测试
// -----------------------------------------------------------------------------

func TestDB_Zet_Functions(t *testing.T) {
	db := helperOpenDB(t)
	zName := "leaderboard"

	kvs := [][]byte{
		[]byte("p1"), I2b(100),
		[]byte("p2"), []byte("200"), // 混合测试 string score
		[]byte("p3"), I2b(300),
		[]byte("p4"), I2b(400),
	}

	err := db.Update(func(tx *bolt.Tx) error {
		if err := db.ZMSet(tx, zName, kvs...); err != nil {
			return err
		}
		// 1. Zincr
		newScore, err := db.Zincr(tx, zName, []byte("p2"), -50)
		if err != nil || newScore != 150 {
			t.Errorf("Zincr failed: got %d", newScore)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	err = db.View(func(tx *bolt.Tx) error {
		// 2. ZGetFunc
		var finalScore uint64
		err := db.ZGetFunc(tx, zName, []byte("p2"), func(score uint64) error {
			finalScore = score
			return nil
		})
		if err != nil || finalScore != 150 {
			t.Errorf("ZGetFunc failed: got %d", finalScore)
		}

		// 测试 ErrKeyNotFound
		err = db.ZGetFunc(tx, zName, []byte("p_x"), func(s uint64) error { return nil })
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("ZGetFunc expected ErrKeyNotFound, got %v", err)
		}

		// 3. ZMGetFunc
		queryKeys := [][]byte{[]byte("p1"), []byte("p4"), []byte("px")}
		results := make(map[string]uint64)
		err = db.ZMGetFunc(tx, zName, queryKeys, func(k []byte, score uint64, exists bool) error {
			if exists {
				results[string(k)] = score
			}
			return nil
		})
		if err != nil || results["p1"] != 100 || results["p4"] != 400 || len(results) != 2 {
			t.Errorf("ZMGetFunc failed: %v", results)
		}

		// 4. ZScanFunc (按 Score 区间扫描 + Early Break)
		var scannedKeys []string
		err = db.ZScanFunc(tx, zName, nil, 100, 300, 100, func(key []byte, score uint64) bool {
			scannedKeys = append(scannedKeys, string(key))
			// 扫描到 p2 即中止
			return string(key) != "p2"
		})
		if err != nil {
			t.Fatalf("ZScanFunc failed: %v", err)
		}
		// 期望返回区间内的数据 p1(100), p2(150), p3(300)。但在 p2 中止
		if len(scannedKeys) != 2 || scannedKeys[0] != "p1" || scannedKeys[1] != "p2" {
			t.Errorf("ZScanFunc early break mismatch: %v", scannedKeys)
		}

		// 5. ZRScanFunc 逆序扫描
		var revScanned []string
		err = db.ZRScanFunc(tx, zName, nil, 0, 0, 10, func(key []byte, score uint64) bool {
			revScanned = append(revScanned, string(key))
			return true
		})
		// 期望全表逆序: p4(400), p3(300), p2(150), p1(100)
		if len(revScanned) != 4 || revScanned[0] != "p4" || revScanned[3] != "p1" {
			t.Errorf("ZRScanFunc sequence mismatch: %v", revScanned)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}

	// 6. ZDel / ZDelBucket 验证
	err = db.Update(func(tx *bolt.Tx) error {
		_ = db.ZDel(tx, zName, []byte("p1"))

		err := db.ZGetFunc(tx, zName, []byte("p1"), func(s uint64) error { return nil })
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("ZDel failed")
		}

		_ = db.ZDelBucket(tx, zName)
		count := 0
		_ = db.ZScanFunc(tx, zName, nil, 0, 0, 10, func(k []byte, s uint64) bool {
			count++
			return true
		})
		if count != 0 {
			t.Errorf("ZDelBucket failed, remaining %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Del Update failed: %v", err)
	}
}

// -----------------------------------------------------------------------------
// 统计、碎片率与热压实 (Compact) 测试
// -----------------------------------------------------------------------------

func TestDB_Stats_And_Compact(t *testing.T) {
	db := helperOpenDB(t)

	hashName := "compact_hash"
	val1KB := bytes.Repeat([]byte("x"), 1024)

	// 1. 批量插入，扩大物理文件与脏页
	err := db.Update(func(tx *bolt.Tx) error {
		for i := 0; i < 3000; i++ {
			k := []byte(fmt.Sprintf("k_%04d", i))
			if err := db.HSet(tx, hashName, k, val1KB); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// 2. 批量删除产生碎片
	err = db.Update(func(tx *bolt.Tx) error {
		var delKeys [][]byte
		for i := 0; i < 2400; i++ {
			delKeys = append(delKeys, []byte(fmt.Sprintf("k_%04d", i)))
		}
		return db.HMDel(tx, hashName, delKeys)
	})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 3. 检查门槛拦截逻辑
	statsAfterDel, err := db.Stats()
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	shouldCompact, _, err := db.ShouldCompact(500*1024, 0.20)
	if err != nil || !shouldCompact {
		t.Errorf("ShouldCompact expected true")
	}

	shouldCompactHigh, _, _ := db.ShouldCompact(100*1024*1024, 0.20)
	if shouldCompactHigh {
		t.Errorf("ShouldCompact high limit expected false")
	}

	// 4. 执行原位热压实替换
	if err := db.Compact(""); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	statsAfterCompact, err := db.Stats()
	if err != nil {
		t.Fatalf("Stats after compact failed: %v", err)
	}

	if statsAfterCompact.FileSize >= statsAfterDel.FileSize {
		t.Errorf("FileSize did not decrease after compact: before %d, after %d",
			statsAfterDel.FileSize, statsAfterCompact.FileSize)
	}

	// 5. 数据完整性与并发句柄替换校验
	err = db.View(func(tx *bolt.Tx) error {
		count := 0
		err := db.HScanFunc(tx, hashName, nil, 1000, func(k, v []byte) bool {
			count++
			return true
		})
		if err != nil || count != 600 {
			t.Errorf("Data integrity check failed: expected 600, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}
}
