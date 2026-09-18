package mdb

import (
	"bytes"
	"errors"
	"fmt"
	"math"
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
	dbPath := filepath.Join(t.TempDir(), "quant_sys_test.db")
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
// 1. 底层解析器与编解码边界测试
// -----------------------------------------------------------------------------

func TestZeroCopyParsers(t *testing.T) {
	// 测试 parseUintBytes
	u, err := parseUintBytes([]byte("1234567890"))
	if err != nil || u != 1234567890 {
		t.Errorf("parseUintBytes failed: got %d, err %v", u, err)
	}
	if _, err = parseUintBytes([]byte("12a3")); err == nil {
		t.Errorf("parseUintBytes expected error for invalid string")
	}

	// 测试 parseIntBytes
	i, err := parseIntBytes([]byte("-987654321"))
	if err != nil || i != -987654321 {
		t.Errorf("parseIntBytes failed: got %d, err %v", i, err)
	}
	i2, err := parseIntBytes([]byte("8848"))
	if err != nil || i2 != 8848 {
		t.Errorf("parseIntBytes failed: got %d, err %v", i2, err)
	}
}

func TestFloat64SortableConversion(t *testing.T) {
	// 验证 IEEE 754 浮点数转 uint64 后的严格单调递增性 (适用 MACD 等负数指标)
	floats := []float64{-999.99, -1.0, -0.01, 0.0, 0.01, 1.0, 999.99}
	var prevU uint64
	for i, f := range floats {
		u := Float64ToSortableUint64(f)
		backF := SortableUint64ToFloat64(u)
		if math.Abs(backF-f) > 1e-9 {
			t.Errorf("Mismatch conversion: %f != %f", f, backF)
		}
		if i > 0 && u <= prevU {
			t.Errorf("Not strict monotonic: %f(%d) <= previous(%d)", f, u, prevU)
		}
		prevU = u
	}
}

func TestEncodeBufExpansion(t *testing.T) {
	// 验证指针穿透：当对象池给的 buffer 太小导致 make 扩容时，指针是否成功更新
	buf := make([]byte, 2)
	bufPtr := &buf
	out, err := encodeHashKeyToBuf("symbol", []byte("SH600519"), bufPtr)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(out) != 15 || cap(*bufPtr) < 15 {
		t.Errorf("buffer not expanded correctly, cap is %d", cap(*bufPtr))
	}
}

func TestKeyEncodingLimits(t *testing.T) {
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

	_, _, err = DecodeHashKey([]byte{})
	if !errors.Is(err, ErrBufferTooShort) {
		t.Errorf("expected ErrBufferTooShort for empty buf")
	}
}

// -----------------------------------------------------------------------------
// 2. 数据库生命周期与并发测试
// -----------------------------------------------------------------------------

func TestDB_LifecycleAndConcurrency(t *testing.T) {
	var nilDB *DB
	if err := nilDB.View(func(tx *bolt.Tx) error { return nil }); err == nil {
		t.Error("expected error on nil db View")
	}

	db := helperOpenDB(t)
	if err := db.View(nil); err == nil {
		t.Error("expected error on nil callback")
	}

	// 并发读写屏障测试 (模拟量化系统的高频读取与后台 Compact 热替换冲突)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_ = db.View(func(tx *bolt.Tx) error {
			wg.Done()
			time.Sleep(50 * time.Millisecond) // 模拟慢查询持有 RLock
			return nil
		})
	}()

	wg.Wait()
	start := time.Now()
	_ = db.Close()
	if time.Since(start) < 40*time.Millisecond {
		t.Error("Close did not wait for active transaction to release RLock")
	}
}

// -----------------------------------------------------------------------------
// 3. Hash 存储引擎全量测试 (HGetFunc / HScanFunc)
// -----------------------------------------------------------------------------

func TestDB_Hash_Operations(t *testing.T) {
	db := helperOpenDB(t)
	hashName := "tick_data"
	kvs := [][]byte{
		[]byte("09:30:00"), []byte("15.01"),
		[]byte("09:30:03"), []byte("15.05"),
		[]byte("09:30:06"), []byte("15.02"),
		[]byte("09:30:09"), []byte("15.08"),
	}

	err := db.Update(func(tx *bolt.Tx) error {
		_ = db.HMSet(tx, "tick_data_a", kvs...) // 干扰项，测试前缀隔离
		return db.HMSet(tx, hashName, kvs...)
	})
	if err != nil {
		t.Fatalf("HMSet failed: %v", err)
	}

	err = db.View(func(tx *bolt.Tx) error {
		// 1. HGetFunc 命中与未命中
		var price string
		err := db.HGetFunc(tx, hashName, []byte("09:30:03"), func(val []byte) error {
			price = string(val)
			return nil
		})
		if err != nil || price != "15.05" {
			t.Errorf("HGetFunc failed: got %s, err %v", price, err)
		}

		err = db.HGetFunc(tx, hashName, []byte("09:30:01"), func(v []byte) error { return nil })
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("HGetFunc expected ErrKeyNotFound")
		}

		// 2. HMGetFunc 批量获取
		results := make(map[string]string)
		err = db.HMGetFunc(tx, hashName, [][]byte{[]byte("09:30:00"), []byte("09:30:09"), []byte("miss")}, func(k, v []byte) error {
			if v != nil {
				results[string(k)] = string(v)
			}
			return nil
		})
		if len(results) != 2 || results["09:30:09"] != "15.08" {
			t.Errorf("HMGetFunc mismatch: %v", results)
		}

		// 3. HScanFunc 正序扫描与提前终止 (Early Break)
		var scanned []string
		err = db.HScanFunc(tx, hashName, nil, 100, func(key, val []byte) bool {
			scanned = append(scanned, string(key))
			return string(key) != "09:30:03" // 扫到 03 提前退出
		})
		if len(scanned) != 2 || scanned[1] != "09:30:03" {
			t.Errorf("HScanFunc early break failed: %v", scanned)
		}

		// 4. HRScanFunc 逆序扫描
		scanned = nil
		err = db.HRScanFunc(tx, hashName, nil, 2, func(key, val []byte) bool {
			scanned = append(scanned, string(key))
			return true
		})
		if len(scanned) != 2 || scanned[0] != "09:30:09" || scanned[1] != "09:30:06" {
			t.Errorf("HRScanFunc sequence failed: %v", scanned)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Hash View failed: %v", err)
	}

	// 5. Hash 自增与删除
	_ = db.Update(func(tx *bolt.Tx) error {
		v, _ := db.Hincr(tx, "stats", []byte("req_count"), 10)
		if v != 10 {
			t.Errorf("Hincr init failed: %d", v)
		}
		_ = db.HSet(tx, "stats", []byte("str_num"), []byte("100"))
		v, _ = db.Hincr(tx, "stats", []byte("str_num"), 50)
		if v != 150 {
			t.Errorf("Hincr on string failed: %d", v)
		}

		_ = db.HDel(tx, hashName, []byte("09:30:00"))
		_ = db.HMDel(tx, hashName, [][]byte{[]byte("09:30:03")})
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
}

// -----------------------------------------------------------------------------
// 4. ZSet 基础整数排序 (RPS 排名场景)
// -----------------------------------------------------------------------------

func TestDB_Zet_Int_Operations(t *testing.T) {
	db := helperOpenDB(t)
	zName := "rps_rank"
	kvs := [][]byte{
		[]byte("SH600000"), I2b(85),
		[]byte("SZ000001"), []byte("92"), // 测试 string 解析
		[]byte("SH600519"), I2b(99),
		[]byte("SZ002594"), I2b(95),
	}

	_ = db.Update(func(tx *bolt.Tx) error {
		_ = db.ZMSet(tx, zName, kvs...)
		v, _ := db.Zincr(tx, zName, []byte("SZ000001"), -5) // 92 - 5 = 87
		if v != 87 {
			t.Errorf("Zincr failed: %d", v)
		}
		return nil
	})

	_ = db.View(func(tx *bolt.Tx) error {
		var score uint64
		_ = db.ZGetFunc(tx, zName, []byte("SH600519"), func(s uint64) error {
			score = s
			return nil
		})
		if score != 99 {
			t.Errorf("ZGetFunc failed: %d", score)
		}

		var scanOrder []string
		_ = db.ZScanFunc(tx, zName, nil, 0, 0, 10, func(k []byte, s uint64) bool {
			scanOrder = append(scanOrder, string(k))
			return true
		})
		// 期望正序: SH600000(85), SZ000001(87), SZ002594(95), SH600519(99)
		if scanOrder[0] != "SH600000" || scanOrder[3] != "SH600519" {
			t.Errorf("ZScanFunc rank mismatch: %v", scanOrder)
		}

		var rScanOrder []string
		_ = db.ZRScanFunc(tx, zName, nil, 0, 0, 2, func(k []byte, s uint64) bool {
			rScanOrder = append(rScanOrder, string(k))
			return true
		})
		if len(rScanOrder) != 2 || rScanOrder[0] != "SH600519" {
			t.Errorf("ZRScanFunc limit mismatch: %v", rScanOrder)
		}
		return nil
	})

	_ = db.Update(func(tx *bolt.Tx) error {
		_ = db.ZDelBucket(tx, zName)
		return nil
	})
}

// -----------------------------------------------------------------------------
// 5. ZSet 完美浮点数排序 (MACD/因子值等带负数的场景)
// -----------------------------------------------------------------------------

func TestDB_Zet_Float_Operations(t *testing.T) {
	db := helperOpenDB(t)
	zName := "macd_indicators"

	_ = db.Update(func(tx *bolt.Tx) error {
		_ = db.ZSetF(tx, zName, []byte("stock_a"), -1.25)
		_ = db.ZSetF(tx, zName, []byte("stock_b"), 0.0)
		_ = db.ZSetF(tx, zName, []byte("stock_c"), -0.05)
		_ = db.ZSetF(tx, zName, []byte("stock_d"), 2.45)
		return nil
	})

	_ = db.View(func(tx *bolt.Tx) error {
		var s float64
		_ = db.ZGetFuncF(tx, zName, []byte("stock_a"), func(score float64) error {
			s = score
			return nil
		})
		if s != -1.25 {
			t.Errorf("ZGetFuncF failed: got %f", s)
		}

		// 验证浮点数区间扫描 [-1.0, 1.0] (应该排除 a 和 d)
		var scanned []string
		_ = db.ZScanFuncF(tx, zName, nil, -1.0, 1.0, 10, func(k []byte, score float64) bool {
			scanned = append(scanned, string(k))
			return true
		})
		if len(scanned) != 2 || scanned[0] != "stock_c" || scanned[1] != "stock_b" {
			t.Errorf("ZScanFuncF float range failed: %v", scanned)
		}

		// 验证浮点数逆序全表扫描
		var rScanned []string
		_ = db.ZRScanFuncF(tx, zName, nil, 0, 0, 10, func(k []byte, score float64) bool {
			rScanned = append(rScanned, string(k))
			return true
		})
		if len(rScanned) != 4 || rScanned[0] != "stock_d" || rScanned[3] != "stock_a" {
			t.Errorf("ZRScanFuncF full reverse failed: %v", rScanned)
		}
		return nil
	})
}

// -----------------------------------------------------------------------------
// 6. 热压实与碎片整理测试 (Compact)
// -----------------------------------------------------------------------------

func TestDB_CompactAndStats(t *testing.T) {
	db := helperOpenDB(t)
	hashName := "compact_hash"
	val1KB := bytes.Repeat([]byte("x"), 1024)

	// 1. 批量写入产生数据
	_ = db.Update(func(tx *bolt.Tx) error {
		for i := 0; i < 2000; i++ {
			_ = db.HSet(tx, hashName, []byte(fmt.Sprintf("k_%04d", i)), val1KB)
		}
		return nil
	})

	// 2. 批量删除产生碎片空洞
	_ = db.Update(func(tx *bolt.Tx) error {
		var delKeys [][]byte
		for i := 0; i < 1500; i++ {
			delKeys = append(delKeys, []byte(fmt.Sprintf("k_%04d", i)))
		}
		return db.HMDel(tx, hashName, delKeys)
	})

	statsAfterDel, _ := db.Stats()

	// 3. 执行 Compact (内部开启 NoSync 和 FillPercent=0.9)
	if err := db.Compact(""); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	statsAfterCompact, _ := db.Stats()
	if statsAfterCompact.FileSize >= statsAfterDel.FileSize {
		t.Errorf("Compact failed to reduce file size. Before: %d, After: %d",
			statsAfterDel.FileSize, statsAfterCompact.FileSize)
	}

	// 4. 压实后的数据完整性校验
	_ = db.View(func(tx *bolt.Tx) error {
		count := 0
		_ = db.HScanFunc(tx, hashName, nil, 1000, func(k, v []byte) bool {
			count++
			return true
		})
		if count != 500 {
			t.Errorf("Compact data integrity lost, expected 500, got %d", count)
		}
		return nil
	})
}
