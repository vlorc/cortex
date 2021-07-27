package qiniu

import (
	"bytes"
	"context"
	"io/ioutil"
	"testing"
)

func TestQiniuClientAll(t *testing.T) {
	cli, err := NewQiniuObjectClient(Config{
		Access: "W9mQA9_oScvEdqsrfLR9wQLmr5TH57EqBiBX6FnI",
		Secret: "UYdw8mKyLQA2ZaWRoCgg7uJkdn42I2JTVScu_bU2",
		Bucket: "ccnm",
		Flag:   "https,cdn,private",
		Url:    "https://q.nyancat.cn",
	})

	if nil != err {
		t.Error("NewQiniuObjectClient failed:", err.Error())
		return
	}

	ctx := context.Background()
	key := "fake/a70ecbaeaa65a26a_17ab9b3875f_17ab9b3889b_d8c9fe60"
	src := []byte("ccccccccccccccccccccccccccccccccccccccccc")

	if _, _, err = cli.List(ctx, "fake", "/"); nil != err {
		t.Error("List failed:", err.Error())
		return
	}

	if err = cli.PutObject(ctx, key, bytes.NewReader(src)); nil != err {
		t.Error("PutObject failed:", err.Error())
		return
	}

	r, err := cli.GetObject(ctx, key)
	if nil != err {
		t.Error("GetObject failed:", err.Error())
		return
	}
	defer r.Close()

	dst, err := ioutil.ReadAll(r)
	if nil != err {
		t.Error("ReadAll failed:", err.Error())
		return
	}

	if bytes.Compare(src, dst) != 0 {
		t.Error("Compare failed")
		return
	}

	if err = cli.DeleteObject(context.Background(), key); nil != err {
		t.Error("delObject", err.Error())
		return
	}
}
