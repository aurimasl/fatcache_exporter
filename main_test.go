package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/grobie/gomemcache/memcache"
)

func TestAcceptance(t *testing.T) {
	done := false

	addr := "localhost:11211"
	// FATCACHE_PORT might be set by a linked fatcache docker container.
	if env := os.Getenv("FATCACGE_PORT"); env != "" {
		addr = strings.TrimLeft(env, "tcp://")
	}

	exporter := exec.Command("./fatcache_exporter", "--memcached.address", addr)
	go func() {
		if err := exporter.Run(); err != nil && !done {
			t.Fatal(err)
		}
	}()
	defer func() {
		if exporter.Process != nil {
			exporter.Process.Kill()
		}
	}()

	defer func() {
		done = true
	}()

	// TODO(ts): Replace sleep with ready check loop.
	time.Sleep(100 * time.Millisecond)

	client, err := memcache.New(addr)
	if err != nil {
		t.Fatal(err)
	}
	// if err := client.StatsReset(); err != nil {
	// 	t.Fatal(err)
	// }

	item := &memcache.Item{Key: "foo", Value: []byte("bar")}
	if err := client.Set(item); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(item); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Get("foo"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Get("qux"); err != memcache.ErrCacheMiss {
		t.Fatal(err)
	}
	last, err := client.Get("foo")
	if err != nil {
		t.Fatal(err)
	}
	last.Value = []byte("banana")
	if err := client.CompareAndSwap(last); err != nil {
		t.Fatal(err)
	}
	large := &memcache.Item{Key: "large", Value: bytes.Repeat([]byte("."), 130)}
	if err := client.Set(large); err != nil {
		t.Fatal(err)
	}
	fmt.Println("111")
	resp, err := http.Get("http://localhost:9150/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	tests := []string{
		`fatcache_up 1`,
		`fatcache_commands_total{command="cmd_get",status="hit"} `,
		`fatcache_commands_total{command="cmd_get",status="miss"} `,
		`fatcache_commands_total{command="cmd_set",status="hit"} `,
		`fatcache_commands_total{command="cmd_cas",status="hit"} `,
		`fatcache_curr_connections `,
		`fatcache_max_connections `,
		`fatcache_current_items `,
		`fatcache_items_total `,
		`fatcache_slab_current_items{slab="1"} `,
		`fatcache_slab_current_items{slab="5"} `,
		`fatcache_slab_commands_total{chunk_size="80",command="cmd_set",slab="0",status="hit"} `,
		`fatcache_slab_commands_total{chunk_size="80",command="cmd_cas",slab="0",status="hit"} `,
		`fatcache_slab_commands_total{chunk_size="80",command="cmd_set",slab="0",status="hit"} `,
		`fatcache_slab_commands_total{chunk_size="80",command="cmd_cas",slab="0",status="hit"} `,
		`fatcache_slab_current_chunks{slab="1"} `,
		`fatcache_slab_current_chunks{slab="5"} `,
		`fatcache_slab_mem_requested_bytes{slab="1"} `,
		`fatcache_slab_mem_requested_bytes{slab="5"} `,
	}
	for _, test := range tests {
		if !bytes.Contains(body, []byte(test)) {
			t.Errorf("want metrics to include %q, have:\n%s", test, body)
		}
	}
}
