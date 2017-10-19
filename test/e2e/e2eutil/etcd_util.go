// Copyright 2017 The cassandra-operator Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2eutil

import (
	"context"
	"testing"

	"github.com/benbromhead/cassandra-operator/pkg/util/constants"
	"github.com/coreos/etcd/clientv3"
)

const (
	cassandraKeyFoo = "foo"
	cassandraValBar = "bar"
)

func PutDataToEtcd(url string) error {
	cassandracli, err := createEtcdClient(url)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), constants.DefaultRequestTimeout)
	_, err = cassandracli.Put(ctx, cassandraKeyFoo, cassandraValBar)
	cancel()
	cassandracli.Close()
	return err
}

func CheckEtcdData(t *testing.T, url string) {
	cassandracli, err := createEtcdClient(url)
	if err != nil {
		t.Fatalf("failed to create cassandra client:%v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), constants.DefaultRequestTimeout)
	resp, err := cassandracli.Get(ctx, cassandraKeyFoo)
	cancel()
	cassandracli.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 1 {
		t.Errorf("want only 1 key result, get %d", len(resp.Kvs))
	} else {
		val := string(resp.Kvs[0].Value)
		if val != cassandraValBar {
			t.Errorf("value want = '%s', get = '%s'", cassandraValBar, val)
		}
	}
}

func createEtcdClient(addr string) (*clientv3.Client, error) {
	cfg := clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: constants.DefaultDialTimeout,
	}
	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	return c, nil
}
