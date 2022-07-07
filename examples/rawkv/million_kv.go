// Copyright 2021 TiKV Authors
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

package main

import (
	"context"
	"fmt"

	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
)

func main() {
	cli, err := rawkv.NewClient(context.TODO(), []string{"127.0.0.1:2379"}, config.DefaultConfig().Security)
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	fmt.Printf("cluster ID: %d\n", cli.ClusterID())

	keyCnt := 1000000
	companyName := "PingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCapPingCap"
	for i := 0; i < keyCnt; i++ {
		keyStr := fmt.Sprintf("Company%d", i)
		valStr := fmt.Sprintf("%s_%d", companyName, i)
		key := []byte(keyStr)
		val := []byte(valStr)

		// put key into tikv
		err = cli.Put(context.TODO(), key, val)
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("Successfully put %d key to tikv\n", keyCnt)

	// get key from tikv
	for i := 0; i < keyCnt; i++ {
		keyStr := fmt.Sprintf("Company%d", i)
		valStr := fmt.Sprintf("%s_%d", companyName, i)
		key := []byte(keyStr)
		val, err := cli.Get(context.TODO(), key)
		if err != nil {
			panic(err)
		}

		if valStr != string(val) {
			errStr := fmt.Sprintf("expected val str %s, but get %s", valStr, string(val))
			panic(errStr)
		}
	}
	//fmt.Printf("found val: %s for key: %s\n", val, key)

	// delete key from tikv
	for i := 0; i < keyCnt; i++ {
		keyStr := fmt.Sprintf("Company%d", i)
		key := []byte(keyStr)
		err = cli.Delete(context.TODO(), key)
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("deleted %d keys\n", keyCnt)

	// get key again from tikv
	for i := 0; i < keyCnt; i++ {
		keyStr := fmt.Sprintf("Company%d", i)
		key := []byte(keyStr)
		val, err := cli.Get(context.TODO(), key)
		if err != nil {
			panic(err)
		}
		if val != nil {
			errStr := fmt.Sprintf("expected val str nil, but get %s", val)
			panic(errStr)
		}
	}
	fmt.Printf("checked all %d deleted key completed val", keyCnt)
}
