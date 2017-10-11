#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

rm -rf /Users/ben/go/src/github.com/benbromhead/cassandra-operator/pkg/apis/cassandra/v1beta2/zz_generated.deepcopy.go
rm -rf /Users/ben/go/src/github.com/benbromhead/cassandra-operator/pkg/generated/*