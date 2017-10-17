// Copyright 2016 The etcd-operator Authors
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

package cluster

import (
	//"fmt"
	//"strings"

	//api "github.com/benbromhead/cassandra-operator/pkg/apis/cassandra/v1beta2"
	"github.com/benbromhead/cassandra-operator/pkg/util/cassandrautil"
	//"github.com/coreos/etcd/etcdserver/etcdserverpb"
	//"github.com/pkg/errors"

	"k8s.io/api/core/v1"
	"github.com/golang/glog"
)

func (c *Cluster) updateMembers(known cassandrautil.MemberSet) error {

	resp, err := cassandrautil.GetMemberNodes(c.ResolvePodServiceAddress(known.PickOne().Name))
	if err != nil {
		return err
	}

	podLookup := make(map[string]*cassandrautil.Member)

	for _, p := range known {
		podLookup[c.ResolvePodServiceAddress(p.Name)] = p
	}


	members := cassandrautil.MemberSet{}
	for _, host := range resp {
		if _, ok := podLookup[host]; ok {
			members[podLookup[host].Name] = &cassandrautil.Member{
				Name:         podLookup[host].Name,
				Namespace:    c.cluster.Namespace,
				SecurePeer:   c.isSecurePeer(),
				SecureClient: c.isSecureClient(),
			}
		} else {
			glog.Warning("Detected member of cluster not in pod")
		}

	}
	c.memberCounter = len(resp)
	c.members = members
	return nil
}
func (c *Cluster) getJoiningNodes(known cassandrautil.MemberSet) (int, error) {
	joining, err := cassandrautil.GetJoiningNodes(known.ClientContactPoints()[0]) //TODO: make array safe
	return len(joining), err
}

func (c *Cluster) newMember(id int) *cassandrautil.Member {
	name := cassandrautil.CreateMemberName(c.cluster.Name, id)
	return &cassandrautil.Member{
		Name:         name,
		Namespace:    c.cluster.Namespace,
		SecurePeer:   c.isSecurePeer(),
		SecureClient: c.isSecureClient(),
	}
}

func (c *Cluster) podsToMemberSet(pods []*v1.Pod, sc bool) cassandrautil.MemberSet {
	members := cassandrautil.MemberSet{}
	for _, pod := range pods {
		m := &cassandrautil.Member{Name: pod.Name , Namespace: pod.Namespace, SecureClient: sc}
		members.Add(m)
	}
	return members
}