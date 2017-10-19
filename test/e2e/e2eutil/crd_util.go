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
	"fmt"
	"testing"
	"time"

	api "github.com/benbromhead/cassandra-operator/pkg/apis/cassandra/v1beta2"
	"github.com/benbromhead/cassandra-operator/pkg/generated/clientset/versioned"
	"github.com/benbromhead/cassandra-operator/pkg/util/k8sutil"
	"github.com/benbromhead/cassandra-operator/pkg/util/retryutil"

	"github.com/aws/aws-sdk-go/service/s3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type StorageCheckerOptions struct {
	S3Cli          *s3.S3
	S3Bucket       string
	DeletedFromAPI bool
}

func CreateCluster(t *testing.T, crClient versioned.Interface, namespace string, cl *api.CassandraCluster) (*api.CassandraCluster, error) {
	cl.Namespace = namespace
	res, err := crClient.CassandraV1beta2().CassandraClusters(namespace).Create(cl)
	if err != nil {
		return nil, err
	}
	t.Logf("creating cassandra cluster: %s", res.Name)

	return res, nil
}

func UpdateCluster(crClient versioned.Interface, cl *api.CassandraCluster, maxRetries int, updateFunc k8sutil.EtcdClusterCRUpdateFunc) (*api.CassandraCluster, error) {
	return AtomicUpdateClusterCR(crClient, cl.Name, cl.Namespace, maxRetries, updateFunc)
}

func AtomicUpdateClusterCR(crClient versioned.Interface, name, namespace string, maxRetries int, updateFunc k8sutil.EtcdClusterCRUpdateFunc) (*api.CassandraCluster, error) {
	result := &api.CassandraCluster{}
	err := retryutil.Retry(1*time.Second, maxRetries, func() (done bool, err error) {
		cassandraCluster, err := crClient.CassandraV1beta2().CassandraClusters(namespace).Get(name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		updateFunc(cassandraCluster)

		result, err = crClient.CassandraV1beta2().CassandraClusters(namespace).Update(cassandraCluster)
		if err != nil {
			if apierrors.IsConflict(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	})
	return result, err
}

func DeleteCluster(t *testing.T, crClient versioned.Interface, kubeClient kubernetes.Interface, cl *api.CassandraCluster) error {
	t.Logf("deleting cassandra cluster: %v", cl.Name)
	err := crClient.CassandraV1beta2().CassandraClusters(cl.Namespace).Delete(cl.Name, nil)
	if err != nil {
		return err
	}
	return waitResourcesDeleted(t, kubeClient, cl)
}

func DeleteClusterAndBackup(t *testing.T, crClient versioned.Interface, kubecli kubernetes.Interface, cl *api.CassandraCluster, checkerOpt StorageCheckerOptions) error {
	err := DeleteCluster(t, crClient, kubecli, cl)
	if err != nil {
		return err
	}
	t.Logf("waiting backup deleted of cluster (%v)", cl.Name)
	//err = WaitBackupDeleted(kubecli, cl, checkerOpt) TODO: fix
	if err != nil {
		return fmt.Errorf("fail to wait backup deleted: %v", err)
	}
	return nil
}
