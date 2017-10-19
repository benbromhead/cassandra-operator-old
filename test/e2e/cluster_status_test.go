// Copyright 2017 The Cassandra-operator Authors
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

package e2e

import (
	//"fmt"
	"os"
	"testing"
	"time"

	//api "github.com/benbromhead/cassandra-operator/pkg/apis/cassandra/v1beta2"
	"github.com/benbromhead/cassandra-operator/pkg/util/retryutil"
	"github.com/benbromhead/cassandra-operator/test/e2e/e2eutil"
	"github.com/benbromhead/cassandra-operator/test/e2e/framework"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestReadyMembersStatus(t *testing.T) {
	if os.Getenv(envParallelTest) == envParallelTestTrue {
		t.Parallel()
	}
	f := framework.Global
	size := 1
	testCassandra, err := e2eutil.CreateCluster(t, f.CRClient, f.Namespace, e2eutil.NewCluster("test-Cassandra-", size))
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := e2eutil.DeleteCluster(t, f.CRClient, f.KubeClient, testCassandra); err != nil {
			t.Fatal(err)
		}
	}()

	if _, err := e2eutil.WaitUntilSizeReached(t, f.CRClient, size, 3, testCassandra); err != nil {
		t.Fatalf("failed to create %d members Cassandra cluster: %v", size, err)
	}

	err = retryutil.Retry(5*time.Second, 3, func() (done bool, err error) {
		currCassandra, err := f.CRClient.CassandraV1beta2().CassandraClusters(f.Namespace).Get(testCassandra.Name, metav1.GetOptions{})
		if err != nil {
			e2eutil.LogfWithTimestamp(t, "failed to get updated cluster object: %v", err)
			return false, nil
		}
		if len(currCassandra.Status.Members.Ready) != size {
			e2eutil.LogfWithTimestamp(t, "size of ready members want = %d, get = %d ReadyMembers(%v) UnreadyMembers(%v). Will retry checking ReadyMembers", size, len(currCassandra.Status.Members.Ready), currCassandra.Status.Members.Ready, currCassandra.Status.Members.Unready)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("failed to get size of ReadyMembers to reach %d : %v", size, err)
	}
}
//
//func TestBackupStatus(t *testing.T) {
//	if os.Getenv(envParallelTest) == envParallelTestTrue {
//		t.Parallel()
//	}
//	f := framework.Global
//
//	bp := e2eutil.NewPVBackupPolicy(true, "")
//	testCassandra, err := e2eutil.CreateCluster(t, f.CRClient, f.Namespace, e2eutil.ClusterWithBackup(e2eutil.NewCluster("test-Cassandra-", 1), bp))
//	if err != nil {
//		t.Fatal(err)
//	}
//	defer func() {
//		var storageCheckerOptions *e2eutil.StorageCheckerOptions
//		switch testCassandra.Spec.Backup.StorageType {
//		case api.BackupStorageTypePersistentVolume, api.BackupStorageTypeDefault:
//			storageCheckerOptions = &e2eutil.StorageCheckerOptions{}
//		case api.BackupStorageTypeS3:
//			storageCheckerOptions = &e2eutil.StorageCheckerOptions{
//				S3Cli:    f.S3Cli,
//				S3Bucket: f.S3Bucket,
//			}
//		}
//
//		err := e2eutil.DeleteClusterAndBackup(t, f.CRClient, f.KubeClient, testCassandra, *storageCheckerOptions)
//		if err != nil {
//			t.Fatal(err)
//		}
//	}()
//
//	_, err = e2eutil.WaitUntilSizeReached(t, f.CRClient, 1, 6, testCassandra)
//	if err != nil {
//		t.Fatalf("failed to create 1 members Cassandra cluster: %v", err)
//	}
//	err = e2eutil.WaitBackupPodUp(t, f.KubeClient, f.Namespace, testCassandra.Name, 6)
//	if err != nil {
//		t.Fatalf("failed to create backup pod: %v", err)
//	}
//	err = e2eutil.MakeBackup(f.KubeClient, f.Namespace, testCassandra.Name)
//	if err != nil {
//		t.Fatalf("fail to make backup: %v", err)
//	}
//
//	err = retryutil.Retry(5*time.Second, 6, func() (done bool, err error) {
//		c, err := f.CRClient.CassandraV1beta2().CassandraClusters(f.Namespace).Get(testCassandra.Name, metav1.GetOptions{})
//		if err != nil {
//			t.Fatalf("faied to get cluster spec: %v", err)
//		}
//		bs := c.Status.BackupServiceStatus
//		if bs == nil {
//			e2eutil.LogfWithTimestamp(t, "backup status is nil")
//			return false, nil
//		}
//		// We expect it will make one backup eventually.
//		if bs.Backups < 1 {
//			e2eutil.LogfWithTimestamp(t, "backup number is %v", bs.Backups)
//			return false, nil
//		}
//		if bs.BackupSize == 0 {
//			return false, fmt.Errorf("backupsize = 0, want > 0")
//		}
//		return true, nil
//	})
//
//	if err != nil {
//		t.Error(err)
//	}
//}
