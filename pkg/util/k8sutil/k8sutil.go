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

package k8sutil

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	api "github.com/benbromhead/cassandra-operator/pkg/apis/cassandra/v1beta2"
	"github.com/benbromhead/cassandra-operator/pkg/util/cassandrautil"
	"github.com/benbromhead/cassandra-operator/pkg/util/retryutil"

	"github.com/coreos/etcd-operator/pkg/backup/backupapi"
	appsv1beta1 "k8s.io/api/apps/v1beta1"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp" // for gcp auth
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	cassandraVolumeMountDir  = "/var/cassandra"
	dataDir                  = cassandraVolumeMountDir + "/data"
	etcdVersionAnnotationKey = "cassandra.version"
	peerTLSDir               = "/etc/cassandra/keystores/client"
	peerTLSVolume            = "member-peer-tls"
	serverTLSDir             = "/etc/cassandra/keystores/server"
	serverTLSVolume          = "member-server-tls"
	operatorEtcdTLSDir       = "/etc/cassandra/operator/etcd-tls"
	operatorEtcdTLSVolume    = "cassandra-client-tls"
)

const TolerateUnreadyEndpointsAnnotation = "service.alpha.kubernetes.io/tolerate-unready-endpoints"

func GetEtcdVersion(pod *v1.Pod) string {
	return pod.Annotations[etcdVersionAnnotationKey]
}

func SetCassandraVersion(pod *v1.Pod, version string) {
	pod.Annotations[etcdVersionAnnotationKey] = version
}

func GetPodNames(pods []*v1.Pod) []string {
	if len(pods) == 0 {
		return nil
	}
	res := []string{}
	for _, p := range pods {
		res = append(res, p.Name)
	}
	return res
}

func makeRestoreInitContainers(backupAddr, token, baseImage, version string, m *cassandrautil.Member) []v1.Container {
	return []v1.Container{
		{
			Name:  "fetch-backup",
			Image: "tutum/curl",
			Command: []string{
				"/bin/sh", "-ec",
				fmt.Sprintf("curl -o %s %s", "backupFile", backupapi.NewBackupURL("http", backupAddr, version, -1)),
			},
			VolumeMounts: cassandraVolumeMounts(),
		},
		{
			Name:  "restore-datadir",
			Image: ImageName(baseImage, version),
			Command: []string{
				"/bin/sh", "-ec",
				fmt.Sprintf("ETCDCTL_API=3 etcdctl snapshot restore %[1]s"+
					" --name %[2]s"+
					" --initial-cluster %[2]s=%[3]s"+
					" --initial-cluster-token %[4]s"+
					" --initial-advertise-peer-urls %[3]s"+
					" --data-dir %[5]s", "backupFile", m.Name, m.PeerURL(), token, dataDir),
			},
			VolumeMounts: cassandraVolumeMounts(),
		},
	}
}

func ImageName(baseImage, version string) string {
	return fmt.Sprintf("%s:%v", baseImage, version)
}

func PodWithNodeSelector(p *v1.Pod, ns map[string]string) *v1.Pod {
	p.Spec.NodeSelector = ns
	return p
}

func CreateClientService(kubecli kubernetes.Interface, clusterName, ns string, owner metav1.OwnerReference) error {
	ports := []v1.ServicePort{{
		Name:       "client",
		Port:       9042,
		TargetPort: intstr.FromInt(9042),
		Protocol:   v1.ProtocolTCP,
	}}
	return createService(kubecli, ClientServiceName(clusterName), clusterName, ns, "", ports, owner)
}

func ClientServiceName(clusterName string) string {
	return clusterName + "-client"
}

func CreatePeerService(kubecli kubernetes.Interface, clusterName, ns string, owner metav1.OwnerReference) error {
	ports := []v1.ServicePort{{
		Name:       "client",
		Port:       9042,
		TargetPort: intstr.FromInt(9042),
		Protocol:   v1.ProtocolTCP,
	}, {
		Name:       "peer",
		Port:       7001,
		TargetPort: intstr.FromInt(7001),
		Protocol:   v1.ProtocolTCP,
	}}

	return createService(kubecli, clusterName, clusterName, ns, v1.ClusterIPNone, ports, owner)
}

func createService(kubecli kubernetes.Interface, svcName, clusterName, ns, clusterIP string, ports []v1.ServicePort, owner metav1.OwnerReference) error {
	svc := newEtcdServiceManifest(svcName, clusterName, clusterIP, ports)
	addOwnerRefToObject(svc.GetObjectMeta(), owner)
	_, err := kubecli.CoreV1().Services(ns).Create(svc)
	return err
}

// CreateAndWaitPod is a workaround for self hosted and util for testing.
// We should eventually get rid of this in critical code path and move it to test util.
func CreateAndWaitPod(kubecli kubernetes.Interface, ns string, pod *v1.Pod, timeout time.Duration) (*v1.Pod, error) {
	_, err := kubecli.CoreV1().Pods(ns).Create(pod)
	if err != nil {
		return nil, err
	}

	interval := 5 * time.Second
	var retPod *v1.Pod
	err = retryutil.Retry(interval, int(timeout/(interval)), func() (bool, error) {
		retPod, err = kubecli.CoreV1().Pods(ns).Get(pod.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		switch retPod.Status.Phase {
		case v1.PodRunning:
			return true, nil
		case v1.PodPending:
			return false, nil
		default:
			return false, fmt.Errorf("unexpected pod status.phase: %v", retPod.Status.Phase)
		}
	})

	if err != nil {
		if retryutil.IsRetryFailure(err) {
			return nil, fmt.Errorf("failed to wait pod running, it is still pending: %v", err)
		}
		return nil, fmt.Errorf("failed to wait pod running: %v", err)
	}

	return retPod, nil
}

func newEtcdServiceManifest(svcName, clusterName, clusterIP string, ports []v1.ServicePort) *v1.Service {
	labels := LabelsForCluster(clusterName)
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:   svcName,
			Labels: labels,
			Annotations: map[string]string{
				TolerateUnreadyEndpointsAnnotation: "false",
			},
		},
		Spec: v1.ServiceSpec{
			Ports:     ports,
			Selector:  labels,
			ClusterIP: clusterIP,
		},
	}
	return svc
}

func AddRecoveryToPod(pod *v1.Pod, clusterName, token string, m *cassandrautil.Member, cs api.ClusterSpec) {
	pod.Spec.InitContainers = makeRestoreInitContainers(
		BackupServiceAddr(clusterName), token, cs.BaseImage, cs.Version, m)
}

func addOwnerRefToObject(o metav1.Object, r metav1.OwnerReference) {
	o.SetOwnerReferences(append(o.GetOwnerReferences(), r))
}

func NewCassandraPod(m *cassandrautil.Member, seeds []string, clusterName string, state string, cs api.ClusterSpec, owner metav1.OwnerReference) *v1.Pod {
	labels := map[string]string{
		"app":               "cassandra",
		"cassandra_node":    m.Name,
		"cassandra_cluster": clusterName,
	}

	container := containerWithLivenessProbe(cassandraContainer(cs.BaseImage, cs.Version))

	if cs.Pod != nil {
		container = containerWithRequirements(container, cs.Pod.Resources)
	}

	seedUrl := ""

	if state == "new" {
		seedUrl = m.Name
	} else {
		if len(seeds) <= 2 {
			seedUrl = strings.Join(seeds, ",")
		} else {
			seedUrl = strings.Join(seeds[0:1], ",")
		}
	}

	volumes := []v1.Volume{
		{Name: "cassandra-data", VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}},
	}

	container.Env = append(container.Env, v1.EnvVar{
		Name:  "CASSANDRA_CLUSTER_NAME",
		Value: clusterName,
	}, v1.EnvVar{
		Name:  "CASSANDRA_DC",
		Value: "",
	}, v1.EnvVar{
		Name:  "MAX_HEAP_SIZE",
		Value: "400M",
	}, v1.EnvVar{
		Name:  "HEAP_NEWSIZE",
		Value: "100M",
	}, v1.EnvVar{
		Name: "POD_IP",
		ValueFrom: &v1.EnvVarSource{
			FieldRef: &v1.ObjectFieldSelector{
				FieldPath: "status.podIP",
			},
		},
	}, v1.EnvVar{
		Name:  "CASSANDRA_SEEDS",
		Value: seedUrl,
	})

	if m.SecurePeer {
		container.VolumeMounts = append(container.VolumeMounts, v1.VolumeMount{
			MountPath: peerTLSDir,
			Name:      peerTLSVolume,
		})
		volumes = append(volumes, v1.Volume{Name: peerTLSVolume, VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{SecretName: cs.TLS.Static.Member.PeerSecret},
		}})
	}
	if m.SecureClient {
		container.VolumeMounts = append(container.VolumeMounts, v1.VolumeMount{
			MountPath: serverTLSDir,
			Name:      serverTLSVolume,
		}, v1.VolumeMount{
			MountPath: operatorEtcdTLSDir,
			Name:      operatorEtcdTLSVolume,
		})
		volumes = append(volumes, v1.Volume{Name: serverTLSVolume, VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{SecretName: cs.TLS.Static.Member.ServerSecret},
		}}, v1.Volume{Name: operatorEtcdTLSVolume, VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{SecretName: cs.TLS.Static.OperatorSecret},
		}})
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        m.Name,
			Labels:      labels,
			Annotations: map[string]string{},
		},
		Spec: v1.PodSpec{
			Containers:    []v1.Container{container},
			RestartPolicy: v1.RestartPolicyOnFailure,
			Volumes:       volumes,
			// DNS A record: `[m.Name].[clusterName].Namespace.svc`
			// For example, etcd-0000 in default namesapce will have DNS name
			// `etcd-0000.etcd.default.svc`.
			Hostname:  m.Name,
			Subdomain: clusterName,
		},
	}

	applyPodPolicy(clusterName, pod, cs.Pod)

	SetCassandraVersion(pod, cs.Version)

	addOwnerRefToObject(pod.GetObjectMeta(), owner)
	return pod
}

func GetClientConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func MustNewKubeClient(kubeconfig string) kubernetes.Interface {
	cfg, err := GetClientConfig(kubeconfig)
	if err != nil {
		panic(err)
	}
	return kubernetes.NewForConfigOrDie(cfg)
}

func InClusterConfig() (*rest.Config, error) {
	// Work around https://github.com/kubernetes/kubernetes/issues/40973
	// See https://github.com/benbromhead/cassandra-operator/issues/731#issuecomment-283804819
	if len(os.Getenv("KUBERNETES_SERVICE_HOST")) == 0 {
		addrs, err := net.LookupHost("kubernetes.default.svc")
		if err != nil {
			panic(err)
		}
		os.Setenv("KUBERNETES_SERVICE_HOST", addrs[0])
	}
	if len(os.Getenv("KUBERNETES_SERVICE_PORT")) == 0 {
		os.Setenv("KUBERNETES_SERVICE_PORT", "443")
	}
	return rest.InClusterConfig()
}

func IsKubernetesResourceAlreadyExistError(err error) bool {
	return apierrors.IsAlreadyExists(err)
}

func IsKubernetesResourceNotFoundError(err error) bool {
	return apierrors.IsNotFound(err)
}

// We are using internal api types for cluster related.
func ClusterListOpt(clusterName string) metav1.ListOptions {
	return metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(LabelsForCluster(clusterName)).String(),
	}
}

func LabelsForCluster(clusterName string) map[string]string {
	return map[string]string{
		"cassandra_cluster": clusterName,
		"app":               "cassandra",
	}
}

func CreatePatch(o, n, datastruct interface{}) ([]byte, error) {
	oldData, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	newData, err := json.Marshal(n)
	if err != nil {
		return nil, err
	}
	return strategicpatch.CreateTwoWayMergePatch(oldData, newData, datastruct)
}

func PatchDeployment(kubecli kubernetes.Interface, namespace, name string, updateFunc func(*appsv1beta1.Deployment)) error {
	od, err := kubecli.AppsV1beta1().Deployments(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	nd := od.DeepCopy()
	updateFunc(nd)
	patchData, err := CreatePatch(od, nd, appsv1beta1.Deployment{})
	if err != nil {
		return err
	}
	_, err = kubecli.AppsV1beta1().Deployments(namespace).Patch(name, types.StrategicMergePatchType, patchData)
	return err
}

func CascadeDeleteOptions(gracePeriodSeconds int64) *metav1.DeleteOptions {
	return &metav1.DeleteOptions{
		GracePeriodSeconds: func(t int64) *int64 { return &t }(gracePeriodSeconds),
		PropagationPolicy: func() *metav1.DeletionPropagation {
			foreground := metav1.DeletePropagationForeground
			return &foreground
		}(),
	}
}

// mergeLables merges l2 into l1. Conflicting label will be skipped.
func mergeLabels(l1, l2 map[string]string) {
	for k, v := range l2 {
		if _, ok := l1[k]; ok {
			continue
		}
		l1[k] = v
	}
}
