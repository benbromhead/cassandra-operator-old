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
	//"fmt"
	"path"
	"strings"

	api "github.com/benbromhead/cassandra-operator/pkg/apis/cassandra/v1beta2"
	"github.com/benbromhead/cassandra-operator/pkg/util/cassandrautil"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	shouldCheckpointAnnotation = "checkpointer.alpha.coreos.com/checkpoint" // = "true"
	varLockVolumeName          = "var-lock"
	varLockDir                 = "/var/lock"
	etcdLockPath               = "/var/lock/etcd.lock"
)

func selfHostedDataDir(ns, name string) string {
	return path.Join(cassandraVolumeMountDir, ns+"-"+name)
}

func NewSelfHostedCassandraPod(m *cassandrautil.Member, seeds []string, clusterName, state string, cs api.ClusterSpec, owner metav1.OwnerReference) *v1.Pod {

	labels := map[string]string{
		"app":          "etcd",
		"etcd_node":    m.Name,
		"etcd_cluster": clusterName,
	}

	seedUrl := ""

	if state == "new" {
		seedUrl = m.Name
	} else {
		if len(seeds) <=2 {
			seedUrl = strings.Join( seeds, ",")
		} else {
			seedUrl = strings.Join(seeds[0:2], ",")
		}
	}

	c := cassandraContainer(cs.BaseImage, cs.Version)
	// On node reboot, there will be two copies of etcd pod: scheduled and checkpointed one.
	// Checkpointed one will start first. But then the scheduler will detect host port conflict,
	// and set the pod (in APIServer) failed. This further affects etcd service by removing the endpoints.
	// To make scheduling phase succeed, we work around by removing ports in spec.
	// However, the scheduled pod will fail when running on node because resources (e.g. host port) are taken.
	// Thus, we make etcd pod flock first before starting etcd server.
	c.Ports = nil
	c.VolumeMounts = append(c.VolumeMounts, v1.VolumeMount{
		Name:      varLockVolumeName,
		MountPath: varLockDir,
		ReadOnly:  false,
	})
	if cs.Pod != nil {
		c = containerWithRequirements(c, cs.Pod.Resources)
	}

	volumes := []v1.Volume{{
		Name: cassandraVolumeName,
		// TODO: configurable mount host path.
		VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: cassandraVolumeMountDir}},
	}, {
		Name:         varLockVolumeName,
		VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: varLockDir}},
	}}
	if m.SecurePeer {
		c.VolumeMounts = append(c.VolumeMounts, v1.VolumeMount{
			MountPath: peerTLSDir,
			Name:      peerTLSVolume,
		})
		volumes = append(volumes, v1.Volume{Name: peerTLSVolume, VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{SecretName: cs.TLS.Static.Member.PeerSecret},
		}})
	}
	if m.SecureClient {
		c.VolumeMounts = append(c.VolumeMounts, v1.VolumeMount{
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

	c.Env = append(c.Env, v1.EnvVar{
		Name: "CASSANDRA_CLUSTER_NAME",
		Value: clusterName,
	}, v1.EnvVar{
		Name: "CASSANDRA_DC",
		Value: "",
	}, v1.EnvVar{
		Name: "POD_IP",
		ValueFrom: &v1.EnvVarSource{
			FieldRef: &v1.ObjectFieldSelector{
				FieldPath: "status.podIP",
			},
		},
	}, v1.EnvVar{
		Name: "CASSANDRA_SEEDS",
		Value: seedUrl,
	})

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   m.Name,
			Labels: labels,
			Annotations: map[string]string{
				shouldCheckpointAnnotation: "true",
			},
		},
		Spec: v1.PodSpec{
			// Self-hosted etcd pod need to endure node restart.
			// If we set it to Never, the pod won't restart. If etcd won't come up, nor does other k8s API components.
			RestartPolicy: v1.RestartPolicyOnFailure,
			Containers:    []v1.Container{c},
			Volumes:       volumes,
			HostNetwork:   true,
			DNSPolicy:     v1.DNSClusterFirstWithHostNet,
			Hostname:      m.Name,
			Subdomain:     clusterName,
		},
	}

	SetCassandraVersion(pod, cs.Version)

	applyPodPolicy(clusterName, pod, cs.Pod)
	// overwrites the antiAffinity setting for self hosted cluster.
	pod = selfHostedPodWithAntiAffinity(pod)
	addOwnerRefToObject(pod.GetObjectMeta(), owner)
	return pod
}

func selfHostedPodWithAntiAffinity(pod *v1.Pod) *v1.Pod {
	// self hosted pods should sit on different nodes even if they are from different cluster.
	ls := &metav1.LabelSelector{MatchLabels: map[string]string{
		"app": "etcd",
	}}
	return podWithAntiAffinity(pod, ls)
}
