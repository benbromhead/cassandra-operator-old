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
	//"fmt"

	api "github.com/benbromhead/cassandra-operator/pkg/apis/cassandra/v1beta2"
	//"github.com/benbromhead/cassandra-operator/pkg/util/cassandrautil"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	cassandraVolumeName = "cassandra-data"
)

func cassandraVolumeMounts() []v1.VolumeMount {
	return []v1.VolumeMount{
		{Name: cassandraVolumeName, MountPath: cassandraVolumeMountDir},
	}
}

func cassandraContainer(baseImage, version string) v1.Container {
	c := v1.Container{
		//Command: []string{"/bin/sh", "-ec", commands},
		Name:    "cassandra",
		Image:   ImageName(baseImage, version),
		Ports: []v1.ContainerPort{
			{
				Name:          "intra",
				ContainerPort: int32(7000),
				Protocol:      v1.ProtocolTCP,
			},
			{
				Name:          "tls",
				ContainerPort: int32(7001),
				Protocol:      v1.ProtocolTCP,
			},
			{
				Name:          "jmx",
				ContainerPort: int32(7199),
				Protocol:      v1.ProtocolTCP,
			},
			{
				Name:          "cql",
				ContainerPort: int32(9042),
				Protocol:      v1.ProtocolTCP,
			},
		},
		VolumeMounts: cassandraVolumeMounts(),

	}

	return c
}

func containerWithLivenessProbe(c v1.Container) v1.Container {
	c.LivenessProbe = cassandraLivenessProbe()
	c.ReadinessProbe = cassandraReadinessProbe()
	return c
}

func containerWithRequirements(c v1.Container, r v1.ResourceRequirements) v1.Container {
	c.Resources = r
	return c
}

func cassandraLivenessProbe() *v1.Probe {
	return &v1.Probe{
		Handler: v1.Handler{
			Exec: &v1.ExecAction{
				Command: []string{"/bin/sh", "-c", "nodetool status"},
			},
		},
		InitialDelaySeconds: 90,
		TimeoutSeconds:      10,
		PeriodSeconds:       60,
		FailureThreshold:    3,
	}
}

func cassandraReadinessProbe() *v1.Probe {
	return &v1.Probe{
		Handler: v1.Handler{
			Exec: &v1.ExecAction{
				Command: []string{"/bin/sh", "-c", "nodetool status | grep -E \"^UN\\s+${POD_IP}\""},
			},
		},
		InitialDelaySeconds: 90,
		TimeoutSeconds:      10,
		PeriodSeconds:       60,
		FailureThreshold:    3,
	}
}

func PodWithAntiAffinity(pod *v1.Pod, clusterName string) *v1.Pod {
	// set pod anti-affinity with the pods that belongs to the same etcd cluster
	ls := &metav1.LabelSelector{MatchLabels: map[string]string{
		"etcd_cluster": clusterName,
	}}
	return podWithAntiAffinity(pod, ls)
}

func podWithAntiAffinity(pod *v1.Pod, ls *metav1.LabelSelector) *v1.Pod {
	affinity := &v1.Affinity{
		PodAntiAffinity: &v1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
				{
					LabelSelector: ls,
					TopologyKey:   "kubernetes.io/hostname",
				},
			},
		},
	}

	pod.Spec.Affinity = affinity
	return pod
}

func applyPodPolicy(clusterName string, pod *v1.Pod, policy *api.PodPolicy) {
	if policy == nil {
		return
	}

	if policy.AntiAffinity {
		pod = PodWithAntiAffinity(pod, clusterName)
	}

	if len(policy.NodeSelector) != 0 {
		pod = PodWithNodeSelector(pod, policy.NodeSelector)
	}
	if len(policy.Tolerations) != 0 {
		pod.Spec.Tolerations = policy.Tolerations
	}
	if policy.AutomountServiceAccountToken != nil {
		pod.Spec.AutomountServiceAccountToken = policy.AutomountServiceAccountToken
	}

	mergeLabels(pod.Labels, policy.Labels)

	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "etcd" {
			pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, policy.EtcdEnv...)
		}
	}
}

// only used for backup pod.
func applyPodPolicyToPodTemplateSpec(clusterName string, pod *v1.PodTemplateSpec, policy *api.PodPolicy) {
	if policy == nil {
		return
	}

	// TODO: anti-affinity for backup pod?

	if len(policy.NodeSelector) != 0 {
		pod.Spec.NodeSelector = policy.NodeSelector
	}
	if len(policy.Tolerations) != 0 {
		pod.Spec.Tolerations = policy.Tolerations
	}
	if policy.AutomountServiceAccountToken != nil {
		pod.Spec.AutomountServiceAccountToken = policy.AutomountServiceAccountToken
	}

	mergeLabels(pod.Labels, policy.Labels)
}

// IsPodReady returns false if the Pod Status is nil
func IsPodReady(pod *v1.Pod) bool {
	condition := getPodReadyCondition(&pod.Status)
	return condition != nil && condition.Status == v1.ConditionTrue
}

func getPodReadyCondition(status *v1.PodStatus) *v1.PodCondition {
	for i := range status.Conditions {
		if status.Conditions[i].Type == v1.PodReady {
			return &status.Conditions[i]
		}
	}
	return nil
}

func PodSpecToPrettyJSON(pod *v1.Pod) (string, error) {
	bytes, err := json.MarshalIndent(pod.Spec, "", "    ")
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
