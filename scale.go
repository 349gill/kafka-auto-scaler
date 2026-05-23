package main

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func currentReplicas(ctx context.Context, clientset *kubernetes.Clientset, namespace, name string) (int32, error) {
	scale, err := clientset.AppsV1().Deployments(namespace).GetScale(ctx, name, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	return scale.Spec.Replicas, nil
}

func setReplicas(ctx context.Context, clientset *kubernetes.Clientset, namespace, name string, replicas int32) error {
	scale, err := clientset.AppsV1().Deployments(namespace).GetScale(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	scale.Spec.Replicas = replicas
	_, err = clientset.AppsV1().Deployments(namespace).UpdateScale(ctx, name, scale, metav1.UpdateOptions{})
	return err
}
