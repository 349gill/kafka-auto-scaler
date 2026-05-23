package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type config struct {
	kubeconfig    string
	namespace     string
	deployment    string
	brokers       []string
	topic         string
	group         string
	pollInterval  time.Duration
	maxReplicas   int32
	lagPerReplica int64
}

func loadConfig() config {
	return config{
		kubeconfig:    envString("KUBECONFIG", ""),
		namespace:     envString("TARGET_NAMESPACE", "default"),
		deployment:    envString("TARGET_DEPLOYMENT", "kafka-consumer"),
		brokers:       strings.Split(envString("KAFKA_BROKERS", "localhost:9092"), ","),
		topic:         envString("KAFKA_TOPIC", "orders"),
		group:         envString("KAFKA_CONSUMER_GROUP", "kafka-consumer-group"),
		pollInterval:  envDuration("POLL_INTERVAL", 10*time.Second),
		maxReplicas:   int32(envInt("MAX_REPLICAS", 5)),
		lagPerReplica: int64(envInt("LAG_PER_REPLICA", 100)),
	}
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("invalid value for %s: %v", key, err)
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("invalid value for %s: %v", key, err)
	}
	return d
}

func buildK8sClient(kubeconfigPath string) *kubernetes.Clientset {
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		restConfig, err = rest.InClusterConfig()
		if err != nil {
			log.Fatalf("failed to load kubernetes config: %v", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.Fatalf("failed to create kubernetes client: %v", err)
	}
	return clientset
}

func buildKafkaClient(brokers []string) (sarama.Client, sarama.ClusterAdmin) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.V2_8_0_0

	client, err := sarama.NewClient(brokers, kafkaConfig)
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Fatalf("failed to create kafka admin client: %v", err)
	}

	return client, admin
}

func desiredReplicas(lag int64, maxReplicas int32, lagPerReplica int64) int32 {
	if lag <= 0 {
		return 0
	}

	n := lag / lagPerReplica
	if lag%lagPerReplica != 0 {
		n++
	}
	if n < 1 {
		n = 1
	}
	if int32(n) > maxReplicas {
		return maxReplicas
	}
	return int32(n)
}

func reconcile(ctx context.Context, cfg config, clientset *kubernetes.Clientset, kafkaClient sarama.Client, admin sarama.ClusterAdmin) {
	lag, err := consumerGroupLag(kafkaClient, admin, cfg.group, cfg.topic)
	if err != nil {
		log.Printf("failed to compute consumer lag: %v", err)
		return
	}

	desired := desiredReplicas(lag, cfg.maxReplicas, cfg.lagPerReplica)

	current, err := currentReplicas(ctx, clientset, cfg.namespace, cfg.deployment)
	if err != nil {
		log.Printf("failed to read current replicas: %v", err)
		return
	}

	log.Printf("lag=%d current_replicas=%d desired_replicas=%d", lag, current, desired)

	if desired == current {
		return
	}

	if err := setReplicas(ctx, clientset, cfg.namespace, cfg.deployment, desired); err != nil {
		log.Printf("failed to scale deployment: %v", err)
		return
	}

	log.Printf("scaled %s/%s from %d to %d replicas", cfg.namespace, cfg.deployment, current, desired)
}

func main() {
	cfg := loadConfig()

	clientset := buildK8sClient(cfg.kubeconfig)
	kafkaClient, admin := buildKafkaClient(cfg.brokers)
	defer kafkaClient.Close()

	log.Printf("kafka-auto-scaler started: topic=%s group=%s target=%s/%s poll=%s max_replicas=%d lag_per_replica=%d",
		cfg.topic, cfg.group, cfg.namespace, cfg.deployment, cfg.pollInterval, cfg.maxReplicas, cfg.lagPerReplica)

	ctx := context.Background()
	ticker := time.NewTicker(cfg.pollInterval)
	defer ticker.Stop()

	reconcile(ctx, cfg, clientset, kafkaClient, admin)
	for range ticker.C {
		reconcile(ctx, cfg, clientset, kafkaClient, admin)
	}
}
