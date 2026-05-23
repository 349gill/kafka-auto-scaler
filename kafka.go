package main

import "github.com/IBM/sarama"

func consumerGroupLag(client sarama.Client, admin sarama.ClusterAdmin, group, topic string) (int64, error) {
	partitions, err := client.Partitions(topic)
	if err != nil {
		return 0, err
	}

	offsets, err := admin.ListConsumerGroupOffsets(group, map[string][]int32{topic: partitions})
	if err != nil {
		return 0, err
	}

	var totalLag int64
	for _, partition := range partitions {
		committed := int64(0)
		if block := offsets.GetBlock(topic, partition); block != nil && block.Offset >= 0 {
			committed = block.Offset
		}

		latest, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return 0, err
		}

		if lag := latest - committed; lag > 0 {
			totalLag += lag
		}
	}

	return totalLag, nil
}
