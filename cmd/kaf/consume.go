package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/Shopify/sarama"
	"github.com/birdayz/kaf/avro"
	"github.com/birdayz/kaf/pkg/proto"
	"github.com/golang/protobuf/jsonpb"
	"github.com/hokaccha/go-prettyjson"
	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
)

var (
	tailFlag    int
	tailsFlag   []int
	partitionsWhitelistFlag []int
	offsetFlag  string
	raw         bool
	follow      bool
	schemaCache *avro.SchemaCache
	keyfmt      *prettyjson.Formatter

	protoType string
)

func init() {
	rootCmd.AddCommand(consumeCmd)
	consumeCmd.Flags().StringVar(&offsetFlag, "offset", "oldest", "Offset to start consuming. Possible values: oldest, newest.")
	consumeCmd.Flags().IntVarP(&tailFlag, "tail", "t", 0, "Number of recent messages to try to read from topic. Overrides --offset flag.")
	consumeCmd.Flags().IntSliceVarP(&tailsFlag, "tails", "T", []int{}, "Number of recent messages to read from each partition. If only one value is provided, it will be used for each partition; otherwise you must provide a value for each partition. Overrides --offset flag.")
	consumeCmd.Flags().IntSliceVar(&partitionsWhitelistFlag, "partitions", []int{}, "Partitions to read from; if unset, all partitions will be read.")
	consumeCmd.Flags().BoolVar(&raw, "raw", false, "Print raw output of messages, without key or prettified JSON.")
	consumeCmd.Flags().BoolVarP(&follow, "follow", "f", false, "Shorthand to start consuming with offset HEAD-1 on each partition. Overrides --offset flag.")
	consumeCmd.Flags().StringSliceVar(&protoFiles, "proto-include", []string{}, "Path to proto files.")
	consumeCmd.Flags().StringSliceVar(&protoExclude, "proto-exclude", []string{}, "Proto exclusions (path prefixes).")
	consumeCmd.Flags().StringVar(&protoType, "proto-type", "", "Fully qualified name of the proto message type. Example: com.test.SampleMessage.")

	keyfmt = prettyjson.NewFormatter()
	keyfmt.Newline = " " // Replace newline with space to avoid condensed output.
	keyfmt.Indent = 0
}


func getOffsetRange(
	client sarama.Client, topic string, partition int32,
) (min int64, max int64, err error) {

	min, err = client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return
	}
	max, err = client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return
	}

	return
}

const (
	offsetsRetry       = 500 * time.Millisecond
	configProtobufType = "protobuf.type"
)

var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Consume messages",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		var allPartitionsOffset int64
		switch offsetFlag {
		case "oldest":
			allPartitionsOffset = sarama.OffsetOldest
		case "newest":
			allPartitionsOffset = sarama.OffsetNewest
		default:
			// TODO: normally we would parse this to int64 but it's
			// difficult as we can have multiple partitions. need to
			// find a way to give offsets from CLI with a good
			// syntax.
			allPartitionsOffset = sarama.OffsetNewest
		}



		topic := args[0]
		client := getClient()

		// admin := getClusterAdmin()

		// Fetch topic config to find out if we have some en/decoding settings
		// configEntries, err := admin.DescribeConfig(sarama.ConfigResource{
		// 	Type:        sarama.TopicResource,
		// 	Name:        topic,
		// 	ConfigNames: []string{configProtobufType},
		// })

		// for _, entry := range configEntries {
		// 	if entry.Name == configProtobufType {
		// 		protobufType = entry.Name
		// 		fmt.Printf("Detected protobuf.type=%v\n", protobufType)
		// 	}
		// }

		// if topic == "public.device.last-contact" {
		// 	fmt.Println("found type")
		// 	protobufType = "eon.iotcore.devicetwin.kafka.LastContact"
		// }

		consumer, err := sarama.NewConsumerFromClient(client)
		if err != nil {
			errorExit("Unable to create consumer from client: %v\n", err)
		}

		foundPartitions, err := consumer.Partitions(topic)
		if err != nil {
			errorExit("Unable to get partitions: %v\n", err)
		}

		// filter partitions
		var partitions []int
		if len(partitionsWhitelistFlag) > 0 {
			// filter the partitions but keep the order the user specified
			foundPartitionMap := make(map[int32]bool)
			for _, partition := range foundPartitions{
				foundPartitionMap[partition] = true
			}
			for _, partition := range partitionsWhitelistFlag {
				if !foundPartitionMap[int32(partition)] {
					errorExit("Requested partition %d was not found.", partition)
				}
				partitions = append(partitions, partition)
			}
		} else {
			for _, partition := range foundPartitions {
				partitions = append(partitions, int(partition))
			}
			// make sure partitions are sorted in a reasonable order if the user provided tails
			sort.Ints(partitions)
		}
		
		schemaCache = getSchemaCache()

		offsets := make(map[int]int64, len(partitions))
		offsetDeltas := make(map[int]int64, len(partitions))
		if tailFlag != 0 {
			if len(tailsFlag) > 0 {
				errorExit("The --tail and --tails flags are mutually exclusive, please choose one.")
			}
			avgDelta := int(math.Ceil(float64(tailFlag) / float64(len(partitions))))
			fmt.Fprintf(os.Stderr, "Distributing --tail flag of %d over %d partitions: subtracting %d per partition.\n", tailFlag, len(partitions), avgDelta)
			tailsFlag = []int{avgDelta}
		}

		switch len(tailsFlag) {
		case 0:
			if follow {
				fmt.Fprintf(os.Stderr, "Setting all partitions to max offset minus one because of --follow flag.\n")
				for _, partition := range partitions {
					offsetDeltas[partition] = 1
				}
			} else {
				for _, partition := range partitions {
					offsets[partition] = allPartitionsOffset
				}
			}
		case 1:
			delta := tailsFlag[0]
			fmt.Fprintf(os.Stderr, "Setting all partitions to max offset minus %d.\n", delta)
			for _, partition := range partitions {
				offsetDeltas[partition] = int64(delta)
			}
		case len(partitions):
			for i, partition := range partitions {
				delta := tailsFlag[i]
				fmt.Fprintf(os.Stderr, "Setting starting point for partition %d to max offset minus %d\n", partition, delta)
				offsetDeltas[partition] = int64(delta)
			}
		default:
			errorExit("The --tails flag had %d values, but there are %d partitions available.\n", len(tailsFlag), len(partitions))
		}


		wg := sync.WaitGroup{}
		mu := sync.Mutex{} // Synchronizes stderr and stdout.
		for _, partition := range partitions {

			wg.Add(1)

			go func(partition int32) {

				min, max, err := getOffsetRange(client, topic, partition)
				if err != nil {
					errorExit("Unable to get offsets for partition %d: %v.", partition, err)
				}

				offset, ok := offsets[int(partition)]
				if !ok {
					delta := offsetDeltas[int(partition)]
					offset = max - delta
					if offset < min {
						fmt.Fprintf(os.Stderr, "Subtracting tail argument of %d from newest offset %d on partition %v is earlier than the oldest offset %d, using oldest offset instead.\n", delta, max, partition, min)
						offset = min
					} else {
						fmt.Fprintf(os.Stderr, "Starting on partition %v with offset %v\n", partition, offset)
					}
				}

				pc, err := consumer.ConsumePartition(topic, partition, offset)
				if err != nil {
					if strings.Contains(err.Error(), "outside the range of offsets maintained") {
						fmt.Fprintf(os.Stderr, "Unable to consume partition %d starting at offset %d (reported offsets %d - %d); will use newest offset; error was: %v\n", partition, allPartitionsOffset, min, max, err)
						pc, err = consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
					}
					if err != nil {
						errorExit("Unable to consume partition %d starting at offset %d (reported offsets %d - %d): %v\n", partition, allPartitionsOffset, min, max, err)
					}
				}

				for msg := range pc.Messages() {
					var stderr bytes.Buffer

					// TODO make this nicer
					var dataToDisplay []byte
					if protoType != "" {
						dataToDisplay, err = protoDecode(msg.Value, protoType)
						if err != nil {
							fmt.Fprintf(&stderr, "failed to decode proto. falling back to binary output. Error: %v", err)
						}
					} else {
						dataToDisplay, err = avroDecode(msg.Value)
						if err != nil {
							fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
						}
					}

					if !raw {
						formatted, err := prettyjson.Format(dataToDisplay)
						if err == nil {
							dataToDisplay = formatted
						}

						w := tabwriter.NewWriter(&stderr, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

						if len(msg.Headers) > 0 {
							fmt.Fprintf(w, "Headers:\n")
						}

						for _, hdr := range msg.Headers {
							var hdrValue string
							// Try to detect azure eventhub-specific encoding
							if len(hdr.Value) > 0 {
								switch hdr.Value[0] {
								case 161:
									hdrValue = string(hdr.Value[2 : 2+hdr.Value[1]])
								case 131:
									hdrValue = strconv.FormatUint(binary.BigEndian.Uint64(hdr.Value[1:9]), 10)
								default:
									hdrValue = string(hdr.Value)
								}
							}

							fmt.Fprintf(w, "\tKey: %v\tValue: %v\n", string(hdr.Key), hdrValue)

						}

						if msg.Key != nil && len(msg.Key) > 0 {

							key, err := avroDecode(msg.Key)
							if err != nil {
								fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
							}
							fmt.Fprintf(w, "Key:\t%v\n", formatKey(key))
						}
						fmt.Fprintf(w, "Partition:\t%v\nOffset:\t%v\nTimestamp:\t%v\n", msg.Partition, msg.Offset, msg.Timestamp)
						w.Flush()
					}

					mu.Lock()
					stderr.WriteTo(os.Stderr)
					colorable.NewColorableStdout().Write(dataToDisplay)
					fmt.Print("\n")
					mu.Unlock()
				}
				wg.Done()
			}(int32(partition))
		}
		wg.Wait()

	},
}

// proto to JSON
func protoDecode(b []byte, _type string) ([]byte, error) {
	reg, err := proto.NewDescriptorRegistry(protoFiles, protoExclude)
	if err != nil {
		return nil, err
	}

	dynamicMessage := reg.MessageForType(_type)
	if dynamicMessage == nil {
		return b, nil
	}

	dynamicMessage.Unmarshal(b)

	var m jsonpb.Marshaler
	var w bytes.Buffer

	err = m.Marshal(&w, dynamicMessage)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil

}

func avroDecode(b []byte) ([]byte, error) {
	if schemaCache != nil {
		return schemaCache.DecodeMessage(b)
	}
	return b, nil
}

func formatKey(key []byte) string {
	b, err := keyfmt.Format(key)
	if err != nil {
		return string(key)
	}
	return string(b)
}
