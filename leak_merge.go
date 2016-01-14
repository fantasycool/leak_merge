package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
	"gzlog"
	"kafka_tool"
	"leak_merge/u_leak_merge"
	"log"
	"os"
	"strings"
	"time"
)

const (
	DefaultConsumerGroup = "leak-merge"
	MysqlUser            = ""
	MysqlPasswd          = ""
	MysqlHost            = "10.174.33.90:3306"
)

var (
	zookeeper = flag.String("zookeeper", "",
		"A comma-separated Zookeeper connection string (e.g. `zookeeper1.local:2181,zookeeper2.local:2181,zookeeper3.local:2181`)")
	brokerList    = flag.String("brokers", "localhost:9092", "The comma separated list of brokers in the Kafka cluster")
	consumerGroup = flag.String("group",
		DefaultConsumerGroup, "The name of the consumer group, used for coordination and load balancing")
	kafkaTopic     = flag.String("topic", "merge_event", "kafka topic to consume")
	errDisk        = flag.String("errdisk", "/home/yom/", "if flush failed where we persistence failed message!")
	LogFile        = flag.String("file_name", "leak_merge.log", "app_analyze log to write")
	zookeeperNodes []string
)

func init() {
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
}

func main() {
	flag.Parse()
	gzlog.InitGZLogger(*LogFile, 50*1000*1000, 5)
	if *kafkaTopic == "" {
		log.Printf("topicShould not be null!\n")
		return
	}
	if *zookeeper == "" {
		log.Printf("zookeeper should not be null! \n")
		return
	}
	if *brokerList == "" {
		log.Printf("kafka brokers must not be null\n")
	}
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 2 * time.Second
	config.Consumer.MaxProcessingTime = 2 * time.Second
	kafkaTopics := strings.Split(*kafkaTopic, ",")
	zookeeperNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(*zookeeper)
	consumer, consumerErr := consumergroup.JoinConsumerGroup(*consumerGroup, kafkaTopics, zookeeperNodes, config)
	if consumerErr != nil {
		log.Fatalln(consumerErr)
	}
	log.Printf("start to get mysl connection!\n")
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@(%s)/jwlwl?charset=utf8&parseTime=True", MysqlUser, MysqlPasswd, MysqlHost))
	defer db.Close()
	if err != nil {
		log.Printf("mysql db connect failed !errMessage:%s \n", err)
		return
	}
	log.Printf("start to get kafka producer\n")
	producer, err := kafka_tool.GetKafkaProducer(*brokerList)
	if err != nil {
		log.Printf("Kafka  get producer failed !err: %s \n", err)
		return
	}

	log.Printf("Start to call consummer messages method !\n")
	for message := range consumer.Messages() {
		log.Printf("Start to call Run method with message:%s \n", message.Value)
		latestLeakEventArg := &u_leak_merge.LatestEventArg{Properties: string(message.Value)}
		err := u_leak_merge.Run("0", latestLeakEventArg, db, producer)
		if err != nil {
			log.Printf("message failed!:%s, errMessage:%s \n", message.Value, err)
			continue
		}
		log.Printf("Start to commit message! \n")
		consumer.CommitUpto(message)
		time.Sleep(100 * time.Millisecond)
	}
}
