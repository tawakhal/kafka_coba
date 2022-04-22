package kafkacoba_test

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"testing"

	"github.com/Shopify/sarama"
)

var (
	brokerList []string = []string{"localhost:9092"}
)

func newError(funcName, whatTodo, errorMessage string) error {
	return fmt.Errorf("%s:%s => %s", funcName, whatTodo, errorMessage)
}

func newSyncPublisher(brokerList []string, tlsConfig *tls.Config) (sarama.SyncProducer, error) {
	const (
		funcName = `newSyncPublisher`
	)
	// For the data collector, we are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true
	if tlsConfig != nil {
		config.Net.TLS.Config = tlsConfig
		config.Net.TLS.Enable = true
	}

	// On the broker side, you may want to change the following settings to get
	// stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return nil, newError(funcName, "newSyncProducer", err.Error())
	}

	return producer, nil
}

func newConsumer(brokerList []string, groupID string, tlsConfig *tls.Config) (sarama.Consumer, error) {
	const funcName = `newSubscriber`

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true
	if tlsConfig != nil {
		config.Net.TLS.Config = tlsConfig
		config.Net.TLS.Enable = true
	}

	consumer, err := sarama.NewConsumerGroup(brokerList, groupID, config)
	if err != nil {
		return nil, newError(funcName, "newConsumer", err.Error())
	}

	return consumer, nil
}

func createTlsConfiguration(certFile, keyFile, caFile string, verifySSL bool) (t *tls.Config, err error) {
	const (
		funcName = `createTlsConfiguration`
	)
	if certFile != "" && keyFile != "" && caFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, newError(funcName, "LoadX509KeyPair", err.Error())
		}

		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, newError(funcName, "ReadFile", err.Error())
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: verifySSL,
		}
	}
	// will be nil by default if nothing is provided
	return t, nil
}

func Test_newSyncPublisherNoTLS(t *testing.T) {
	const funcName = `Test_newSyncPublisherNoTLS`

	pub, err := newSyncPublisher(brokerList, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	t.Logf("%s: Successfully created a new producer : %+v", funcName, pub)
}

func Test_newSyncPublisherTLS(t *testing.T) {
	const funcName = `Test_newSyncPublisherTLS`

	_tls, err := createTlsConfiguration("./cert/client.pem", "./cert/client.key", "./cert/ca.pem", true)
	if err != nil {
		t.Fatal(err)
	}

	pub, err := newSyncPublisher(brokerList, _tls)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	t.Logf("%s: Successfully created a new producer : %+v", funcName, pub)
}

func Test_newConsumerNoTLS(t *testing.T) {
	const (
		funcName = `Test_newConsumer`
		topic    = "kafka_coba"
	)

	conClient, err := newConsumer(brokerList, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conClient.Close()

	sub, err := conClient.ConsumePartition("topic", 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

}
