package consumer

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/xdg/scram"
	"hash"
	"os"
	"strconv"
	"strings"
)

func NewConsumerGroup(addr, consumerGroup string) (sarama.ConsumerGroup, error) {
	conf, err := newConfig()
	if err != nil {
		return nil, err
	}

	return sarama.NewConsumerGroup(strings.Split(addr, ","), consumerGroup, conf)
}

func NewConsumer(addr string) (sarama.Consumer, error) {
	conf, err := newConfig()
	if err != nil {
		return nil, err
	}

	return sarama.NewConsumer(strings.Split(addr, ","), conf)
}

func newConfig() (*sarama.Config, error) {
	conf := sarama.NewConfig()
	conf.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	conf.Consumer.Return.Errors = true

	if ssl, _ := strconv.ParseBool(os.Getenv("KAFKA_ENABLE_SSL")); ssl {
		conf.Net.SASL.Enable = true
		conf.Net.SASL.Password = os.Getenv("KAFKA_USER")
		conf.Net.SASL.User = os.Getenv("KAFKA_PASSWORD")
		conf.Net.SASL.Handshake = true
		conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
		conf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512

		certs := x509.NewCertPool()
		pemPath := os.Getenv("KAFKA_CA") // "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"
		pemData, err := os.ReadFile(pemPath)
		if err != nil {
			return nil, fmt.Errorf("couldn't load cert: %w", err)
		}
		certs.AppendCertsFromPEM(pemData)

		conf.Net.TLS.Enable = true
		conf.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: true,
			RootCAs:            certs,
		}
	}

	return conf, nil
}

var SHA256 scram.HashGeneratorFcn = func() hash.Hash { return sha256.New() }
var SHA512 scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
