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
	"strings"
)

func NewConsumerGroup(addr, consumerGroup string) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(strings.Split(addr, ","), consumerGroup, initConf())
}

func NewConsumer(addr string) (sarama.Consumer, error) {
	return sarama.NewConsumer(strings.Split(addr, ","), initConf())
}

func initConf() *sarama.Config {
	conf := sarama.NewConfig()
	conf.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	conf.Consumer.Return.Errors = true
	conf.Metadata.Full = true

	//conf.Net.SASL.Enable = true
	//conf.Net.SASL.Password = "2ckP-XMvsZl6je"
	//conf.Net.SASL.User = "asttest"
	//conf.Net.SASL.Handshake = true
	//conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
	//conf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512

	ssl := false
	if ssl {
		certs := x509.NewCertPool()
		pemPath := "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"
		pemData, err := os.ReadFile(pemPath)
		if err != nil {
			fmt.Println("Couldn't load cert: ", err.Error())
			panic(err)
		}
		certs.AppendCertsFromPEM(pemData)

		conf.Net.TLS.Enable = true
		conf.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: true,
			RootCAs:            certs,
		}
	}

	return conf
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
