package producer

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"github.com/IBM/sarama"
	"github.com/xdg/scram"
	"hash"
	"log"
	"os"
	"strings"
)

func NewSyncProducer(addrs string) (sarama.SyncProducer, error) {
	return sarama.NewSyncProducer(strings.Split(addrs, ","), getConfig())
}

func NewASyncProducer(addrs string) (sarama.AsyncProducer, error) {
	p, err := sarama.NewAsyncProducer(strings.Split(addrs, ","), getConfig())
	if err != nil {
		return nil, err
	}
	go func() {
		for err := range p.Errors() {
			log.Println("Failed to write access log entry:", err)
		}
	}()
	go func() {
		for range p.Successes() {
			//log.Println("sss", st.Key)
		}
	}()

	return p, nil
}

func getConfig() *sarama.Config {
	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true

	//conf.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	//conf.Producer.Flush.Bytes = 10
	//conf.Producer.RequiredAcks = sarama.NoResponse
	//conf.Producer.Flush.Messages = 0
	//conf.Producer.Flush.MaxMessages = 1
	//conf.Producer.Flush.Bytes = 1
	//conf.Producer.Flush.Frequency = 0
	//conf.Producer.Flush.Messages = 1
	//conf.Producer.Retry.Max = 0

	//conf.Net.SASL.Handshake = true
	//conf.Net.SASL.Password = "2ckP-XMvsZl6je"
	//conf.Net.SASL.User = "asttest"
	//conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
	//conf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	//conf.Net.SASL.Enable = true

	ssl := false
	if ssl {
		certs := x509.NewCertPool()
		pemPath := "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"
		pemData, err := os.ReadFile(pemPath)
		if err != nil {
			panic("Couldn't load cert: " + err.Error())
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
