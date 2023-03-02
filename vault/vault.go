package vault

import (
	"context"
	"fmt"

	hashivault "github.com/hashicorp/vault/api"
)

type Config struct {
	URL   string
	Token string
}

type Vault struct {
	client *hashivault.Client
	config Config
}

func NewVault(cfg Config) (*Vault, error) {
	config := hashivault.DefaultConfig()
	config.Address = cfg.URL

	client, err := hashivault.NewClient(config)
	if err != nil {
		return nil, err
	}

	client.SetToken(cfg.Token)
	vault := &Vault{
		client: client,
		config: cfg,
	}

	return vault, nil
}

func (v *Vault) GetSecret(path string) (*hashivault.KVSecret, error) {
	secret, err := v.client.KVv2("secret").Get(context.Background(), path)
	if err != nil {
		return nil, fmt.Errorf("unable to read secret from vault: %v", err)
	}

	return secret, nil
}
