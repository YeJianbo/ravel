package panel

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/YeJianbo/ravel/conf"
	"github.com/go-resty/resty/v2"
)

const ravelServerCapability = "ravel_server_v1"
const ravelOverlapCapability = "ravel_credential_overlap_v1"

// Panel is the interface for different panel's api.

type Client struct {
	client           *resty.Client
	APIHost          string
	Token            string
	NodeId           int
	nodeEtag         string
	userEtag         string
	responseBodyHash string
	UserList         *UserListBody
	AliveMap         *AliveMap
}

type panelRequestError struct {
	cause   error
	message string
}

func (e *panelRequestError) Error() string {
	return e.message
}

func (e *panelRequestError) Unwrap() error {
	return e.cause
}

func New(c *conf.NodeConfig) (*Client, error) {
	client := resty.New()
	transport, err := client.Transport()
	if err != nil {
		return nil, err
	}
	transport.MaxIdleConns = 4
	transport.MaxIdleConnsPerHost = 2
	transport.MaxResponseHeaderBytes = 64 << 10
	retryCount := conf.DefaultNodeRetryCount
	if c.RetryCount != nil {
		retryCount = *c.RetryCount
	}
	client.SetRetryCount(retryCount)
	client.SetHeader("User-Agent", panelUserAgent())
	if c.Key != "" {
		client.SetHeader("Authorization", "Bearer "+c.Key)
	}
	if c.Timeout > 0 {
		client.SetTimeout(time.Duration(c.Timeout) * time.Second)
	} else {
		client.SetTimeout(time.Duration(conf.DefaultNodeTimeout) * time.Second)
	}
	client.OnError(func(req *resty.Request, err error) {
		var v *resty.ResponseError
		if errors.As(err, &v) {
			// v.Response contains the last response from the server
			// v.Err contains the original error
			logrus.WithField("error", redactPanelError(v.Err, c.Key)).
				Error("panel request failed")
		}
	})
	client.SetBaseURL(c.APIHost)
	queryParams := map[string]string{
		"node_type": "ravel",
		"node_id":   strconv.Itoa(c.NodeID),
	}
	// Scoped credentials imply a header-aware panel; legacy panels retain compatibility.
	if !strings.HasPrefix(c.Key, "machine-v1.") {
		queryParams["token"] = c.Key
	}
	client.SetQueryParams(queryParams)
	return &Client{
		client:   client,
		Token:    c.Key,
		APIHost:  c.APIHost,
		NodeId:   c.NodeID,
		UserList: &UserListBody{},
		AliveMap: &AliveMap{},
	}, nil
}

func (c *Client) CloseIdleConnections() {
	if c != nil && c.client != nil {
		c.client.GetClient().CloseIdleConnections()
	}
}

func panelUserAgent() string {
	return fmt.Sprintf(
		"ravel go-resty/%s capability/%s capability/%s (https://github.com/go-resty/resty)",
		resty.Version,
		ravelServerCapability,
		ravelOverlapCapability,
	)
}

func redactPanelError(err error, token string) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if token == "" {
		return message
	}
	message = strings.ReplaceAll(message, token, "[REDACTED]")
	encodedToken := url.QueryEscape(token)
	if encodedToken != token {
		message = strings.ReplaceAll(message, encodedToken, "[REDACTED]")
	}
	return message
}

func (c *Client) sanitizeRequestError(err error) error {
	if err == nil || c.Token == "" {
		return err
	}
	return &panelRequestError{
		cause:   err,
		message: redactPanelError(err, c.Token),
	}
}
