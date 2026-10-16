package panel

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/go-resty/resty/v2"
)

const (
	nodeResponseMaxBytes  = 8 << 20
	userResponseMaxBytes  = 32 << 20
	aliveResponseMaxBytes = 4 << 20
	ackResponseMaxBytes   = 128 << 10
)

func readPanelResponseBody(response *resty.Response, limit int64) ([]byte, error) {
	if response.RawResponse.ContentLength > limit {
		return nil, fmt.Errorf("panel response exceeds %d bytes", limit)
	}
	body, err := io.ReadAll(io.LimitReader(response.RawResponse.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read panel response: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("panel response exceeds %d bytes", limit)
	}
	return body, nil
}

func requireReportAcknowledgement(response *resty.Response, expectedBatchIDs ...string) error {
	if response == nil || response.RawResponse == nil {
		return fmt.Errorf("received nil report response")
	}
	defer response.RawResponse.Body.Close()
	if response.StatusCode() < 200 || response.StatusCode() >= 300 {
		return fmt.Errorf("report returned HTTP %d", response.StatusCode())
	}
	body, err := readPanelResponseBody(response, ackResponseMaxBytes)
	if err != nil {
		return err
	}
	var acknowledgement struct {
		Data    bool   `json:"data"`
		BatchID string `json:"batch_id"`
	}
	if json.Unmarshal(body, &acknowledgement) != nil || !acknowledgement.Data {
		return fmt.Errorf("panel did not acknowledge report")
	}
	if len(expectedBatchIDs) > 0 && expectedBatchIDs[0] != "" && acknowledgement.BatchID != "" && acknowledgement.BatchID != expectedBatchIDs[0] {
		return fmt.Errorf("panel acknowledged a different traffic batch")
	}
	return nil
}
