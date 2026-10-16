package panel

import (
	"context"
	"fmt"
	"github.com/YeJianbo/ravel/common/domaintraffic"
	"time"
)

type DomainTrafficBatch struct {
	ID         string                 `json:"batch_id"`
	RecordedAt int64                  `json:"recorded_at"`
	Records    []domaintraffic.Record `json:"records"`
}

func (c *Client) ReportDomainTraffic(ctx context.Context, batch *DomainTrafficBatch) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// The controller retains the batch and retries on the next reporting window.
	r, err := c.client.Clone().SetRetryCount(0).R().SetContext(ctx).SetBody(batch).
		SetDoNotParseResponse(true).Post("/api/v2/server/domainTraffic")
	if err != nil {
		return fmt.Errorf("domain traffic transport failed")
	}
	if r == nil || r.RawResponse == nil {
		return fmt.Errorf("domain traffic report rejected")
	}
	defer r.RawResponse.Body.Close()
	if r.StatusCode() != 200 || !r.IsSuccess() {
		return fmt.Errorf("domain traffic report rejected")
	}
	body, err := readPanelResponseBody(r, ackResponseMaxBytes)
	if err != nil {
		return fmt.Errorf("domain traffic report response: %w", err)
	}
	// Authentication failures in legacy panels can use HTTP 200.
	if !jsonAccepted(body) {
		return fmt.Errorf("domain traffic report not acknowledged")
	}
	return nil
}
