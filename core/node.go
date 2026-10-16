package core

import (
	"errors"
	"fmt"

	panel "github.com/YeJianbo/ravel/api/v2board"
)

func (v *V2Core) AddNode(tag string, info *panel.NodeInfo) error {
	if info != nil && info.Type == "ravel" {
		runtime, err := newRavelRuntime(tag, info)
		if err != nil {
			return fmt.Errorf("build ravel runtime: %w", err)
		}
		v.ravelMu.Lock()
		defer v.ravelMu.Unlock()
		if _, exists := v.ravel[tag]; exists {
			_ = runtime.close()
			return fmt.Errorf("ravel node %s already exists", tag)
		}
		v.ravel[tag] = runtime
		return nil
	}
	inBoundConfig, err := buildInbound(info, tag)
	if err != nil {
		return fmt.Errorf("build inbound error: %s", err)
	}
	err = v.addInbound(inBoundConfig)
	if err != nil {
		return fmt.Errorf("add inbound error: %s", err)
	}
	if v.dispatcher != nil {
		if err := v.dispatcher.OpenNode(tag); err != nil {
			_ = v.removeInbound(tag)
			return err
		}
	}
	return nil
}

func (v *V2Core) DelNode(tag string) error {
	_, err := v.delNode(tag, false)
	return err
}

// DelNodeWithTraffic keeps the runtime's remaining counters available to reload snapshots.
func (v *V2Core) DelNodeWithTraffic(tag string) ([]panel.UserTraffic, error) {
	return v.delNode(tag, true)
}

func (v *V2Core) delNode(tag string, captureTraffic bool) ([]panel.UserTraffic, error) {
	v.ravelMu.Lock()
	runtime, exists := v.ravel[tag]
	if exists {
		delete(v.ravel, tag)
	}
	v.ravelMu.Unlock()
	if exists {
		err := runtime.close()
		// Gateway close waits for sessions and their final traffic callbacks.
		var traffic []panel.UserTraffic
		if captureTraffic {
			traffic = runtime.trafficSlice(0)
		}
		if err != nil {
			return traffic, fmt.Errorf("close ravel node: %w", err)
		}
		return traffic, nil
	}
	err := v.removeInbound(tag)
	if err != nil {
		err = fmt.Errorf("remove in error: %w", err)
	}
	// Even a missing inbound can leave established links and unreported counters.
	if v.dispatcher != nil {
		v.dispatcher.CloseNode(tag)
		if captureTraffic {
			traffic, trafficErr := v.GetUserTrafficSlice(tag, 0)
			return traffic, errors.Join(err, trafficErr)
		}
	}
	return nil, err
}
