// Disk-pressure monitor (architecture H.4 / P7 spec line 2062).
//
// Periodically samples used/total bytes on Config.StoreDir and pubs
// sys.events{type:disk_pressure} when usage crosses the configured
// threshold (default 80%). The monitor does NOT auto-trim history
// streams; that's a deliberate H.4 decision — owners decide whether
// to `session rm` cold sessions or extend storage.
//
// Tests inject Config.DiskUsageFn to drive the threshold without
// filling actual disk. Production uses diskUsage (syscall.Statfs).
package broker

import (
	"context"
	"fmt"
	"syscall"
	"time"
)

// defaultDiskCheckInterval / defaultDiskPressureThreshold mirror
// architecture H.4 ("如每 5 min" + "硬盘 > 80%").
const (
	defaultDiskCheckInterval     = 5 * time.Minute
	defaultDiskPressureThreshold = 0.80
)

// startDiskMonitor spawns the polling goroutine if a StoreDir is
// configured AND the threshold is in (0, 1]. No-op otherwise. The
// goroutine exits when ctx is canceled.
//
// One-shot edge-trigger: once we observe usage above threshold we
// emit a disk_pressure event, then we hold off re-emitting until
// usage drops back below the threshold (avoids spamming events on
// every tick while the disk stays full). On the next dip-and-spike
// cycle the next emission fires.
func (b *Broker) startDiskMonitor(ctx context.Context) {
	if b.cfg.StoreDir == "" {
		return
	}
	threshold := b.cfg.DiskPressureThreshold
	if threshold <= 0 {
		threshold = defaultDiskPressureThreshold
	}
	if threshold > 1 {
		b.cfg.Logger.Warn("broker: disk monitor disabled — threshold > 1",
			"threshold", threshold)
		return
	}
	interval := b.cfg.DiskCheckInterval
	if interval <= 0 {
		interval = defaultDiskCheckInterval
	}
	usageFn := b.cfg.DiskUsageFn
	if usageFn == nil {
		usageFn = diskUsage
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		// Tick once on startup so an already-full disk surfaces
		// without waiting one full interval.
		emitted := false
		check := func() {
			used, total, err := usageFn(b.cfg.StoreDir)
			if err != nil {
				b.cfg.Logger.Warn("broker: disk usage probe", "err", err, "path", b.cfg.StoreDir)
				return
			}
			if total == 0 {
				return
			}
			frac := float64(used) / float64(total)
			if frac >= threshold {
				if !emitted {
					b.pubSysEvent("disk_pressure", map[string]any{
						"path":      b.cfg.StoreDir,
						"used":      used,
						"total":     total,
						"fraction":  frac,
						"threshold": threshold,
					})
					emitted = true
				}
			} else {
				// Recovered below threshold — re-arm for next spike.
				emitted = false
			}
		}
		// Audit shard 01 F12: ctx-check before the first probe so
		// a broker that's already shutting down (Run started but
		// then immediately canceled) doesn't pub a disk_pressure
		// onto a draining nats.Conn.
		if ctx.Err() != nil {
			return
		}
		check()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				check()
			}
		}
	}()
}

// diskUsage returns (used, total) bytes on the filesystem containing
// path, via syscall.Statfs. Linux-only; Windows / Plan 9 builds would
// need a different implementation but v1 ships Linux-only (see
// architecture I.5).
func diskUsage(path string) (used, total uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, fmt.Errorf("statfs %s: %w", path, err)
	}
	bsize := uint64(st.Bsize) //nolint:unconvert,gosec // Statfs_t.Bsize is int64 on linux
	total = uint64(st.Blocks) * bsize
	free := uint64(st.Bavail) * bsize
	if free > total {
		return 0, total, nil
	}
	used = total - free
	return used, total, nil
}
