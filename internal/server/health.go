package server

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

const probeTimeout = time.Second

type probeComponent struct {
	Name  string `json:"name"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type probeResponse struct {
	Status     string             `json:"status"`
	Lifecycle  LifecycleSnapshot  `json:"lifecycle"`
	Components []probeComponent   `json:"components,omitempty"`
	Raft       *raftProbeResponse `json:"raft,omitempty"`
}

type raftProbeResponse struct {
	Mode       string            `json:"mode"`
	SelfID     string            `json:"selfId,omitempty"`
	BindAddr   string            `json:"bindAddr,omitempty"`
	IsLeader   bool              `json:"isLeader,omitempty"`
	LeaderID   string            `json:"leaderId,omitempty"`
	LeaderAddr string            `json:"leaderAddr,omitempty"`
	Shards     []raftShardStatus `json:"shards,omitempty"`
}

type raftShardStatus struct {
	ID              uint32   `json:"id"`
	LocalVoter      bool     `json:"localVoter"`
	LocalNonVoter   bool     `json:"localNonVoter"`
	Voters          []string `json:"voters,omitempty"`
	NonVoters       []string `json:"nonVoters,omitempty"`
	IsLeader        bool     `json:"isLeader"`
	LeaderID        string   `json:"leaderId,omitempty"`
	LeaderAddr      string   `json:"leaderAddr,omitempty"`
	LeaderKnown     bool     `json:"leaderKnown"`
	StorageOpen     bool     `json:"storageOpen"`
	RaftInitialized bool     `json:"raftInitialized"`
}

func (s *Server) handleLivez(w http.ResponseWriter, r *http.Request) {
	components := s.storageProbeComponents(r.Context())
	if !componentsOK(components) {
		writeJSON(w, http.StatusServiceUnavailable, probeResponse{
			Status:     "unhealthy",
			Lifecycle:  s.Lifecycle().Snapshot(),
			Components: components,
		})
		return
	}
	writeJSON(w, http.StatusOK, probeResponse{
		Status:     "ok",
		Lifecycle:  s.Lifecycle().Snapshot(),
		Components: components,
	})
}

func (s *Server) handleStartupz(w http.ResponseWriter, r *http.Request) {
	snap := s.Lifecycle().Snapshot()
	if !snap.Started {
		writeJSON(w, http.StatusServiceUnavailable, probeResponse{
			Status:    "starting",
			Lifecycle: snap,
		})
		return
	}
	writeJSON(w, http.StatusOK, probeResponse{
		Status:    "ok",
		Lifecycle: snap,
	})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	resp, ok := s.readinessReport(r.Context())
	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, resp)
}

func (s *Server) handleRaftz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.raftProbe())
}

func (s *Server) readinessReport(ctx context.Context) (probeResponse, bool) {
	resp := probeResponse{
		Status:    "ok",
		Lifecycle: s.Lifecycle().Snapshot(),
		Raft:      s.raftProbe(),
	}
	add := func(name string, err error) {
		c := probeComponent{Name: name, OK: err == nil}
		if err != nil {
			c.Error = err.Error()
			resp.Status = "unhealthy"
		}
		resp.Components = append(resp.Components, c)
	}

	add("lifecycle", s.Lifecycle().servingError())
	for _, c := range s.storageProbeComponents(ctx) {
		if !c.OK {
			resp.Status = "unhealthy"
		}
		resp.Components = append(resp.Components, c)
	}
	add("raft", s.raftReadinessError())
	for _, check := range s.Lifecycle().readinessChecks() {
		add(check.name, check.fn(ctx))
	}
	return resp, resp.Status == "ok"
}

func (s *Server) storageProbeComponents(ctx context.Context) []probeComponent {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	if s.manager == nil {
		return []probeComponent{probeComponentFromError("storage", s.db.Health(ctx))}
	}
	shards := s.manager.Shards()
	out := make([]probeComponent, 0, len(shards))
	for _, sh := range shards {
		name := "storage"
		if sh != nil {
			name = fmt.Sprintf("storage/shard-%d", sh.ID)
		}
		if sh == nil || sh.Storage == nil {
			out = append(out, probeComponentFromError(name, fmt.Errorf("storage not open")))
			continue
		}
		out = append(out, probeComponentFromError(name, sh.Storage.Health(ctx)))
	}
	return out
}

func probeComponentFromError(name string, err error) probeComponent {
	c := probeComponent{Name: name, OK: err == nil}
	if err != nil {
		c.Error = err.Error()
	}
	return c
}

func componentsOK(components []probeComponent) bool {
	for _, c := range components {
		if !c.OK {
			return false
		}
	}
	return true
}

func (s *Server) raftProbe() *raftProbeResponse {
	if s.manager != nil {
		shards := s.manager.Shards()
		out := &raftProbeResponse{Mode: "multi-shard-local"}
		for _, sh := range shards {
			if sh != nil && sh.Raft != nil {
				out.Mode = "multi-raft"
				break
			}
		}
		for _, sh := range shards {
			if sh == nil {
				continue
			}
			st := raftShardStatus{
				ID:            sh.ID,
				LocalVoter:    sh.IsLocalVoter,
				LocalNonVoter: sh.IsLocalNonVoter,
				Voters:        append([]string(nil), sh.Voters...),
				NonVoters:     append([]string(nil), sh.NonVoters...),
				StorageOpen:   sh.Storage != nil,
			}
			if sh.Raft != nil {
				st.RaftInitialized = true
				st.IsLeader = sh.Raft.IsLeader()
				st.LeaderID, st.LeaderAddr = sh.Raft.LeaderInfo()
				st.LeaderKnown = st.LeaderID != ""
			}
			out.Shards = append(out.Shards, st)
		}
		return out
	}
	if s.cluster == nil {
		return &raftProbeResponse{Mode: "single-node"}
	}
	leaderID, leaderAddr := s.cluster.LeaderInfo()
	return &raftProbeResponse{
		Mode:       "raft",
		SelfID:     s.cluster.SelfID(),
		BindAddr:   s.cluster.BindAddr(),
		IsLeader:   s.cluster.IsLeader(),
		LeaderID:   leaderID,
		LeaderAddr: leaderAddr,
	}
}

func (s *Server) raftReadinessError() error {
	if s.manager != nil {
		shards := s.manager.Shards()
		raftConfigured := false
		for _, sh := range shards {
			if sh != nil && sh.Raft != nil {
				raftConfigured = true
				break
			}
		}
		if !raftConfigured {
			return nil
		}
		var first error
		for _, sh := range shards {
			if sh == nil {
				if first == nil {
					first = fmt.Errorf("missing shard")
				}
				continue
			}
			if sh.Storage == nil {
				if first == nil {
					first = fmt.Errorf("shard %d storage not open", sh.ID)
				}
				continue
			}
			if sh.Raft == nil {
				if first == nil {
					first = fmt.Errorf("shard %d raft not initialized", sh.ID)
				}
				continue
			}
			leaderID, _ := sh.Raft.LeaderInfo()
			if leaderID == "" && first == nil {
				first = fmt.Errorf("shard %d has no known leader", sh.ID)
			}
		}
		return first
	}
	if s.cluster == nil {
		return nil
	}
	leaderID, _ := s.cluster.LeaderInfo()
	if leaderID == "" {
		return fmt.Errorf("raft has no known leader")
	}
	return nil
}
