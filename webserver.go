package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

const defaultTimeout = 30 * time.Second

// abortWithError sends a JSON error and aborts the request
func abortWithError(c *gin.Context, code int, err error) {
	logrus.WithError(err).WithField("status", code).Error("request failed")
	c.JSON(code, gin.H{
		"statusCode": strconv.Itoa(code),
		"message":    err.Error(),
	})
	c.Abort()
}

// ClusterView manages cluster topology and primary selection
type ClusterView struct {
	mu        sync.RWMutex
	nodes     map[int]string
	primaryID int
	selfID    int
	timeout   time.Duration
}

// NewClusterView initializes the view for this node
func NewClusterView(selfID int, timeout time.Duration) *ClusterView {
	return &ClusterView{
		nodes:     make(map[int]string),
		primaryID: -1,
		selfID:    selfID,
		timeout:   timeout,
	}
}

// Update replaces the entire view and recalculates the primary
func (v *ClusterView) Update(view map[int]string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.nodes = view
	minID := math.MaxInt
	for id := range view {
		if id < minID {
			minID = id
		}
	}
	v.primaryID = minID
}

// IsOnline returns true if we have a view
type ViewRequest struct {
	View []struct {
		ID      int    `json:"id" binding:"required"`
		Address string `json:"address" binding:"required"`
	} `json:"view" binding:"required"`
}

func (v *ClusterView) IsOnline() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return len(v.nodes) > 0
}

// IsPrimary returns true if this node is the primary
func (v *ClusterView) IsPrimary() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.selfID == v.primaryID
}

// PrimaryAddr returns the address of the primary node
func (v *ClusterView) PrimaryAddr() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.nodes[v.primaryID]
}

// Backups returns the addresses of all non-primary nodes
func (v *ClusterView) Backups() []string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	addrs := make([]string, 0, len(v.nodes)-1)
	for id, addr := range v.nodes {
		if id != v.primaryID {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

// KVStore is a thread-safe in-memory key-value store
type KVStore struct {
	mu    sync.RWMutex
	store map[string]string
}

// NewKVStore initializes the key-value store
func NewKVStore() *KVStore {
	return &KVStore{store: make(map[string]string)}
}

// Get retrieves a value, returns (value, true) if found
func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.store[key]
	return val, ok
}

// Set stores a value, returns true if key existed
func (s *KVStore) Set(key, val string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.store[key]
	s.store[key] = val
	return existed
}

// Delete removes a key, returns true if key existed
func (s *KVStore) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.store[key]
	if existed {
		delete(s.store, key)
	}
	return existed
}

// GetAll returns a copy of the entire store
func (s *KVStore) GetAll() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[string]string, len(s.store))
	for k, v := range s.store {
		cp[k] = v
	}
	return cp
}

// Handler ties HTTP routes to store and view logic
type Handler struct {
	view  *ClusterView
	store *KVStore
}

// NewHandler constructs a Handler
func NewHandler(view *ClusterView, store *KVStore) *Handler {
	return &Handler{view: view, store: store}
}

// proxy forwards client requests to the primary node
func (h *Handler) proxy(c *gin.Context, method, path string, body io.Reader) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), h.view.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, "http://"+h.view.PrimaryAddr()+path, body)
	if err != nil {
		abortWithError(c, http.StatusInternalServerError, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			abortWithError(c, http.StatusRequestTimeout, fmt.Errorf("proxy timed out: %w", err))
		} else {
			abortWithError(c, http.StatusBadGateway, err)
		}
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	c.Data(resp.StatusCode, resp.Header.Get("Content-Type"), respBody)
}

func (h *Handler) fanOut(ctx context.Context, op func(string) error) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(h.view.Backups()))

	for _, addr := range h.view.Backups() {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			if err := op(a); err != nil {
				errCh <- err
			}
		}(addr)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		select {
		case err := <-errCh:
			return err
		default:
			return nil
		}
	}
}

// sendBackupPut issues a PUT to /internal/data on a backup
func (h *Handler) sendBackupPut(ctx context.Context, key, val, addr string) error {
	payload, _ := json.Marshal(map[string]string{"value": val})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, "http://"+addr+"/internal/data/"+key, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var respMap map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&respMap); err != nil {
		return err
	}
	if code, ok := respMap["statusCode"]; !ok || (code != "200" && code != "201") {
		return fmt.Errorf("backup responded %v", respMap)
	}
	return nil
}

// sendBackupDelete issues a DELETE to /internal/data on a backup
func (h *Handler) sendBackupDelete(ctx context.Context, key, addr string) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, "http://"+addr+"/internal/data/"+key, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var respMap map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&respMap); err != nil {
		return err
	}
	if code, ok := respMap["statusCode"]; !ok || code != "200" {
		return fmt.Errorf("backup responded %v", respMap)
	}
	return nil
}

// Handlers for view & data operations
func (h *Handler) Ping(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"statusCode": "200", "message": "ready to handle requests"})
}

func (h *Handler) TestView(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"primary": h.view.PrimaryAddr(),
		"id":      h.view.selfID,
		"view":    h.view.nodes,
	})
}

func (h *Handler) UpdateView(c *gin.Context) {
	var req ViewRequest
	if err := c.BindJSON(&req); err != nil {
		abortWithError(c, http.StatusBadRequest, err)
		return
	}
	m := make(map[int]string, len(req.View))
	for _, item := range req.View {
		m[item.ID] = item.Address
	}
	h.view.Update(m)
	c.JSON(http.StatusOK, gin.H{"message": "view updated"})
}

func (h *Handler) GetResource(c *gin.Context) {
	key := c.Param("key")
	if !h.view.IsOnline() {
		abortWithError(c, http.StatusServiceUnavailable, fmt.Errorf("waiting for view update"))
		return
	}
	if !h.view.IsPrimary() {
		h.proxy(c, http.MethodGet, "/data/"+key, nil)
		return
	}
	if val, ok := h.store.Get(key); ok {
		c.JSON(http.StatusOK, gin.H{"statusCode": "200", "value": val})
	} else {
		abortWithError(c, http.StatusNotFound, fmt.Errorf("key doesn't exist"))
	}
}

func (h *Handler) GetAllResources(c *gin.Context) {
	if !h.view.IsOnline() {
		abortWithError(c, http.StatusServiceUnavailable, fmt.Errorf("waiting for view update"))
		return
	}
	if !h.view.IsPrimary() {
		h.proxy(c, http.MethodGet, "/data", nil)
		return
	}
	c.JSON(http.StatusOK, h.store.GetAll())
}

func (h *Handler) UpdateResource(c *gin.Context) {
	if !h.view.IsOnline() {
		abortWithError(c, http.StatusServiceUnavailable, fmt.Errorf("waiting for view update"))
		return
	}
	key := c.Param("key")
	var body map[string]string
	if err := c.BindJSON(&body); err != nil {
		abortWithError(c, http.StatusBadRequest, err)
		return
	}
	val, ok := body["value"]
	if !ok {
		abortWithError(c, http.StatusBadRequest, fmt.Errorf("missing value field"))
		return
	}
	if h.view.IsPrimary() {
		ctx, cancel := context.WithTimeout(c.Request.Context(), h.view.timeout)
		defer cancel()
		if err := h.fanOut(ctx, func(addr string) error {
			return h.sendBackupPut(ctx, key, val, addr)
		}); err != nil {
			abortWithError(c, http.StatusGatewayTimeout, err)
			return
		}
		existed := h.store.Set(key, val)
		code := http.StatusCreated
		if existed {
			code = http.StatusOK
		}
		c.JSON(code, gin.H{"statusCode": strconv.Itoa(code)})
	} else {
		// proxy to primary
		payload, _ := json.Marshal(body)
		h.proxy(c, http.MethodPut, "/data/"+key, bytes.NewReader(payload))
	}
}

func (h *Handler) DeleteResource(c *gin.Context) {
	if !h.view.IsOnline() {
		abortWithError(c, http.StatusServiceUnavailable, fmt.Errorf("waiting for view update"))
		return
	}
	key := c.Param("key")
	if !h.store.Delete(key) {
		abortWithError(c, http.StatusNotFound, fmt.Errorf("key doesn't exist"))
		return
	}
	if h.view.IsPrimary() {
		ctx, cancel := context.WithTimeout(c.Request.Context(), h.view.timeout)
		defer cancel()
		if err := h.fanOut(ctx, func(addr string) error {
			return h.sendBackupDelete(ctx, key, addr)
		}); err != nil {
			abortWithError(c, http.StatusGatewayTimeout, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"statusCode": "200"})
	} else {
		h.proxy(c, http.MethodDelete, "/data/"+key, nil)
	}
}

func main() {
	id, err := strconv.Atoi(os.Getenv("NODE_IDENTIFIER"))
	if err != nil {
		logrus.Fatal("invalid NODE_IDENTIFIER")
	}
	view := NewClusterView(id, defaultTimeout)
	store := NewKVStore()
	h := NewHandler(view, store)

	r := gin.Default()
	r.GET("/ping", h.Ping)
	r.GET("/testview", h.TestView)
	r.PUT("/view", h.UpdateView)

	r.GET("/data/:key", h.GetResource)
	r.GET("/data", h.GetAllResources)
	r.PUT("/data/:key", h.UpdateResource)
	r.DELETE("/data/:key", h.DeleteResource)

	// internal
	r.PUT("/internal/data/:key", h.UpdateResource)
	r.DELETE("/internal/data/:key", h.DeleteResource)

	r.Run(":8081")
}
