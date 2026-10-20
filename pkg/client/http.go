package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/CefasDb/cefasdb/pkg/encoding/ddbjson"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// HTTPClient is the canonical context-bound JSON client. The token provider is
// called per request so a process can rotate credentials without rebuilding
// clients. Credentials and response bodies are never included in errors.
type HTTPClient struct {
	base      string
	transport *http.Client
	token     func(context.Context) (string, error)
}

type HTTPError struct{ StatusCode int }

func (e *HTTPError) Error() string { return fmt.Sprintf("cefas HTTP status %d", e.StatusCode) }

func NewHTTPClient(endpoint string, transport *http.Client, token func(context.Context) (string, error)) (*HTTPClient, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("cefas: invalid HTTP endpoint")
	}
	if transport == nil {
		transport = &http.Client{Timeout: 35 * time.Second}
	}
	// Never replay a bearer credential or write body to a redirect supplied by
	// the server. Callers can select a trusted leader endpoint explicitly.
	copyClient := *transport
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &HTTPClient{base: strings.TrimRight(endpoint, "/"), transport: &copyClient, token: token}, nil
}

func (c *HTTPClient) request(ctx context.Context, method, path string, body, out any) error {
	ctx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	var input io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("cefas: invalid request")
		}
		input = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, input)
	if err != nil {
		return fmt.Errorf("cefas: invalid request")
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != nil {
		token, err := c.token(ctx)
		if err != nil {
			return fmt.Errorf("cefas: credential provider failed")
		}
		if token == "" {
			return fmt.Errorf("cefas: credential provider returned empty credential")
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.transport.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("cefas: HTTP transport failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &HTTPError{StatusCode: resp.StatusCode}
	}
	if out == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, 16<<20))
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (16<<20)+1))
	if err != nil || len(raw) > 16<<20 {
		return fmt.Errorf("cefas: response exceeds limit or cannot be read")
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("cefas: malformed JSON response")
	}
	return nil
}

func (c *HTTPClient) CreateTable(ctx context.Context, td types.TableDescriptor) (types.TableDescriptor, error) {
	var result types.TableDescriptor
	err := c.request(ctx, http.MethodPost, "/v1/tables", td, &result)
	return result, err
}

func (c *HTTPClient) DescribeTable(ctx context.Context, table string) (types.TableDescriptor, error) {
	var result types.TableDescriptor
	err := c.request(ctx, http.MethodGet, "/v1/tables/"+url.PathEscape(table), nil, &result)
	return result, err
}

func (c *HTTPClient) UpdateTimeToLive(ctx context.Context, table, attribute string, enabled bool) (TTLState, error) {
	var result struct {
		TTLAttribute string `json:"ttlAttribute"`
	}
	err := c.request(ctx, http.MethodPost, "/v1/UpdateTimeToLive", map[string]any{"table": table, "attributeName": attribute, "enabled": enabled}, &result)
	return TTLState{Enabled: result.TTLAttribute != "", AttributeName: result.TTLAttribute}, err
}

func (c *HTTPClient) PutItem(ctx context.Context, table string, item types.Item, opts PutOptions) error {
	return c.request(ctx, http.MethodPost, "/v1/PutItem", map[string]any{"table": table, "item": ddbjson.EncodeItem(item), "condition": opts.Condition, "binds": ddbjson.EncodeItem(opts.Binds)}, nil)
}

func (c *HTTPClient) GetItem(ctx context.Context, table string, key types.Item, strong bool) (types.Item, error) {
	var result struct {
		Found bool                         `json:"found"`
		Item  map[string]ddbjson.Attribute `json:"item"`
	}
	consistency := "eventual"
	if strong {
		consistency = "strong"
	}
	if err := c.request(ctx, http.MethodPost, "/v1/GetItem", map[string]any{"table": table, "key": ddbjson.EncodeItem(key), "consistency": consistency}, &result); err != nil {
		return nil, err
	}
	if !result.Found {
		return nil, nil
	}
	return ddbjson.DecodeItem(result.Item)
}

func (c *HTTPClient) DeleteItem(ctx context.Context, table string, key types.Item, opts DeleteOptions) error {
	return c.request(ctx, http.MethodPost, "/v1/DeleteItem", map[string]any{"table": table, "key": ddbjson.EncodeItem(key), "condition": opts.Condition, "binds": ddbjson.EncodeItem(opts.Binds)}, nil)
}

func textHTTPBody(q types.TextQuery) map[string]any {
	body := map[string]any{"table": q.Table, "indexName": q.IndexName, "pkValue": ddbjson.FromAttr(q.PKValue), "search": q.Search, "phrase": q.Phrase, "fields": ddbjson.EncodeItem(q.Fields), "minValues": ddbjson.EncodeItem(q.MinValues), "maxValues": ddbjson.EncodeItem(q.MaxValues), "descending": q.Descending, "limit": q.Limit, "cursor": q.Cursor, "consistency": q.Consistency}
	if q.SKLow.T != types.AttrNull {
		body["skLow"] = ddbjson.FromAttr(q.SKLow)
	}
	if q.SKHigh.T != types.AttrNull {
		body["skHigh"] = ddbjson.FromAttr(q.SKHigh)
	}
	return body
}

func (c *HTTPClient) TextQuery(ctx context.Context, q types.TextQuery) (types.TextQueryResult, error) {
	var wire struct {
		Items      []map[string]ddbjson.Attribute `json:"items"`
		NextCursor string                         `json:"nextCursor"`
	}
	if err := c.request(ctx, http.MethodPost, "/v1/TextQuery", textHTTPBody(q), &wire); err != nil {
		return types.TextQueryResult{}, err
	}
	out := types.TextQueryResult{Items: make([]types.Item, 0, len(wire.Items)), NextCursor: wire.NextCursor}
	for _, item := range wire.Items {
		decoded, err := ddbjson.DecodeItem(item)
		if err != nil {
			return types.TextQueryResult{}, fmt.Errorf("cefas: malformed item response")
		}
		out.Items = append(out.Items, decoded)
	}
	return out, nil
}

func (c *HTTPClient) TextAggregate(ctx context.Context, q types.TextAggregate) (types.TextAggregateResult, error) {
	body := textHTTPBody(q.TextQuery)
	body["timeField"] = q.TimeField
	body["intervalSeconds"] = q.IntervalSeconds
	body["groupBy"] = q.GroupBy
	body["errorField"] = q.ErrorField
	body["errorValues"] = q.ErrorValues
	var result types.TextAggregateResult
	err := c.request(ctx, http.MethodPost, "/v1/TextAggregate", body, &result)
	return result, err
}
