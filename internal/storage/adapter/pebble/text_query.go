package pebble

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
	pebbledb "github.com/cockroachdb/pebble"
)

var (
	ErrTextQueryInvalid = errors.New("cefas: invalid native text query")
	ErrTextQueryLimit   = errors.New("cefas: native text query resource limit")
)

const (
	maxTextCandidates = 1_000_000
	maxTextPageBytes  = 4 << 20
	maxTextBuckets    = 10000
	maxTextGroups     = 10000
)

type textCursor struct {
	Version int    `json:"v"`
	Hash    string `json:"q"`
	SK      []byte `json:"sk"`
}
type textQueryPlan struct {
	query                                           types.TextQuery
	index                                           types.TextIndexDescriptor
	pk, lower, upper, prefix, primaryPrefix, resume []byte
	low, high, overflowPrefix                       []byte
	fingerprint                                     string
	postings                                        bool
}

func textInvalid(message string) error { return fmt.Errorf("%w: %s", ErrTextQueryInvalid, message) }

func prepareTextQuery(td types.TableDescriptor, q types.TextQuery) (textQueryPlan, error) {
	var p textQueryPlan
	if q.Table != "" && q.Table != td.Name {
		return p, textInvalid("table does not match descriptor")
	}
	q.Table = td.Name
	if q.IndexName == "" || td.KeySchema.PK == "" || td.KeySchema.SK == "" || isMemoryTable(td) {
		return p, textInvalid("index, partition and disk sort-key table required")
	}
	found := false
	for _, idx := range td.TextIndexes {
		if idx.Name == q.IndexName {
			p.index = idx
			found = true
		}
	}
	if !found {
		return p, textInvalid("native text index is not declared")
	}
	if q.Limit < 0 || q.Limit > 1000 || len(q.Search)+len(q.Phrase) > 8192 || !utf8.ValidString(q.Search) || !utf8.ValidString(q.Phrase) || len(q.Fields)+len(q.MinValues)+len(q.MaxValues) > 64 {
		return p, textInvalid("query bounds exceeded")
	}
	if q.Consistency != "" && q.Consistency != "eventual" && q.Consistency != "strong" {
		return p, textInvalid("unsupported consistency")
	}
	for _, fields := range []map[string]types.AttributeValue{q.Fields, q.MinValues, q.MaxValues} {
		for path, value := range fields {
			if len(path) == 0 || len(path) > 256 || strings.ContainsAny(path, "[]\x00") {
				return p, textInvalid("invalid scalar path")
			}
			for _, segment := range strings.Split(path, ".") {
				if segment == "" {
					return p, textInvalid("empty scalar path segment")
				}
			}
			if value.T != types.AttrS && value.T != types.AttrN && value.T != types.AttrBOOL && value.T != types.AttrNull {
				return p, textInvalid("filters must be scalar")
			}
			if len(value.S)+len(value.N) > 8192 {
				return p, textInvalid("scalar filter too long")
			}
			if value.T == types.AttrN {
				if _, ok := textNumber(value.N); !ok {
					return p, textInvalid("invalid numeric filter")
				}
			}
		}
	}
	for _, key := range []types.AttributeValue{q.PKValue, q.SKLow, q.SKHigh} {
		if key.T == types.AttrN {
			if _, ok := textNumber(key.N); !ok {
				return p, textInvalid("invalid bounded numeric key")
			}
		}
	}
	pk, err := storage.AttrCanonicalBytes(q.PKValue)
	if err != nil || len(pk) > 4096 {
		return p, textInvalid("valid bounded pkValue required")
	}
	p.pk = pk
	p.primaryPrefix, _ = storage.PrefixPrimaryByPK(td.Name, pk)
	p.prefix = p.primaryPrefix
	// Any matching row must contain this gram. Other grams/terms are verified
	// on the primary, avoiding unbounded posting-list intersections in RAM.
	longest := []rune(strings.ToLower(q.Phrase))
	for _, term := range strings.Fields(strings.ToLower(q.Search)) {
		if r := []rune(term); len(r) > len(longest) {
			longest = r
		}
	}
	if len(longest) >= 3 {
		p.prefix = textPrefix(td.Name, q.IndexName, pk, string(longest[:3]))
		p.postings = true
		p.overflowPrefix = textPrefix(td.Name, q.IndexName, pk, textOverflowGram)
	}
	p.lower = bytes.Clone(p.prefix)
	p.upper = prefixEnd(p.prefix)
	var low, high []byte
	if q.SKLow.T != types.AttrNull {
		low, err = storage.AttrCanonicalBytes(q.SKLow)
		if err != nil || len(low) > 4096 {
			return p, textInvalid("invalid skLow")
		}
		p.lower = append(bytes.Clone(p.prefix), low...)
	}
	if q.SKHigh.T != types.AttrNull {
		high, err = storage.AttrCanonicalBytes(q.SKHigh)
		if err != nil || len(high) > 4096 {
			return p, textInvalid("invalid skHigh")
		}
		p.upper = append(bytes.Clone(p.prefix), high...)
	}
	p.low = low
	p.high = high
	if len(low) > 0 && len(high) > 0 && bytes.Compare(low, high) >= 0 {
		return p, textInvalid("sort range must be increasing")
	}
	bound := q
	bound.Cursor = ""
	bound.Limit = 0
	raw, err := json.Marshal(struct {
		Query types.TextQuery
		Field string
	}{bound, p.index.Field})
	if err != nil {
		return p, err
	}
	p.fingerprint = fmt.Sprintf("%x", sha256.Sum256(raw))
	if q.Cursor != "" {
		if len(q.Cursor) > 8192 {
			return p, textInvalid("cursor too long")
		}
		raw, err := base64.RawURLEncoding.DecodeString(q.Cursor)
		if err != nil {
			return p, textInvalid("invalid cursor encoding")
		}
		var cursor textCursor
		if json.Unmarshal(raw, &cursor) != nil || cursor.Version != 1 || cursor.Hash != p.fingerprint || len(cursor.SK) == 0 || len(cursor.SK) > 4096 {
			return p, textInvalid("cursor does not match query")
		}
		key := append(bytes.Clone(p.prefix), cursor.SK...)
		if bytes.Compare(key, p.lower) < 0 || (p.upper != nil && bytes.Compare(key, p.upper) >= 0) {
			return p, textInvalid("cursor outside range")
		}
		p.resume = key
	}
	p.query = q
	return p, nil
}

func scalarCompare(a, b types.AttributeValue) (int, bool) {
	if a.T != b.T {
		return 0, false
	}
	switch a.T {
	case types.AttrS:
		return strings.Compare(a.S, b.S), true
	case types.AttrN:
		x, ok := textNumber(a.N)
		if !ok {
			return 0, false
		}
		y, ok := textNumber(b.N)
		if !ok {
			return 0, false
		}
		return x.Cmp(y), true
	case types.AttrBOOL:
		if a.BOOL == b.BOOL {
			return 0, true
		}
		if !a.BOOL {
			return -1, true
		}
		return 1, true
	case types.AttrNull:
		return 0, true
	}
	return 0, false
}

func textMatches(td types.TableDescriptor, p textQueryPlan, item types.Item) bool {
	pk, exists := item[td.KeySchema.PK]
	if cmp, valid := scalarCompare(pk, p.query.PKValue); !exists || !valid || cmp != 0 {
		return false
	}
	if itemExpired(td, item, time.Now()) {
		return false
	}
	value, exists := textPath(item, p.index.Field)
	if !exists || value.T != types.AttrS {
		return false
	}
	text := strings.ToLower(value.S)
	if !strings.Contains(text, strings.ToLower(p.query.Phrase)) {
		return false
	}
	for _, term := range strings.Fields(strings.ToLower(p.query.Search)) {
		if !strings.Contains(text, term) {
			return false
		}
	}
	for path, want := range p.query.Fields {
		got, exists := textPath(item, path)
		cmp, ok := scalarCompare(got, want)
		if !exists || !ok || cmp != 0 {
			return false
		}
	}
	for path, min := range p.query.MinValues {
		got, exists := textPath(item, path)
		cmp, ok := scalarCompare(got, min)
		if !exists || !ok || cmp < 0 {
			return false
		}
	}
	for path, max := range p.query.MaxValues {
		got, exists := textPath(item, path)
		cmp, ok := scalarCompare(got, max)
		if !exists || !ok || cmp > 0 {
			return false
		}
	}
	return true
}

// walkText owns only iterator/snapshot state, not a corpus or posting list.
// A hard budget is an error, never a successful approximate aggregate.
type textCandidateIterator struct {
	it         *pebbledb.Iterator
	prefix     []byte
	valid      bool
	descending bool
}

func openTextCandidates(snap *pebbledb.Snapshot, p textQueryPlan, prefix []byte) (*textCandidateIterator, error) {
	lower := append(bytes.Clone(prefix), p.low...)
	upper := prefixEnd(prefix)
	if len(p.high) > 0 {
		upper = append(bytes.Clone(prefix), p.high...)
	}
	it, err := snap.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	candidate := &textCandidateIterator{it: it, prefix: prefix, descending: p.query.Descending}
	if len(p.resume) > 0 {
		resume := append(bytes.Clone(prefix), p.resume[len(p.prefix):]...)
		if p.query.Descending {
			candidate.valid = it.SeekLT(resume)
		} else {
			candidate.valid = it.SeekGE(resume)
			if candidate.valid && bytes.Equal(it.Key(), resume) {
				candidate.valid = it.Next()
			}
		}
	} else if p.query.Descending {
		candidate.valid = it.Last()
	} else {
		candidate.valid = it.First()
	}
	return candidate, nil
}
func (it *textCandidateIterator) sk() []byte { return it.it.Key()[len(it.prefix):] }
func (it *textCandidateIterator) advance() {
	if it.descending {
		it.valid = it.it.Prev()
	} else {
		it.valid = it.it.Next()
	}
}

func (d *DB) walkText(ctx context.Context, td types.TableDescriptor, p textQueryPlan, visit func(types.Item, []byte) (bool, error)) error {
	return d.runReadCtx(ctx, func() error {
		snap := d.db.NewSnapshot()
		defer snap.Close()
		first, err := openTextCandidates(snap, p, p.prefix)
		if err != nil {
			return err
		}
		defer first.it.Close()
		var overflow *textCandidateIterator
		if p.postings {
			overflow, err = openTextCandidates(snap, p, p.overflowPrefix)
			if err != nil {
				return err
			}
			defer overflow.it.Close()
		}
		for scanned := 0; first.valid || (overflow != nil && overflow.valid); scanned++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if scanned >= maxTextCandidates {
				return fmt.Errorf("%w: more than %d candidate rows", ErrTextQueryLimit, maxTextCandidates)
			}
			current := first
			duplicate := false
			if !first.valid {
				current = overflow
			} else if overflow != nil && overflow.valid {
				cmp := bytes.Compare(first.sk(), overflow.sk())
				duplicate = cmp == 0
				if (p.query.Descending && cmp < 0) || (!p.query.Descending && cmp > 0) {
					current = overflow
				}
			}
			sk := bytes.Clone(current.sk())
			var raw []byte
			if p.postings {
				value, closer, err := snap.Get(append(bytes.Clone(p.primaryPrefix), sk...))
				if err != nil && !errors.Is(err, ErrNotFound) {
					return err
				}
				if err == nil {
					raw = bytes.Clone(value)
					_ = closer.Close()
				}
			} else {
				raw = bytes.Clone(current.it.Value())
			}
			if duplicate {
				first.advance()
				overflow.advance()
			} else {
				current.advance()
			}
			if err := first.it.Error(); err != nil {
				return err
			}
			if overflow != nil {
				if err := overflow.it.Error(); err != nil {
					return err
				}
			}
			if raw == nil {
				continue
			}
			item, err := storage.DecodeItem(raw)
			if err != nil {
				return err
			}
			if textMatches(td, p, item) {
				more, err := visit(item, sk)
				if err != nil {
					return err
				}
				if !more {
					return nil
				}
			}
		}
		if err := first.it.Error(); err != nil {
			return err
		}
		if overflow != nil {
			return overflow.it.Error()
		}
		return nil
	})
}

func (d *DB) TextQuery(ctx context.Context, td types.TableDescriptor, q types.TextQuery) (types.TextQueryResult, error) {
	result := types.TextQueryResult{Items: []types.Item{}}
	p, err := prepareTextQuery(td, q)
	if err != nil {
		return result, err
	}
	limit := q.Limit
	if limit == 0 {
		limit = 100
	}
	var last []byte
	size := 0
	more := false
	err = d.walkText(ctx, td, p, func(item types.Item, sk []byte) (bool, error) {
		raw, err := storage.EncodeItem(item)
		if err != nil {
			return false, err
		}
		if len(raw) > maxTextPageBytes {
			return false, ErrTextQueryLimit
		}
		if len(result.Items) >= limit || size+len(raw) > maxTextPageBytes {
			more = true
			return false, nil
		}
		result.Items = append(result.Items, item)
		last = bytes.Clone(sk)
		size += len(raw)
		return true, nil
	})
	if err != nil {
		return types.TextQueryResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return types.TextQueryResult{}, err
	}
	// Expiry may advance while a slower page is materialized; the final result
	// must not expose a row already expired at the response boundary.
	visible := result.Items[:0]
	for _, item := range result.Items {
		if !itemExpired(td, item, time.Now()) {
			visible = append(visible, item)
		}
	}
	result.Items = visible
	if more && len(last) > 0 {
		raw, _ := json.Marshal(textCursor{1, p.fingerprint, last})
		result.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
	}
	return result, nil
}

func textEventTime(value types.AttributeValue) (time.Time, error) {
	if value.T == types.AttrS {
		return time.Parse(time.RFC3339Nano, value.S)
	}
	if value.T == types.AttrN {
		seconds, err := strconv.ParseInt(value.N, 10, 64)
		return time.Unix(seconds, 0).UTC(), err
	}
	return time.Time{}, textInvalid("aggregate time field must be RFC3339 or epoch seconds")
}

func (d *DB) TextAggregate(ctx context.Context, td types.TableDescriptor, q types.TextAggregate) (types.TextAggregateResult, error) {
	result := types.TextAggregateResult{Buckets: []types.TextBucket{}}
	if q.Cursor != "" || q.Limit != 0 || q.IntervalSeconds < 1 || q.IntervalSeconds > 86400 {
		return result, textInvalid("aggregate requires interval 1..86400 and no page limit/cursor")
	}
	p, err := prepareTextQuery(td, q.TextQuery)
	if err != nil {
		return result, err
	}
	if q.TimeField == "" {
		q.TimeField = "timestamp"
	}
	if q.ErrorField == "" {
		q.ErrorField = "level"
	}
	if len(q.ErrorValues) == 0 {
		q.ErrorValues = []string{"error", "fatal", "panic"}
	}
	if len(q.TimeField)+len(q.GroupBy)+len(q.ErrorField) > 768 || len(q.ErrorValues) > 32 {
		return result, textInvalid("aggregate field bounds exceeded")
	}
	buckets := make(map[int64]*types.TextBucket)
	groups := 0
	var earliestExpiry int64
	err = d.walkText(ctx, td, p, func(item types.Item, _ []byte) (bool, error) {
		av, ok := textPath(item, q.TimeField)
		if !ok {
			return false, textInvalid("aggregate time field missing")
		}
		timestamp, err := textEventTime(av)
		if err != nil {
			return false, textInvalid("invalid aggregate time field")
		}
		interval := int64(q.IntervalSeconds)
		epoch := timestamp.Unix()
		bucketID := epoch - ((epoch%interval)+interval)%interval
		bucket := buckets[bucketID]
		if bucket == nil {
			if len(buckets) >= maxTextBuckets {
				return false, ErrTextQueryLimit
			}
			bucket = &types.TextBucket{Timestamp: time.Unix(bucketID, 0).UTC().Format(time.RFC3339)}
			buckets[bucketID] = bucket
		}
		bucket.Count++
		if td.TTLAttribute != "" {
			expiry, _ := strconv.ParseInt(item[td.TTLAttribute].N, 10, 64)
			if expiry > 0 && (earliestExpiry == 0 || expiry < earliestExpiry) {
				earliestExpiry = expiry
			}
		}
		if av, ok := textPath(item, q.ErrorField); ok && av.T == types.AttrS {
			for _, value := range q.ErrorValues {
				if strings.EqualFold(av.S, value) {
					bucket.Errors++
					break
				}
			}
		}
		if q.GroupBy != "" {
			if av, ok := textPath(item, q.GroupBy); ok {
				var group string
				switch av.T {
				case types.AttrS:
					group = av.S
				case types.AttrN:
					group = av.N
				case types.AttrBOOL:
					group = strconv.FormatBool(av.BOOL)
				default:
					return false, textInvalid("group field must be scalar")
				}
				if len(group) > 1024 {
					return false, ErrTextQueryLimit
				}
				if bucket.Groups == nil {
					bucket.Groups = make(map[string]uint64)
				}
				if _, ok := bucket.Groups[group]; !ok {
					groups++
					if groups > maxTextGroups {
						return false, ErrTextQueryLimit
					}
				}
				bucket.Groups[group]++
			}
		}
		return true, nil
	})
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if earliestExpiry > 0 && earliestExpiry <= time.Now().Unix() {
		return types.TextAggregateResult{}, fmt.Errorf("aggregation visibility changed during read; retry")
	}
	ids := make([]int64, 0, len(buckets))
	for id := range buckets {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		result.Buckets = append(result.Buckets, *buckets[id])
	}
	return result, nil
}
