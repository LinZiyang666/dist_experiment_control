// vocabulary.go owns the disk boundary, including duplicate-member rejection.
// JSON Schema alone cannot detect duplicate keys after a parser has collapsed
// them. Validate the byte stream first, then apply the same embedded schema used
// by the non-Go collectors. No evidence path or command is ever executed here.
// origin: hot-update S10 plan §5.1 / P00
package contract

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"unicode/utf8"
)

//go:embed schema.json
var schemaBytes []byte

// Schema returns independent storage so a caller cannot relax later validation.
func Schema() []byte { return bytes.Clone(schemaBytes) }

// Only the schema keywords used by this contract are implemented. Unknown
// keywords are errors while loading, rather than silently ineffective guards.
type schemaNode struct {
	Schema     string                 `json:"$schema"`
	ID         string                 `json:"$id"`
	Comment    string                 `json:"$comment"`
	Ref        string                 `json:"$ref"`
	Defs       map[string]*schemaNode `json:"$defs"`
	Type       string                 `json:"type"`
	Enum       []json.RawMessage      `json:"enum"`
	Const      json.RawMessage        `json:"const"`
	Properties map[string]*schemaNode `json:"properties"`
	Required   []string               `json:"required"`
	Additional *bool                  `json:"additionalProperties"`
	Items      *schemaNode            `json:"items"`
	MinItems   int                    `json:"minItems"`
	Unique     bool                   `json:"uniqueItems"`
	MinLength  int                    `json:"minLength"`
	Pattern    string                 `json:"pattern"`
	Minimum    *json.Number           `json:"minimum"`
	Maximum    *json.Number           `json:"maximum"`
	OneOf      []*schemaNode          `json:"oneOf"`
	AllOf      []*schemaNode          `json:"allOf"`
	GoType     string                 `json:"x-go-type"`
	GoName     string                 `json:"x-go-name"`
}

var schemaCache struct {
	sync.Once
	root schemaNode
	err  error
}

func schemaDefinition(name string) (*schemaNode, error) {
	schemaCache.Do(func() {
		if _, err := strictJSON(schemaBytes); err != nil {
			schemaCache.err = err
			return
		}
		dec := json.NewDecoder(bytes.NewReader(schemaBytes))
		dec.DisallowUnknownFields()
		schemaCache.err = dec.Decode(&schemaCache.root)
	})
	if schemaCache.err != nil {
		return nil, errors.New("invalid embedded fullstack schema")
	}
	s, ok := schemaCache.root.Defs[name]
	if !ok {
		return nil, errors.New("unknown fullstack schema definition")
	}
	return s, nil
}

// ValidateJSON checks a named $defs entry. deploy_evidence additionally rejects
// hermetic/source-gate receipts; schema validity itself never signs E1 or 99.
// Errors deliberately omit input values, including raw product diagnostics.
func ValidateJSON(name string, data []byte) error {
	s, err := schemaDefinition(name)
	if err != nil {
		return err
	}
	v, err := strictJSON(data)
	if err != nil {
		return err
	}
	return validateNode(s, v, 0)
}

func strictJSON(data []byte) (any, error) {
	// Do not reuse the product image budget for a long-run evidence archive.
	// The collector owns archive bounds; this parser owns syntax and nesting.
	if !utf8.Valid(data) {
		return nil, errors.New("invalid fullstack JSON UTF-8")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	v, err := readValue(dec, 0)
	if err != nil {
		return nil, err
	}
	if _, err = dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing fullstack JSON")
	}
	return v, nil
}

func readValue(dec *json.Decoder, depth int) (any, error) {
	if depth > 128 {
		return nil, errors.New("fullstack JSON nesting limit")
	}
	token, err := dec.Token()
	if err != nil {
		return nil, errors.New("malformed fullstack JSON")
	}
	switch token {
	case json.Delim('{'):
		object := make(map[string]any)
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return nil, errors.New("malformed fullstack member")
			}
			name, ok := key.(string)
			if !ok {
				return nil, errors.New("invalid fullstack member name")
			}
			if _, exists := object[name]; exists {
				return nil, errors.New("duplicate fullstack member")
			}
			object[name], err = readValue(dec, depth+1)
			if err != nil {
				return nil, err
			}
		}
		if end, err := dec.Token(); err != nil || end != json.Delim('}') {
			return nil, errors.New("unterminated fullstack object")
		}
		return object, nil
	case json.Delim('['):
		array := make([]any, 0)
		for dec.More() {
			item, err := readValue(dec, depth+1)
			if err != nil {
				return nil, err
			}
			array = append(array, item)
		}
		if end, err := dec.Token(); err != nil || end != json.Delim(']') {
			return nil, errors.New("unterminated fullstack array")
		}
		return array, nil
	default:
		if _, delim := token.(json.Delim); delim {
			return nil, errors.New("unexpected fullstack delimiter")
		}
		return token, nil
	}
}

func validateNode(s *schemaNode, v any, depth int) error {
	if s == nil || depth > 128 {
		return errors.New("invalid fullstack schema reference")
	}
	if s.Ref != "" {
		target, err := schemaDefinition(strings.TrimPrefix(s.Ref, "#/$defs/"))
		if err != nil {
			return err
		}
		if err := validateNode(target, v, depth+1); err != nil {
			return err
		}
	}
	for _, part := range s.AllOf {
		if err := validateNode(part, v, depth+1); err != nil {
			return err
		}
	}
	if len(s.OneOf) != 0 {
		matched := 0
		for _, part := range s.OneOf {
			if validateNode(part, v, depth+1) == nil {
				matched++
			}
		}
		if matched != 1 {
			return errors.New("fullstack schema alternatives mismatch")
		}
	}
	if s.Const != nil && !equalJSON(s.Const, v) {
		return errors.New("fullstack constant mismatch")
	}
	if s.Enum != nil {
		matched := false
		for _, allowed := range s.Enum {
			matched = matched || equalJSON(allowed, v)
		}
		if !matched {
			return errors.New("unknown fullstack vocabulary")
		}
	}
	switch s.Type {
	case "object":
		if _, ok := v.(map[string]any); !ok {
			return errors.New("fullstack object required")
		}
	case "array":
		items, ok := v.([]any)
		if !ok || len(items) < s.MinItems {
			return errors.New("fullstack required array missing or empty")
		}
		seen := make(map[string]bool)
		for _, item := range items {
			if err := validateNode(s.Items, item, depth+1); err != nil {
				return err
			}
			if s.Unique {
				canonical, err := json.Marshal(item)
				if err != nil {
					return errors.New("invalid fullstack array item")
				}
				if seen[string(canonical)] {
					return errors.New("duplicate fullstack array item")
				}
				seen[string(canonical)] = true
			}
		}
	case "string":
		str, ok := v.(string)
		if !ok || utf8.RuneCountInString(str) < s.MinLength {
			return errors.New("invalid fullstack string")
		}
		if s.Pattern != "" {
			matched, err := regexp.MatchString(s.Pattern, str)
			if err != nil || !matched {
				return errors.New("invalid fullstack string format")
			}
		}
	case "integer":
		number, ok := v.(json.Number)
		if !ok {
			return errors.New("fullstack integer required")
		}
		// Decimal integers preserve uint64 identities beyond float64 precision.
		value, ok := new(big.Int).SetString(string(number), 10)
		if !ok {
			return errors.New("fullstack decimal integer required")
		}
		for _, limit := range []struct {
			n     *json.Number
			lower bool
		}{{s.Minimum, true}, {s.Maximum, false}} {
			if limit.n == nil {
				continue
			}
			bound, ok := new(big.Int).SetString(string(*limit.n), 10)
			if !ok {
				return errors.New("invalid fullstack integer bound")
			}
			cmp := value.Cmp(bound)
			if (limit.lower && cmp < 0) || (!limit.lower && cmp > 0) {
				return errors.New("fullstack integer outside bounds")
			}
		}
	case "boolean":
		if _, ok := v.(bool); !ok {
			return errors.New("fullstack boolean required")
		}
	case "null":
		if v != nil {
			return errors.New("fullstack null required")
		}
	case "": // Composition nodes inherit their shape from a referenced schema.
	default:
		return errors.New("unsupported fullstack schema type")
	}
	if object, ok := v.(map[string]any); ok {
		for _, key := range s.Required {
			if _, present := object[key]; !present {
				return errors.New("required fullstack member missing")
			}
		}
		for key, value := range object {
			property, exists := s.Properties[key]
			if exists {
				if err := validateNode(property, value, depth+1); err != nil {
					return err
				}
			} else if s.Additional != nil && !*s.Additional {
				return errors.New("unknown fullstack member")
			}
		}
	}
	return nil
}

func equalJSON(raw json.RawMessage, v any) bool {
	want, err := strictJSON(raw)
	return err == nil && reflect.DeepEqual(want, v)
}

// EncodeJSON maps a frozen Go DTO through explicit schema field metadata. Nil
// slices encode as present empty arrays; required populations still fail closed.
// RawMessage fields preserve the complete upstream JSON, not a partial map.
func EncodeJSON(name string, value any) ([]byte, error) {
	s, err := schemaDefinition(name)
	if err != nil {
		return nil, err
	}
	projected, err := projectJSON(s, reflect.ValueOf(value))
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(projected)
	if err != nil {
		return nil, errors.New("cannot encode fullstack DTO")
	}
	if err = ValidateJSON(name, data); err != nil {
		return nil, err
	}
	return data, nil
}

func resolved(s *schemaNode) (*schemaNode, error) {
	if s.Ref == "" {
		return s, nil
	}
	return schemaDefinition(strings.TrimPrefix(s.Ref, "#/$defs/"))
}

func projectJSON(s *schemaNode, v reflect.Value) (any, error) {
	if !v.IsValid() {
		return nil, errors.New("missing fullstack DTO")
	}
	if v.Type() == reflect.TypeFor[json.RawMessage]() {
		// MarshalJSON compacts whitespace. The original raw bytes remain in the
		// digest-bound archive, while the full structured value is retained here.
		return nil, errors.New("raw JSON requires a containing fullstack DTO")
	}
	var err error
	s, err = resolved(s)
	if err != nil {
		return nil, err
	}
	if v.Type() == reflect.TypeFor[Digest]() {
		d, ok := v.Interface().(Digest)
		if !ok {
			return nil, errors.New("fullstack DTO digest mismatch")
		}
		return hex.EncodeToString(d[:]), nil
	}
	switch v.Kind() {
	case reflect.Struct:
		if s.GoType != v.Type().Name() || v.Type().PkgPath() != reflect.TypeFor[Profile]().PkgPath() || len(s.Properties) != v.NumField() {
			return nil, errors.New("fullstack DTO shape mismatch")
		}
		object := make(map[string]any, len(s.Properties))
		for key, property := range s.Properties {
			field := v.FieldByName(property.GoName)
			if !field.IsValid() {
				return nil, errors.New("missing fullstack DTO field")
			}
			if field.Type() == reflect.TypeFor[json.RawMessage]() {
				object[key] = json.RawMessage(bytes.Clone(field.Bytes()))
			} else {
				object[key], err = projectJSON(property, field)
				if err != nil {
					return nil, err
				}
			}
		}
		return object, nil
	case reflect.Slice:
		if s.Items == nil {
			return nil, errors.New("fullstack DTO array mismatch")
		}
		items := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			items[i], err = projectJSON(s.Items, v.Index(i))
			if err != nil {
				return nil, err
			}
		}
		return items, nil
	case reflect.String, reflect.Bool, reflect.Int, reflect.Int64, reflect.Uint32, reflect.Uint64:
		return v.Interface(), nil
	default:
		return nil, errors.New("unsupported fullstack DTO type")
	}
}

// DecodeJSON rejects malformed input before modifying dst. Even a late codec
// mismatch leaves the caller's previous evidence intact instead of half replaced.
func DecodeJSON(name string, data []byte, dst any) error {
	if err := ValidateJSON(name, data); err != nil {
		return err
	}
	s, err := schemaDefinition(name)
	if err != nil {
		return err
	}
	value := reflect.ValueOf(dst)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() {
		return errors.New("fullstack DTO pointer required")
	}
	next := reflect.New(value.Elem().Type()).Elem()
	if err := decodeJSON(s, data, next); err != nil {
		return err
	}
	value.Elem().Set(next)
	return nil
}

func decodeJSON(s *schemaNode, data []byte, dst reflect.Value) error {
	if dst.Type() == reflect.TypeFor[json.RawMessage]() {
		dst.SetBytes(bytes.Clone(data))
		return nil
	}
	var err error
	s, err = resolved(s)
	if err != nil {
		return err
	}
	if dst.Type() == reflect.TypeFor[Digest]() {
		var encoded string
		if err := json.Unmarshal(data, &encoded); err != nil {
			return errors.New("invalid fullstack digest")
		}
		decoded, err := hex.DecodeString(encoded)
		if err != nil || len(decoded) != 32 {
			return errors.New("invalid fullstack digest")
		}
		dst.Set(reflect.ValueOf(Digest(decoded)))
		return nil
	}
	switch dst.Kind() {
	case reflect.Struct:
		if s.GoType != dst.Type().Name() || dst.Type().PkgPath() != reflect.TypeFor[Profile]().PkgPath() || len(s.Properties) != dst.NumField() {
			return errors.New("fullstack DTO shape mismatch")
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(data, &object); err != nil {
			return errors.New("invalid fullstack object")
		}
		for key, property := range s.Properties {
			field := dst.FieldByName(property.GoName)
			if !field.IsValid() || !field.CanSet() {
				return errors.New("missing fullstack DTO field")
			}
			if err := decodeJSON(property, object[key], field); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if s.Items == nil {
			return errors.New("fullstack DTO array mismatch")
		}
		var items []json.RawMessage
		if err := json.Unmarshal(data, &items); err != nil {
			return errors.New("invalid fullstack array")
		}
		dst.Set(reflect.MakeSlice(dst.Type(), len(items), len(items)))
		for i, item := range items {
			if err := decodeJSON(s.Items, item, dst.Index(i)); err != nil {
				return err
			}
		}
	case reflect.String, reflect.Bool, reflect.Int, reflect.Int64, reflect.Uint32, reflect.Uint64:
		if err := json.Unmarshal(data, dst.Addr().Interface()); err != nil {
			return errors.New("invalid fullstack scalar")
		}
	default:
		return fmt.Errorf("unsupported fullstack DTO kind: %s", dst.Kind())
	}
	return nil
}
