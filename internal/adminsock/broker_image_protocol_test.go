package adminsock

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: hot-update S6 plan §9.1 / T46
func TestBrokerAdminImageProtocol(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[Response](), reflect.TypeFor[RuntimeReport]()} {
		field, found := typ.FieldByName("BrokerImage")
		if !found || field.Type != reflect.TypeFor[*proto.BrokerImageStatus]() || field.Tag.Get("json") != "broker_image,omitempty" {
			t.Fatalf("%s must share the actual proto status pointer and additive tag: %+v", typ, field)
		}
	}
	field, found := reflect.TypeFor[Response]().FieldByName("NextGeneration")
	if !found || field.Type.Kind() != reflect.Uint64 || field.Tag.Get("json") != "next_generation,omitempty" {
		t.Fatalf("sealed next generation type/tag changed: %+v", field)
	}
	status := &proto.BrokerImageStatus{RunningSHA256: "running", Generation: 2, Phase: "control_ready", CutID: "cut", Mode: "single"}
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"ordinary reply", Response{Op: "broker_upgrade_reload", OK: true}, `{"op":"broker_upgrade_reload","ok":true}`},
		{"armed reply", Response{Op: "broker_upgrade_reload", OK: true, Reloading: true, NextGeneration: 3}, `{"op":"broker_upgrade_reload","ok":true,"reloading":true,"next_generation":3}`},
		{"completed reply", Response{Op: "broker_upgrade_reload", OK: true, BrokerImage: status, NextGeneration: 3}, `{"op":"broker_upgrade_reload","ok":true,"broker_image":{"running_sha256":"running","generation":2,"phase":"control_ready","cut_id":"cut","mode":"single"},"next_generation":3}`},
		{"unmeasured runtime", RuntimeReport{Schema: "admin_runtime", SchemaVersion: 2}, `{"schema":"admin_runtime","schema_version":2,"goroutines":0,"threads":0,"open_fds":0,"rss_bytes":0,"uptime_seconds":0,"reconcilers":null}`},
		{"measured runtime", RuntimeReport{Schema: "admin_runtime", SchemaVersion: 2, BrokerImage: status}, `{"schema":"admin_runtime","schema_version":2,"goroutines":0,"threads":0,"open_fds":0,"rss_bytes":0,"uptime_seconds":0,"reconcilers":null,"broker_image":{"running_sha256":"running","generation":2,"phase":"control_ready","cut_id":"cut","mode":"single"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.value)
			if err != nil || string(got) != tc.want {
				t.Fatalf("admin wire=%s want=%s err=%v", got, tc.want, err)
			}
			// An older local reader must still decode its known fields when
			// the new running-image observation is present or absent.
			var old struct {
				Op            string `json:"op"`
				OK            bool   `json:"ok"`
				Schema        string `json:"schema"`
				SchemaVersion int    `json:"schema_version"`
			}
			if err := json.Unmarshal(got, &old); err != nil {
				t.Fatal(err)
			}
			switch value := tc.value.(type) {
			case Response:
				if old.Op != value.Op || old.OK != value.OK {
					t.Fatal("additive status changed legacy response fields")
				}
			case RuntimeReport:
				if old.Schema != "admin_runtime" || old.SchemaVersion != 2 {
					t.Fatal("additive status bumped the local runtime schema")
				}
			default:
				t.Fatalf("unhandled protocol fixture %T", value)
			}
			back := reflect.New(reflect.TypeOf(tc.value))
			if err := json.Unmarshal(got, back.Interface()); err != nil || !reflect.DeepEqual(back.Elem().Interface(), tc.value) {
				t.Fatalf("local status did not roundtrip: %+v err=%v", back.Elem().Interface(), err)
			}
		})
	}
}
