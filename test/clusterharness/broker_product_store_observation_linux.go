// Read the actual current store before Raft replay from the moved constructor.
// origin: hot-update S8 plan §12.1; S8 internal review upgrade-maketest F10
package clusterharness

import (
	"strings"
	"testing"
)

func productStoreOpenObservation(t *testing.T, read func(string) string, write func(string, string)) {
	t.Helper()
	path := "internal/brokerupgrade/brokerruntime/cut.go"
	source := read(path)
	anchor := "ImageOpenCurrent: storage.OpenCurrent"
	if strings.Count(source, anchor) != 1 {
		t.Fatal("product store observer lost the actual current-store constructor")
	}
	source = strings.Replace(source, anchor, `ImageOpenCurrent: func(cfg storage.OpenImageConfig) (*sql.DB,error) { db,err:=storage.OpenCurrent(cfg); if err==nil { s6ProductBeforeReplay(db) }; return db,err }`, 1)
	// The shutdown closure moved with the current-store constructor. Observe
	// its successful return at the real leaf, before the cut can enter exec.
	// origin: S6 lead decision D-C / plan §3.2 / T01
	shutdown := "return n.Shutdown()"
	if strings.Count(source, shutdown) != 1 {
		t.Fatal("product node observer lost the actual shutdown closure")
	}
	source = strings.Replace(source, shutdown, `err:=n.Shutdown();if err==nil {s6ProductNodeClosed()};return err`, 1)
	source = strings.Replace(source, `"encoding/hex"`, `"encoding/hex"`+"\n\"encoding/json\"\n\"path/filepath\"", 1)
	source += `
func s6ProductNodeClosed() {
 root:=""
 for i:=2;i<len(os.Args);i++ {name,value,inline:=strings.Cut(os.Args[i],"=");if name=="--db" {if !inline && i+1<len(os.Args){value=os.Args[i+1]};root=filepath.Dir(value)}}
 if root=="" {panic("product node observer: database argument missing")}
 data,err:=json.Marshal(map[string]any{"closed":true,"seq":time.Now().UnixNano()});if err!=nil {panic(err)}
 path:=filepath.Join(root,"node-closed.json")
 if err=os.WriteFile(path+".tmp",data,0600);err!=nil {panic(err)}
 if err=os.Rename(path+".tmp",path);err!=nil {panic(err)}
}
// Private read-only witness, before any replay can conceal a stale database.
func s6ProductBeforeReplay(db *sql.DB) {
 rows,err:=db.Query("SELECT fp,note FROM session_creators ORDER BY fp");if err!=nil {panic(err)}
 values:=map[string]string{}
 for rows.Next(){var fp,note string;if err=rows.Scan(&fp,&note);err!=nil {panic(err)};values[fp]=note}
 if err=rows.Close();err!=nil {panic(err)}
 var cursor uint64
 err=db.QueryRow("SELECT value FROM cluster_meta WHERE key='applied_index'").Scan(&cursor)
 if err!=nil && err!=sql.ErrNoRows {panic(err)}
 root:=""
 for i:=2;i<len(os.Args);i++ {name,value,inline:=strings.Cut(os.Args[i],"=");if name=="--db" {if !inline && i+1<len(os.Args){value=os.Args[i+1]};root=filepath.Dir(value)}}
 if root=="" {panic("product store observer: database argument missing")}
 data,err:=json.Marshal(map[string]any{"rows":values,"cursor":cursor});if err!=nil {panic(err)}
 path:=filepath.Join(root,"pre-replay.json")
 if err=os.WriteFile(path+".tmp",data,0600);err!=nil {panic(err)}
 if err=os.Rename(path+".tmp",path);err!=nil {panic(err)}
}
`
	write(path, source)
}
