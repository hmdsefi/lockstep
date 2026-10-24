package bad

import (
	"context"
	"database/sql"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"sync"
)

// channels holds the channel operations (blocking).
func channels(ch chan int) {
	ch <- 1        // want `^LS100: channel send blocks; actors talk with lockstep\.Send \(actor code: \(\*Account\)\.Receive -> channels\)$`
	_ = <-ch       // want `^LS100: channel receive blocks; actors get input only as messages \(actor code: \(\*Account\)\.Receive -> channels\)$`
	for range ch { // want `^LS100: range over a channel blocks; actors get input only as messages \(actor code: \(\*Account\)\.Receive -> channels\)$`
	}
	select { // want `^LS100: select statement waits on channels; actors get input only as messages \(actor code: \(\*Account\)\.Receive -> channels\)$`
	case v := <-ch: // want `^LS100: channel receive blocks; actors get input only as messages \(actor code: \(\*Account\)\.Receive -> channels\)$`
		_ = v
	case ch <- 2: // want `^LS100: channel send blocks; actors talk with lockstep\.Send \(actor code: \(\*Account\)\.Receive -> channels\)$`
	}
}

// rangeChan ranges over a type parameter whose constraint allows a channel (blocking).
func rangeChan[C interface{ ~chan int }](ch C) {
	for range ch { // want `^LS100: range over a channel blocks; actors get input only as messages \(actor code: \(\*Account\)\.Receive -> rangeChan\)$`
	}
}

// calls holds one blocking call per package of ANL-140.
func calls(ctx context.Context, conn net.Conn, client *http.Client, req *http.Request, db *sql.DB, cmd *exec.Cmd, f *os.File) {
	var wg sync.WaitGroup
	wg.Wait() // want `^LS100: \(\*sync\.WaitGroup\)\.Wait blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls\)$`
	var mu sync.Mutex
	mu.Lock() // want `^LS100: \(\*sync\.Mutex\)\.Lock blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls\)$`
	mu.Unlock()
	_, _ = net.Dial("tcp", "localhost:1") // want `^LS100: net\.Dial blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls\)$`
	buf := make([]byte, 8)
	_, _ = conn.Read(buf)                   // want `^LS100: \(net\.Conn\)\.Read blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls\)$`
	_, _ = http.Get("http://localhost")     // want `^LS100: net/http\.Get blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls\)$`
	_, _ = client.Do(req)                   // want `^LS100: \(\*net/http\.Client\)\.Do blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls\)$`
	_, _ = rpc.Dial("tcp", "localhost:1")   // want `^LS100: net/rpc\.Dial blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls\)$`
	_, _ = db.QueryContext(ctx, "SELECT 1") // want `^LS100: \(\*database/sql\.DB\)\.QueryContext blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls\)$`
	_ = cmd.Run()                           // want `^LS100: \(\*os/exec\.Cmd\)\.Run blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls\)$`
	_, _ = os.ReadFile("x")                 // want `^LS100: os\.ReadFile blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls\)$`
	_, _ = f.Write(buf)                     // want `^LS100: \(\*os\.File\)\.Write blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls\)$`
	moreCalls(db)
}

// moreCalls holds further names of the table: prefixes, the any-type rule of net and the database
// rows.
func moreCalls(db *sql.DB) {
	_, _ = net.LookupHost("localhost") // want `^LS100: net\.LookupHost blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls -> moreCalls\)$`
	var ln net.Listener
	_, _ = ln.Accept() // want `^LS100: \(net\.Listener\)\.Accept blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls -> moreCalls\)$`
	var udp *net.UDPConn
	_, _, _ = udp.ReadFromUDP(nil) // want `^LS100: \(\*net\.UDPConn\)\.ReadFromUDP blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls -> moreCalls\)$`
	var rw sync.RWMutex
	rw.RLock()                               // want `^LS100: \(\*sync\.RWMutex\)\.RLock blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls -> moreCalls\)$`
	conn, _ := db.Conn(context.Background()) // want `^LS100: \(\*database/sql\.DB\)\.Conn blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls -> moreCalls\)$`
	_ = conn
	rows, _ := db.Query("SELECT 1") // want `^LS100: \(\*database/sql\.DB\)\.Query blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls -> moreCalls\)$`
	for rows.Next() {               // want `^LS100: \(\*database/sql\.Rows\)\.Next blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls -> moreCalls\)$`
	}
	_ = rows.Close()
	var client *rpc.Client
	_ = client.Call("S.M", 1, nil) // want `^LS100: \(\*net/rpc\.Client\)\.Call blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls -> moreCalls\)$`
	var srv *http.Server
	_ = srv.Shutdown(context.Background()) // want `^LS100: \(\*net/http\.Server\)\.Shutdown blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls -> moreCalls\)$`
	_, _ = os.Stat("x")                    // want `^LS100: os\.Stat blocks; run it as an effect with lockstep\.Do \(actor code: \(\*Account\)\.Receive -> calls -> moreCalls\)$`
}
