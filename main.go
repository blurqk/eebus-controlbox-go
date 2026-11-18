package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	_ "github.com/joho/godotenv/autoload"
)

var remoteSki string

var frontend WebsocketClient

// main app
func usage() {
	fmt.Println("Usage: controlbox <port>")
	fmt.Println()
	fmt.Println("Certificate configuration via .env file:")
	fmt.Println("  CERT_PEM + KEY_PEM   inline PEM content")
	fmt.Println("  (auto-generated and persisted on first run if absent)")
	fmt.Println()
	fmt.Println("Time service for the Weidmueller ERC (smgw.local, NTP over TLS on port 4460):")
	fmt.Println("  on by default, SMGW_TIME=0 switches it off")
	fmt.Println("  SMGW_IP=a.b.c.d      address announced for smgw.local (default: arrival interface)")
}

func setupRoutes(h *controlbox) {
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(h, w, r)
	})
	// Serve the built frontend (npm run build, or start.sh) on the same port.
	if _, err := os.Stat(frontendDir + "/index.html"); err == nil {
		http.Handle("/", http.FileServer(http.Dir(frontendDir)))
		log.Printf("Frontend: http://localhost:%d/", httpdPort)
	} else {
		log.Printf("Frontend not built (%s missing) - run start.sh or npm run build in frontend/", frontendDir)
	}
}

func main() {
	if len(os.Args) != 2 {
		usage()
		os.Exit(1)
	}

	srv := new(controlbox)
	srv.run()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sig
		// close SHIP connections properly so remote devices see a regular
		// disconnect (e.g. the ERC starts its pairing replacement timer)
		srv.myService.Shutdown()
		os.Exit(0)
	}()

	// HTTP_PORT: port of the web interface (default 7080). UI=vue serves the
	// original Vue frontend instead of the Tasmota-style steuerbox page.
	port := httpdPort
	if p, err := strconv.Atoi(os.Getenv("HTTP_PORT")); err == nil && p > 0 {
		port = p
	}
	if os.Getenv("UI") == "vue" {
		setupRoutes(srv)
		log.Fatal(http.ListenAndServe(":"+strconv.Itoa(port), nil))
	}
	startTasmotaUI(srv, port)
}
