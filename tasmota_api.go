package main

// Tasmota-compatible web interface.
//
// The Tasmota EEBUS control box (eebus-controlbox-tasmota) is operated through
// its page /steuerbox, which talks to the device only via console commands
// (/cm?cmnd=EEBus...) and their JSON answers. Home Assistant reads the same
// commands (EEBusData, EEBusStatus). This file answers those commands with the
// same JSON shape, so the unchanged steuerbox.html and existing integrations
// work against this Linux control box: switching over is a change of address.
//
// Implemented: EEBusStatus, EEBusPeers, EEBusScan, EEBusData, EEBusStruct,
// EEBusRead, EEBusCert, EEBusPair, EEBusSmgw, EEBusLpc, EEBusLimit, EEBusRelease,
// EEBusLpp, EEBusLppFrei, EEBusFailsafe, EEBusConnect, EEBusDisconnect,
// EEBusPeerMode. Anything else answers {"Command":"Unknown"} like Tasmota.

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	eebusapi "github.com/enbility/eebus-go/api"
	ucapi "github.com/enbility/eebus-go/usecases/api"
	shipapi "github.com/enbility/ship-go/api"
	spineapi "github.com/enbility/spine-go/api"
	"github.com/enbility/spine-go/model"
	"github.com/enbility/zeroconf/v2"
	"github.com/joho/godotenv"
	"github.com/miekg/dns"
)

//go:embed web/steuerbox.html
var steuerboxHTML []byte

type eebusEntityScenarios = eebusapi.RemoteEntityScenarios

// Limits without a duration argument get this one, as on the Tasmota (HEMS mode).
const defaultLimitDuration = 900 * time.Second

// ---- connection map shared by SHIP callbacks, SPINE events and HTTP handlers ----

type connMap struct {
	mu sync.RWMutex
	m  map[string]bool
}

func newConnMap() *connMap { return &connMap{m: map[string]bool{}} }

func (c *connMap) set(ski string, v bool) {
	c.mu.Lock()
	c.m[ski] = v
	c.mu.Unlock()
}

func (c *connMap) get(ski string) (bool, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.m[ski]
	return v, ok
}

func (c *connMap) snapshot() map[string]bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]bool, len(c.m))
	for k, v := range c.m {
		out[k] = v
	}
	return out
}

// ---- state kept per remote device ----

type peerState struct {
	since     time.Time // connected since
	hbAt      time.Time // last heartbeat received
	hbTmo     time.Duration
	hbLost    bool
	lostCnt   int
	lostAt    time.Time
	lostSince time.Time
	reconnAt  time.Time
	reconnCnt int
	lpcState  string // idle, wait, done, fail
	lpcResult string
	lpcSeq    int
	fsResult  string
	fsSeq     int
	err       string
	limitAt   time.Time // last loadControlLimitListData received
}

type mdnsPeer struct {
	Ski, Id, Brand, Model, Type, Cat, Register, Ip, Host, Path string
	Port                                                       int
	seen                                                       time.Time
}

type readRequest struct {
	ski    string
	ent    string
	feat   uint
	fn     model.FunctionType
	remote spineapi.FeatureRemoteInterface
	at     time.Time
}

type tasmotaAPI struct {
	h *controlbox

	mu        sync.Mutex
	peers     map[string]*peerState
	mdns      map[string]*mdnsPeer
	read      *readRequest
	ownSki    string
	shipID    string
	serial    string
	pairQR    string
	pairing   *shipapi.PairingTarget
	pairDone  bool
	reconnect bool
}

var tapi *tasmotaAPI

func newTasmotaAPI(h *controlbox, ownSki, shipID, serial string) *tasmotaAPI {
	return &tasmotaAPI{
		h: h, peers: map[string]*peerState{}, mdns: map[string]*mdnsPeer{},
		ownSki: ownSki, shipID: shipID, serial: serial,
		reconnect: os.Getenv("HB_RECONNECT") != "0",
	}
}

func (t *tasmotaAPI) peer(ski string) *peerState {
	ski = strings.ToLower(ski)
	p := t.peers[ski]
	if p == nil {
		p = &peerState{lpcState: "idle"}
		t.peers[ski] = p
	}
	return p
}

// connected is called from the SHIP callbacks.
func (t *tasmotaAPI) connected(ski string, on bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.peer(ski)
	if on {
		p.since = time.Now()
		p.err = ""
		p.hbAt = time.Time{}
		p.hbLost = false
		if t.pairing != nil && strings.EqualFold(t.pairing.SKI, ski) {
			t.pairDone = true
		}
	} else {
		p.since = time.Time{}
		p.err = "Verbindung getrennt"
	}
}

// HandleEvent records heartbeats and limit updates of every remote device.
func (t *tasmotaAPI) HandleEvent(payload spineapi.EventPayload) {
	if payload.EventType != spineapi.EventTypeDataChange || payload.Ski == "" {
		return
	}
	switch data := payload.Data.(type) {
	case *model.DeviceDiagnosisHeartbeatDataType:
		if payload.Function != model.FunctionTypeDeviceDiagnosisHeartbeatData {
			return
		}
		t.mu.Lock()
		p := t.peer(payload.Ski)
		p.hbAt = time.Now()
		p.hbLost = false
		p.lostSince = time.Time{}
		if data != nil && data.HeartbeatTimeout != nil {
			if d, err := data.HeartbeatTimeout.GetTimeDuration(); err == nil {
				p.hbTmo = d
			}
		}
		t.mu.Unlock()
	case *model.LoadControlLimitListDataType:
		t.mu.Lock()
		t.peer(payload.Ski).limitAt = time.Now()
		t.mu.Unlock()
	}
}

// watchdog marks lost heartbeats like the Tasmota (twice the promised timeout,
// 120 s if none was promised) and, unlike the Tasmota, reconnects a device whose
// heartbeat stays away: a connection without heartbeat looks fine but no longer
// carries limits (ERC, 06.11.-17.11.2026). Energy managers (evcc) are never
// disconnected from here, they connect by themselves. HB_RECONNECT=0 disables it.
func (t *tasmotaAPI) watchdog() {
	for range time.Tick(5 * time.Second) {
		var drop []string
		t.mu.Lock()
		for ski, p := range t.peers {
			if p.since.IsZero() || p.hbAt.IsZero() {
				continue
			}
			tmo := p.hbTmo
			if tmo <= 0 {
				tmo = 120 * time.Second
			}
			if !p.hbLost && time.Since(p.hbAt) > 2*tmo {
				p.hbLost = true
				p.lostCnt++
				p.lostAt = time.Now()
				p.lostSince = time.Now()
				log.Printf("Herzschlag von %s verloren (letzter vor %s)", ski, time.Since(p.hbAt).Round(time.Second))
			}
			if t.reconnect && p.hbLost && time.Since(p.lostSince) > 3*time.Minute &&
				time.Since(p.reconnAt) > 10*time.Minute && !t.isHemsLocked(ski) {
				p.reconnAt = time.Now()
				p.reconnCnt++
				drop = append(drop, ski)
			}
		}
		t.mu.Unlock()
		for _, ski := range drop {
			log.Printf("Herzschlag von %s bleibt aus - Verbindung wird neu aufgebaut", ski)
			t.h.myService.DisconnectService(t.identityOf(ski), "heartbeat timeout")
		}
	}
}

// identityOf returns the full SHIP identity of a known remote service.
func (t *tasmotaAPI) identityOf(ski string) shipapi.ServiceIdentity {
	identity := shipapi.NewServiceIdentity(ski, "", "")
	if d := t.h.myService.RemoteServiceFor(identity); d != nil {
		identity = shipapi.NewServiceIdentity(d.SKI(), d.Fingerprint(), d.ShipID())
	}
	return identity
}

// isHemsLocked: true if the device is an energy manager (t.mu held).
func (t *tasmotaAPI) isHemsLocked(ski string) bool {
	if m := t.mdns[strings.ToLower(ski)]; m != nil {
		return isHemsType(m.Type)
	}
	return false
}

func isHemsType(typ string) bool {
	return strings.Contains(strings.ToLower(strings.ReplaceAll(typ, " ", "")), "energymanag")
}

// ---- mDNS: addresses of the SHIP services (ship-go does not expose them) ----

func (t *tasmotaAPI) browse() {
	for {
		entries := make(chan *zeroconf.ServiceEntry, 16)
		removed := make(chan *zeroconf.ServiceEntry, 16)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			for {
				select {
				case e, ok := <-entries:
					if !ok {
						return
					}
					t.mdnsEntry(e)
				case <-removed:
				case <-ctx.Done():
					return
				}
			}
		}()
		if err := zeroconf.Browse(ctx, "_ship._tcp", "local.", entries, removed); err != nil {
			log.Printf("mDNS browse: %v", err)
		}
		cancel()
		time.Sleep(10 * time.Second)
	}
}

func (t *tasmotaAPI) mdnsEntry(e *zeroconf.ServiceEntry) {
	if e == nil {
		return
	}
	txt := map[string]string{}
	for _, s := range e.Text {
		if k, v, ok := strings.Cut(unescapeDNS(s), "="); ok {
			txt[strings.ToLower(k)] = v
		}
	}
	ski := strings.ToLower(txt["ski"])
	if ski == "" || ski == strings.ToLower(t.ownSki) {
		return
	}
	ip := ""
	for _, a := range e.AddrIPv4 {
		if a.To4() != nil {
			ip = a.String()
			break
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.mdns[ski]
	if m == nil {
		m = &mdnsPeer{}
		t.mdns[ski] = m
	}
	if ip == "" { // later announcements may come without address record
		ip = m.Ip
	}
	*m = mdnsPeer{
		Ski: ski, Id: txt["id"], Brand: txt["brand"], Model: txt["model"], Type: txt["type"],
		Cat: txt["cat"], Register: txt["register"], Ip: ip, Host: strings.TrimSuffix(strings.TrimSuffix(e.HostName, "."), ".local"),
		Path: txt["path"], Port: e.Port, seen: time.Now(),
	}
	if m.Ip == "" && m.Host != "" {
		go t.resolveHost(ski, m.Host)
	}
}

// unescapeDNS turns the \\DDD escapes of DNS text (UTF-8 bytes) back into text.
func unescapeDNS(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 >= len(s) {
			b = append(b, s[i])
			continue
		}
		if i+3 < len(s) && isDigits(s[i+1:i+4]) {
			n, _ := strconv.Atoi(s[i+1 : i+4])
			b = append(b, byte(n))
			i += 3
			continue
		}
		b = append(b, s[i+1])
		i++
	}
	return string(b)
}

func isDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}

// mdnsLookupA sends a one-shot (legacy unicast) mDNS query for an A record.
func mdnsLookupA(ctx context.Context, name string) (string, error) {
	c, err := net.ListenUDP("udp4", nil)
	if err != nil {
		return "", err
	}
	defer c.Close()
	q := new(dns.Msg)
	q.SetQuestion(name, dns.TypeA)
	q.RecursionDesired = false
	pkt, err := q.Pack()
	if err != nil {
		return "", err
	}
	if _, err := c.WriteToUDP(pkt, mdnsGroup); err != nil {
		return "", err
	}
	dl, _ := ctx.Deadline()
	_ = c.SetReadDeadline(dl)
	buf := make([]byte, 9000)
	for {
		n, _, err := c.ReadFromUDP(buf)
		if err != nil {
			return "", err
		}
		var r dns.Msg
		if r.Unpack(buf[:n]) != nil {
			continue
		}
		for _, rr := range append(r.Answer, r.Extra...) {
			if a, ok := rr.(*dns.A); ok && strings.EqualFold(a.Hdr.Name, name) {
				return a.A.String(), nil
			}
		}
	}
}

// resolveHost asks for the IPv4 address of <host>.local via mDNS (the container
// has no mDNS resolver of its own).
func (t *tasmotaAPI) resolveHost(ski, host string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ip, err := mdnsLookupA(ctx, host+".local.")
	if err != nil || ip == "" {
		return
	}
	t.mu.Lock()
	if m := t.mdns[ski]; m != nil && m.Ip == "" {
		m.Ip = ip
	}
	t.mu.Unlock()
}

// ---- HTTP ----

func (t *tasmotaAPI) routes(mux *http.ServeMux) {
	page := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/steuerbox" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(steuerboxHTML)
	}
	mux.HandleFunc("/", page)
	mux.HandleFunc("/steuerbox", page)
	mux.HandleFunc("/cm", t.serveCm)
	mux.HandleFunc("/ufsd", t.serveDownload)
}

// serveDownload answers the identity backup of the page. The certificate is
// public; the private key is not served over HTTP - it lives in .env on the
// server and is backed up from there.
func (t *tasmotaAPI) serveDownload(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Query().Get("download") {
	case "/eebus_cert.der":
		cert := t.h.myService.Configuration().Certificate()
		if len(cert.Certificate) == 0 {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", "attachment; filename=eebus_cert.der")
		_, _ = w.Write(cert.Certificate[0])
	case "/eebus_key.der":
		http.Error(w, "Der private Schluessel wird nicht ueber das Netz ausgeliefert. Er liegt auf dem Server in .env (KEY_PEM).", http.StatusForbidden)
	default:
		http.NotFound(w, r)
	}
}

func (t *tasmotaAPI) serveCm(w http.ResponseWriter, r *http.Request) {
	line := strings.TrimSpace(r.URL.Query().Get("cmnd"))
	cmd, arg, _ := strings.Cut(line, " ")
	arg = strings.TrimSpace(arg)
	var res any
	switch strings.ToLower(cmd) {
	case "eebusstatus":
		res = t.cmdStatus()
	case "eebuspeers":
		res = t.cmdPeers("EEBusPeers")
	case "eebusscan":
		res = map[string]any{"EEBusScan": "Scanning"}
	case "eebusdata":
		res = t.cmdData()
	case "eebusstruct":
		res = t.cmdStruct(arg)
	case "eebusread":
		res = t.cmdRead(arg)
	case "eebuscert":
		res = t.cmdCert(arg)
	case "eebuspair":
		res = t.cmdPair(arg)
	case "eebussmgw":
		res = t.cmdSmgw()
	case "eebuslpc", "eebuslimit":
		res = t.cmdLimit("EEBusLpc", arg, false, true)
	case "eebusrelease":
		res = t.cmdLimit("EEBusRelease", arg, false, false)
	case "eebuslpp":
		res = t.cmdLimit("EEBusLpp", arg, true, true)
	case "eebuslppfrei":
		res = t.cmdLimit("EEBusLppFrei", arg, true, false)
	case "eebusfailsafe":
		res = t.cmdFailsafe(arg)
	case "eebusconnect":
		res = t.cmdConnect(arg)
	case "eebusdisconnect":
		res = t.cmdDisconnect(arg)
	case "eebuspeermode":
		res = map[string]any{"EEBusPeerMode": "Betriebsart folgt dem Geraetetyp"}
	default:
		res = map[string]any{"Command": "Unknown"}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	_ = json.NewEncoder(w).Encode(res)
}

// ---- helpers ----

// num renders a value like the Tasmota: [mantissa, exponent].
func num(v float64) []int64 {
	if math.IsInf(v, 0) || math.Abs(v) >= 1e12 {
		return []int64{1000000000000, 0} // "unbegrenzt" on the page
	}
	if math.IsNaN(v) {
		return []int64{0, 0}
	}
	if v == math.Round(v) {
		return []int64{int64(v), 0}
	}
	for exp := int64(1); exp <= 3; exp++ {
		s := math.Pow(10, float64(exp))
		if m := v * s; math.Abs(m-math.Round(m)) < 1e-6 || exp == 3 {
			return []int64{int64(math.Round(m)), -exp}
		}
	}
	return []int64{int64(math.Round(v)), 0}
}

func isoSecs(d time.Duration) string {
	return fmt.Sprintf("PT%dS", int64(d/time.Second))
}

func localTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Local().Format("2006-01-02T15:04:05")
}

func (t *tasmotaAPI) connectedSkis() []string {
	out := []string{}
	for ski, on := range t.h.isConnected.snapshot() {
		if on {
			out = append(out, strings.ToLower(ski))
		}
	}
	sort.Strings(out)
	return out
}

// resolve finds the connected device addressed by a SKI prefix or an IP address.
func (t *tasmotaAPI) resolve(sel string) (string, string) {
	sel = strings.ToLower(strings.TrimSpace(sel))
	if sel == "" {
		return "", "kein Geraet angegeben"
	}
	skis := t.connectedSkis()
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, ski := range skis {
		if strings.Contains(sel, ".") {
			if m := t.mdns[ski]; m != nil && m.Ip == sel {
				return ski, ""
			}
		} else if strings.HasPrefix(ski, sel) {
			return ski, ""
		}
	}
	return "", "Geraet " + sel + " nicht verbunden"
}

func (t *tasmotaAPI) ipOf(ski string) string {
	t.mu.Lock()
	m := t.mdns[ski]
	t.mu.Unlock()
	if m != nil && m.Ip != "" {
		return m.Ip
	}
	if d := t.h.myService.RemoteServiceFor(shipapi.NewServiceIdentity(ski, "", "")); d != nil {
		return d.IPv4()
	}
	return ""
}

// entitiesFor returns the remote entities of a device supporting the use case.
func entitiesFor(scenarios []eebusEntityScenarios, ski string) []spineapi.EntityRemoteInterface {
	out := []spineapi.EntityRemoteInterface{}
	for _, sc := range scenarios {
		if sc.Entity != nil && sc.Entity.Device() != nil && strings.EqualFold(sc.Entity.Device().Ski(), ski) {
			out = append(out, sc.Entity)
		}
	}
	return out
}

func (t *tasmotaAPI) device(ski string) spineapi.DeviceRemoteInterface {
	t.h.mutex.Lock()
	defer t.h.mutex.Unlock()
	for k, info := range t.h.remoteInfos {
		if strings.EqualFold(k, ski) && info.Device != nil {
			return info.Device
		}
	}
	return nil
}

func (t *tasmotaAPI) useCases(ski string) []string {
	t.h.mutex.Lock()
	defer t.h.mutex.Unlock()
	for k, info := range t.h.remoteInfos {
		if strings.EqualFold(k, ski) {
			return slices.Clone(info.UseCases)
		}
	}
	return nil
}

// ---- EEBusStatus ----

type slotJSON struct {
	Slot      int     `json:"Slot"`
	State     string  `json:"State"`
	Sme       string  `json:"Sme"`
	Ip        string  `json:"Ip"`
	Port      int     `json:"Port"`
	PeerSki   string  `json:"PeerSki"`
	PeerId    string  `json:"PeerId"`
	Lpc       string  `json:"Lpc"`
	LpcResult string  `json:"LpcResult"`
	FsResult  string  `json:"FsResult"`
	HbAge     *int64  `json:"PeerHerzschlagS,omitempty"`
	HbTmo     *int64  `json:"ZusageS,omitempty"`
	Lost      *int    `json:"Verloren,omitempty"`
	LostAt    *string `json:"VerlorenAm,omitempty"`
	Reconn    *int    `json:"Neuaufbau,omitempty"`
	Error     string  `json:"Error"`
}

func (t *tasmotaAPI) cmdStatus() any {
	conn := t.h.isConnected.snapshot()
	t.mu.Lock()
	defer t.mu.Unlock()
	skis := []string{}
	for ski := range t.peers {
		skis = append(skis, ski)
	}
	if t.pairing != nil {
		ps := strings.ToLower(t.pairing.SKI)
		if !slices.Contains(skis, ps) {
			skis = append(skis, ps)
		}
	}
	sort.Slice(skis, func(i, j int) bool {
		a, b := t.mdns[skis[i]], t.mdns[skis[j]]
		ai, bi := "", ""
		if a != nil {
			ai = a.Ip
		}
		if b != nil {
			bi = b.Ip
		}
		if ai != bi {
			return ai < bi
		}
		return skis[i] < skis[j]
	})
	slots := []slotJSON{}
	active := 0
	for _, ski := range skis {
		on := false
		for k, v := range conn {
			if strings.EqualFold(k, ski) && v {
				on = true
			}
		}
		p := t.peer(ski)
		s := slotJSON{Slot: len(slots), State: "idle", Sme: "off", PeerSki: ski, Lpc: p.lpcState,
			LpcResult: p.lpcResult, FsResult: p.fsResult, Error: p.err}
		if m := t.mdns[ski]; m != nil {
			s.Ip, s.Port, s.PeerId = m.Ip, m.Port, m.Id
		}
		if s.Ip == "" {
			if d := t.h.myService.RemoteServiceFor(shipapi.NewServiceIdentity(ski, "", "")); d != nil {
				s.Ip = d.IPv4()
				if s.PeerId == "" {
					s.PeerId = d.ShipID()
				}
			}
		}
		if t.pairing != nil && strings.EqualFold(t.pairing.SKI, ski) && s.PeerId == "" {
			s.PeerId = t.pairing.ShipID
		}
		if on {
			s.State, s.Sme, s.Error = "cmi_ok", "done", ""
			active++
		} else if s.Error == "" {
			s.Error = "nicht verbunden"
		}
		if !p.hbAt.IsZero() || p.lostCnt > 0 {
			age := int64(-1)
			if !p.hbAt.IsZero() {
				age = int64(time.Since(p.hbAt) / time.Second)
			}
			tmo := int64(p.hbTmo / time.Second)
			lost, lostAt, rc := p.lostCnt, localTime(p.lostAt), p.reconnCnt
			s.HbAge, s.HbTmo, s.Lost, s.LostAt, s.Reconn = &age, &tmo, &lost, &lostAt, &rc
		}
		slots = append(slots, s)
	}
	return map[string]any{"EEBusStatus": map[string]any{"Role": 1, "Hems": 1, "Active": active, "Slots": slots}}
}

// ---- EEBusPeers ----

type peerJSON struct {
	Ski      string `json:"Ski"`
	Id       string `json:"Id"`
	Model    string `json:"Model"`
	Brand    string `json:"Brand"`
	Type     string `json:"Type"`
	Cat      string `json:"Cat"`
	Register string `json:"Register"`
	Ip       string `json:"Ip"`
	Host     string `json:"Host"`
	Port     int    `json:"Port"`
	Path     string `json:"Path"`
	Mode     string `json:"Mode"`
	ModeSrc  string `json:"ModeSrc"`
}

func (t *tasmotaAPI) cmdPeers(name string) any {
	t.mu.Lock()
	defer t.mu.Unlock()
	list := []peerJSON{}
	for _, m := range t.mdns {
		if time.Since(m.seen) > 15*time.Minute {
			continue
		}
		mode := "steuve"
		if isHemsType(m.Type) {
			mode = "hems"
		}
		list = append(list, peerJSON{Ski: m.Ski, Id: m.Id, Model: m.Model, Brand: m.Brand, Type: m.Type,
			Cat: m.Cat, Register: m.Register, Ip: m.Ip, Host: m.Host, Port: m.Port, Path: m.Path,
			Mode: mode, ModeSrc: "typ"})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Ip < list[j].Ip })
	return map[string]any{name: map[string]any{"Found": len(list), "Peers": list}}
}

// ---- EEBusData ----

type limJSON struct {
	Aktiv bool    `json:"Aktiv"`
	Wert  []int64 `json:"Wert"`
	Dauer string  `json:"Dauer,omitempty"`
	RestS *int64  `json:"RestS,omitempty"`
}

func (t *tasmotaAPI) cmdData() any {
	peers := []map[string]any{}
	for _, ski := range t.connectedSkis() {
		d := map[string]any{"Ski": ski, "Ip": t.ipOf(ski)}
		t.mu.Lock()
		limitAt := t.peer(ski).limitAt
		t.mu.Unlock()
		lim := func(l ucapi.LoadLimit, negative bool) limJSON {
			v := l.Value
			if negative && v > 0 {
				v = -v
			}
			j := limJSON{Aktiv: l.IsActive, Wert: num(v)}
			if l.Duration > 0 {
				rest := l.Duration
				if !limitAt.IsZero() {
					rest -= time.Since(limitAt)
				}
				if rest < 0 {
					rest = 0
				}
				rs := int64(rest / time.Second)
				j.Dauer, j.RestS = isoSecs(rest), &rs
			}
			return j
		}
		for _, e := range entitiesFor(t.h.uclpc.RemoteEntitiesScenarios(), ski) {
			if l, err := t.h.uclpc.ConsumptionLimit(e); err == nil {
				d["Lim0"] = lim(l, false)
			}
			if v, err := t.h.uclpc.FailsafeConsumptionActivePowerLimit(e); err == nil {
				d["FsCons"] = num(v)
			}
			if v, err := t.h.uclpc.FailsafeDurationMinimum(e); err == nil {
				d["FsDur"] = isoSecs(v)
			}
			if v, err := t.h.uclpc.ConsumptionNominalMax(e); err == nil {
				d["PnomCons"] = num(v)
			}
		}
		for _, e := range entitiesFor(t.h.uclpp.RemoteEntitiesScenarios(), ski) {
			if l, err := t.h.uclpp.ProductionLimit(e); err == nil {
				d["Lim1"] = lim(l, true)
			}
			if v, err := t.h.uclpp.FailsafeProductionActivePowerLimit(e); err == nil {
				d["FsProd"] = num(v)
			}
			if _, ok := d["FsDur"]; !ok {
				if v, err := t.h.uclpp.FailsafeDurationMinimum(e); err == nil {
					d["FsDur"] = isoSecs(v)
				}
			}
			if v, err := t.h.uclpp.ProductionNominalMax(e); err == nil {
				d["PnomProd"] = num(v)
			}
		}
		// measurements: grid connection point first, else the device's own consumption
		for _, e := range entitiesFor(t.h.ucmgcp.RemoteEntitiesScenarios(), ski) {
			if v, err := t.h.ucmgcp.Power(e); err == nil {
				d["Power"] = num(math.Round(v))
			}
			if v, err := t.h.ucmgcp.EnergyFeedIn(e); err == nil {
				d["FeedIn"] = num(math.Round(v))
			}
			if v, err := t.h.ucmgcp.EnergyConsumed(e); err == nil {
				d["Consum"] = num(math.Round(v))
			}
			if v, err := t.h.ucmgcp.CurrentPerPhase(e); err == nil && len(v) > 0 {
				d["Current"] = num(math.Round(slices.Max(v)*100) / 100)
			}
			if v, err := t.h.ucmgcp.VoltagePerPhase(e); err == nil && len(v) > 0 {
				d["Voltage"] = num(math.Round(v[0]*10) / 10)
			}
			if v, err := t.h.ucmgcp.Frequency(e); err == nil {
				d["Freq"] = num(math.Round(v*100) / 100)
			}
			if v, err := t.h.ucmgcp.PowerLimitationFactor(e); err == nil {
				if v <= 1 {
					v *= 100
				}
				d["Plf"] = num(math.Round(v))
			}
		}
		if _, ok := d["Power"]; !ok {
			for _, e := range entitiesFor(t.h.ucmpc.RemoteEntitiesScenarios(), ski) {
				if v, err := t.h.ucmpc.Power(e); err == nil {
					d["Power"] = num(math.Round(v))
				}
				if v, err := t.h.ucmpc.EnergyConsumed(e); err == nil {
					d["Consum"] = num(math.Round(v))
				}
				if v, err := t.h.ucmpc.EnergyProduced(e); err == nil {
					d["FeedIn"] = num(math.Round(v))
				}
				if v, err := t.h.ucmpc.CurrentPerPhase(e); err == nil && len(v) > 0 {
					d["Current"] = num(math.Round(slices.Max(v)*100) / 100)
				}
				if v, err := t.h.ucmpc.VoltagePerPhase(e); err == nil && len(v) > 0 {
					d["Voltage"] = num(math.Round(v[0]*10) / 10)
				}
				if v, err := t.h.ucmpc.Frequency(e); err == nil {
					d["Freq"] = num(math.Round(v*100) / 100)
				}
			}
		}
		peers = append(peers, d)
	}
	return map[string]any{"EEBusData": map[string]any{"Peers": peers}}
}

// ---- EEBusStruct ----

func addrText(a []model.AddressEntityType) string {
	parts := make([]string, len(a))
	for i, x := range a {
		parts[i] = strconv.FormatUint(uint64(x), 10)
	}
	return strings.Join(parts, ".")
}

func (t *tasmotaAPI) cmdStruct(arg string) any {
	sel, search, _ := strings.Cut(arg, " ")
	search = strings.TrimSpace(search)
	if sel == "" {
		return map[string]any{"EEBusStruct": "Nutzung: EEBusStruct <ski|ip> [Suchwort]"}
	}
	ski, msg := t.resolve(sel)
	if ski == "" {
		return map[string]any{"EEBusStruct": msg}
	}
	dev := t.device(ski)
	if dev == nil {
		return map[string]any{"EEBusStruct": "keine Selbstauskunft vorhanden - erst verbinden"}
	}
	type hit struct {
		Ent   string `json:"Ent"`
		Feat  uint   `json:"Feat"`
		Typ   string `json:"Typ"`
		Rolle string `json:"Rolle"`
	}
	entities, features, hits := []string{}, []string{}, []hit{}
	for _, e := range dev.Entities() {
		if e.Address() == nil {
			continue
		}
		adr := addrText(e.Address().Entity)
		if adr == "" {
			continue
		}
		entities = append(entities, adr+" "+string(e.EntityType()))
		for _, f := range e.Features() {
			if f.Address() == nil || f.Address().Feature == nil {
				continue
			}
			fid := uint(*f.Address().Feature)
			features = append(features, fmt.Sprintf("%s.%d %s/%s", adr, fid, f.Type(), f.Role()))
			if search != "" && strings.Contains(strings.ToLower(string(f.Type())), strings.ToLower(search)) {
				hits = append(hits, hit{Ent: adr, Feat: fid, Typ: string(f.Type()), Rolle: string(f.Role())})
			}
		}
	}
	if search != "" {
		return map[string]any{"EEBusStruct": map[string]any{"Suche": search, "Treffer": hits, "Anzahl": len(hits)}}
	}
	actors, ucs := []string{}, []string{}
	for _, uc := range dev.UseCases() {
		if uc.Actor != nil && !slices.Contains(actors, string(*uc.Actor)) {
			actors = append(actors, string(*uc.Actor))
		}
		for _, s := range uc.UseCaseSupport {
			if s.UseCaseName != nil && !slices.Contains(ucs, string(*s.UseCaseName)) {
				ucs = append(ucs, string(*s.UseCaseName))
			}
		}
	}
	return map[string]any{"EEBusStruct": map[string]any{
		"Entities": entities, "Features": features, "Actors": actors, "UseCases": ucs,
		"AnzEntities": len(entities), "AnzFeatures": len(features), "AnzActors": len(actors), "AnzUseCases": len(ucs)}}
}

// ---- EEBusRead ----

var readFunctions = map[string]model.FunctionType{
	"measurement":          model.FunctionTypeMeasurementListData,
	"electricalconnection": model.FunctionTypeElectricalConnectionParameterDescriptionListData,
	"deviceconfiguration":  model.FunctionTypeDeviceConfigurationKeyValueListData,
	"loadcontrol":          model.FunctionTypeLoadControlLimitListData,
	"deviceclassification": model.FunctionTypeDeviceClassificationManufacturerData,
	"devicediagnosis":      model.FunctionTypeDeviceDiagnosisStateData,
}

func (t *tasmotaAPI) cmdRead(arg string) any {
	if arg == "" { // fetch the answer of the last request
		t.mu.Lock()
		rq := t.read
		t.mu.Unlock()
		if rq == nil || time.Since(rq.at) < 500*time.Millisecond {
			return map[string]any{"EEBusRead": map[string]any{}}
		}
		data := rq.remote.DataCopy(rq.fn)
		if data == nil {
			return map[string]any{"EEBusRead": map[string]any{"Ent": rq.ent, "Feat": rq.feat}}
		}
		raw, _ := json.Marshal(map[string]any{"data": map[string]any{"payload": map[string]any{
			"datagram": map[string]any{"payload": map[string]any{"cmd": map[string]any{string(rq.fn): data}}}}}})
		return map[string]any{"EEBusRead": map[string]any{"Ent": rq.ent, "Feat": rq.feat, "Antwort": string(raw)}}
	}
	a := strings.Fields(arg)
	if len(a) < 4 {
		return map[string]any{"EEBusRead": "Nutzung: EEBusRead <ski|ip> <entity> <feature> <typ>"}
	}
	fn, ok := readFunctions[strings.ToLower(a[3])]
	if !ok {
		return map[string]any{"EEBusRead": "unbekannter Typ " + a[3]}
	}
	ski, msg := t.resolve(a[0])
	if ski == "" {
		return map[string]any{"EEBusRead": msg}
	}
	dev := t.device(ski)
	if dev == nil {
		return map[string]any{"EEBusRead": "keine Selbstauskunft vorhanden"}
	}
	var addr []model.AddressEntityType
	for _, p := range strings.Split(a[1], ".") {
		n, err := strconv.ParseUint(p, 10, 32)
		if err != nil {
			return map[string]any{"EEBusRead": "Entity-Adresse ungueltig"}
		}
		addr = append(addr, model.AddressEntityType(n))
	}
	fid, err := strconv.ParseUint(a[2], 10, 32)
	if err != nil {
		return map[string]any{"EEBusRead": "Feature-Adresse ungueltig"}
	}
	entity := dev.Entity(addr)
	if entity == nil {
		return map[string]any{"EEBusRead": "Entity " + a[1] + " gibt es bei der Gegenstelle nicht"}
	}
	fa := model.AddressFeatureType(fid)
	remote := entity.FeatureOfAddress(&fa)
	if remote == nil {
		return map[string]any{"EEBusRead": "Feature " + a[2] + " gibt es bei der Gegenstelle nicht"}
	}
	local := t.h.myService.LocalDevice().EntityForType(model.EntityTypeTypeGridGuard).GetOrAddFeature(remote.Type(), model.RoleTypeClient)
	if _, e := local.RequestRemoteData(fn, nil, nil, remote); e != nil {
		return map[string]any{"EEBusRead": "nicht gesendet: " + e.String()}
	}
	t.mu.Lock()
	t.read = &readRequest{ski: ski, ent: a[1], feat: uint(fid), fn: fn, remote: remote, at: time.Now()}
	t.mu.Unlock()
	return map[string]any{"EEBusRead": map[string]any{"Gesendet": true, "Ent": a[1], "Feat": fid, "Funktion": string(fn)}}
}

// ---- EEBusCert ----

func (t *tasmotaAPI) cmdCert(arg string) any {
	if arg == "2" {
		return map[string]any{"EEBusCert": "Eine neue Identitaet wird auf dem Server erzeugt (CERT_PEM/KEY_PEM in .env entfernen und den Dienst neu starten)."}
	}
	return map[string]any{"EEBusCert": map[string]any{"Ski": t.ownSki, "Id": t.shipID, "Serial": t.serial, "New": false}}
}

// ---- EEBusPair ----

func (t *tasmotaAPI) setPairing(target *shipapi.PairingTarget, qr string) {
	t.mu.Lock()
	t.pairing, t.pairQR, t.pairDone = target, qr, false
	t.mu.Unlock()
}

func (t *tasmotaAPI) cmdPair(arg string) any {
	svc := t.h.myService
	switch {
	case arg == "0":
		t.mu.Lock()
		p := t.pairing
		t.mu.Unlock()
		if p != nil {
			_ = svc.StopAnnouncementTo(p.ShipID)
		}
		t.setPairing(nil, "")
		_ = updateEnv(map[string]string{"PAIRING_QR": ""})
	case arg != "":
		target, err := parsePairingQR(arg)
		if err != nil {
			return map[string]any{"EEBusPair": "QR-Text ungueltig - erwartet SHIP;SKI:..;ID:..;FPH256:..;SPSEC:..;ENDSHIP;"}
		}
		svc.RegisterRemoteService(shipapi.NewServiceIdentity(target.SKI, target.Fingerprint, target.ShipID))
		if err := svc.StartAnnouncementTo(target); err != nil {
			return map[string]any{"EEBusPair": "Ankuendigung fehlgeschlagen: " + err.Error()}
		}
		t.setPairing(&target, arg)
		if err := updateEnv(map[string]string{"PAIRING_QR": arg}); err != nil {
			log.Printf("PAIRING_QR nicht gespeichert: %v", err)
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	res := map[string]any{"Ziel": "", "Id": "", "Marke": "", "Modell": "", "Angekuendigt": false,
		"Abgeschlossen": false, "Verbunden": false, "VerbundenS": 0}
	if p := t.pairing; p != nil {
		ski := strings.ToLower(p.SKI)
		res["Ziel"], res["Id"] = ski, p.ShipID
		if m := t.mdns[ski]; m != nil {
			res["Marke"], res["Modell"] = m.Brand, m.Model
		}
		res["Angekuendigt"] = svc.IsAnnouncingTo(p.ShipID)
		on, _ := t.h.isConnected.get(ski)
		if !on {
			on, _ = t.h.isConnected.get(strings.ToUpper(ski))
		}
		res["Verbunden"] = on
		res["Abgeschlossen"] = t.pairDone || on
		if ps := t.peers[ski]; on && ps != nil && !ps.since.IsZero() {
			res["VerbundenS"] = int64(time.Since(ps.since) / time.Second)
		}
	}
	return map[string]any{"EEBusPair": res}
}

// updateEnv changes keys in .env (empty value removes the key), keeping the rest.
func updateEnv(kv map[string]string) error {
	env, err := godotenv.Read(".env")
	if err != nil && !os.IsNotExist(err) {
		return err // never overwrite a .env that could not be read
	}
	if env == nil {
		env = map[string]string{}
	}
	for k, v := range kv {
		if v == "" {
			delete(env, k)
		} else {
			env[k] = v
		}
	}
	return godotenv.Write(env, ".env")
}

// ---- EEBusSmgw ----

func (t *tasmotaAPI) cmdSmgw() any {
	on := os.Getenv("SMGW_TIME") != "0"
	st := timeStats()
	last, gap, fail := "", "", ""
	if !st.last.IsZero() {
		last = st.last.UTC().Format("2006-01-02T15:04:05Z")
	}
	if !st.last.IsZero() && !st.prev.IsZero() {
		d := st.last.Sub(st.prev)
		days := int(d.Hours()) / 24
		d -= time.Duration(days) * 24 * time.Hour
		gap = fmt.Sprintf("%dT%02d:%02d:%02d", days, int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60)
	}
	if !st.failedAt.IsZero() {
		fail = st.failedAt.UTC().Format("2006-01-02T15:04:05Z")
	}
	smgw, ziel := 0, "eigene"
	if on {
		smgw = 1
	}
	if ip := os.Getenv("SMGW_IP"); ip != "" {
		ziel = ip
	}
	return map[string]any{"EEBusSmgw": map[string]any{"Smgw": smgw, "Ziel": ziel, "Aktiv": on, "Zeitdienst": on,
		"UhrGueltig": true, "Abrufe": st.served, "LetzterAbruf": last, "Abstand": gap,
		"Fehlversuche": st.failed, "LetzterFehlversuch": fail}}
}

// ---- limits ----

func (t *tasmotaAPI) cmdLimit(name, arg string, production, activate bool) any {
	a := strings.Fields(arg)
	if len(a) < 1 || (activate && len(a) < 2) {
		return map[string]any{name: "Nutzung: " + name + " <ski|ip> <watt> [dauer_s]"}
	}
	watt := 0.0
	if len(a) >= 2 {
		w, err := strconv.ParseFloat(strings.Replace(a[1], ",", ".", 1), 64)
		if err != nil {
			return map[string]any{name: "Leistung ungueltig"}
		}
		watt = math.Abs(w)
	}
	dur := defaultLimitDuration
	if len(a) >= 3 {
		s, err := strconv.ParseInt(a[2], 10, 64)
		if err != nil || s < 0 || s > 86400 {
			return map[string]any{name: map[string]any{"Fehler": fmt.Sprintf("Dauer %s s unzulaessig - erlaubt 0..86400 s (0 = unbefristet)", a[2])}}
		}
		dur = time.Duration(s) * time.Second
	}
	ski, msg := t.resolve(a[0])
	if ski == "" {
		return map[string]any{name: msg}
	}
	uc := "LPC"
	scen := t.h.uclpc.RemoteEntitiesScenarios()
	if production {
		uc, scen = "LPP", t.h.uclpp.RemoteEntitiesScenarios()
	}
	ents := entitiesFor(scen, ski)
	if len(ents) == 0 {
		return map[string]any{name: map[string]any{"Started": false, "Ip": t.ipOf(ski), "Result": "Geraet unterstuetzt " + uc + " nicht"}}
	}
	limit := ucapi.LoadLimit{IsActive: activate, Value: watt}
	if !activate {
		// release: keep the last value, drop any duration
		if production {
			if l, err := t.h.uclpp.ProductionLimit(ents[0]); err == nil {
				limit.Value = math.Abs(l.Value)
			}
		} else if l, err := t.h.uclpc.ConsumptionLimit(ents[0]); err == nil {
			limit.Value = l.Value
		}
		if len(a) >= 2 {
			limit.Value = watt
		}
		limit.DeleteDuration = true
	} else if dur > 0 {
		limit.Duration = dur
	} else {
		limit.DeleteDuration = true
	}
	if production {
		limit.Value = -math.Abs(limit.Value) // feed-in limits go negative on the wire, as on the Tasmota
	}

	t.mu.Lock()
	p := t.peer(ski)
	p.lpcSeq++
	seq := p.lpcSeq
	p.lpcState, p.lpcResult = "wait", "gesendet"
	t.mu.Unlock()

	shown := int64(math.Abs(limit.Value))
	resultCB := func(msg model.ResultDataType, _ model.MsgCounterType) {
		t.mu.Lock()
		defer t.mu.Unlock()
		p := t.peer(ski)
		if p.lpcSeq != seq {
			return
		}
		if msg.ErrorNumber == nil || *msg.ErrorNumber == model.ErrorNumberTypeNoError {
			p.lpcState = "done"
			if activate {
				p.lpcResult = fmt.Sprintf("BESTAETIGT aktiv %d W", shown)
			} else {
				p.lpcResult = "BESTAETIGT freigegeben"
			}
		} else {
			p.lpcState = "fail"
			p.lpcResult = fmt.Sprintf("abgelehnt (Fehler %d: %s)", *msg.ErrorNumber, resultDescription(msg))
		}
	}
	var err error
	if production {
		_, err = t.h.uclpp.WriteProductionLimit(ents[0], limit, resultCB)
	} else {
		_, err = t.h.uclpc.WriteConsumptionLimit(ents[0], limit, resultCB)
	}
	t.mu.Lock()
	if err != nil {
		p.lpcState, p.lpcResult = "fail", "nicht gesendet: "+err.Error()
	}
	state, result := p.lpcState, p.lpcResult
	t.mu.Unlock()
	if err == nil {
		go func() {
			time.Sleep(15 * time.Second)
			t.mu.Lock()
			if p := t.peer(ski); p.lpcSeq == seq && p.lpcState == "wait" {
				p.lpcState, p.lpcResult = "fail", "Timeout (keine Antwort)"
			}
			t.mu.Unlock()
		}()
	}
	log.Printf("%s an %s: aktiv=%v %v W Dauer %v -> %s", name, ski, limit.IsActive, limit.Value, limit.Duration, result)
	res := map[string]any{"Started": err == nil, "Ip": t.ipOf(ski), "Watt": int64(watt), "Lpc": state, "Result": result}
	if activate {
		res["DauerS"] = int64(dur / time.Second)
	}
	if production {
		res["AufDraht"] = int64(limit.Value)
	}
	return map[string]any{name: res}
}

// ---- failsafe ----

func (t *tasmotaAPI) cmdFailsafe(arg string) any {
	a := strings.Fields(arg)
	if len(a) < 3 {
		return map[string]any{"EEBusFailsafe": "Nutzung: EEBusFailsafe <ski|ip> bezug|einspeisung|dauer <wert>  (W bzw. Sekunden)"}
	}
	val, err := strconv.ParseFloat(a[2], 64)
	if err != nil {
		return map[string]any{"EEBusFailsafe": "Wert ungueltig"}
	}
	val = math.Abs(val)
	art := strings.ToLower(a[1])
	switch art {
	case "lpc":
		art = "bezug"
	case "lpp":
		art = "einspeisung"
	case "duration":
		art = "dauer"
	}
	if art != "bezug" && art != "einspeisung" && art != "dauer" {
		return map[string]any{"EEBusFailsafe": "Art: bezug, einspeisung oder dauer"}
	}
	if art == "dauer" && (val < 1 || val > 86400) {
		return map[string]any{"EEBusFailsafe": "Dauer 1..86400 s (Norm: 7200..86400)"}
	}
	ski, msg := t.resolve(a[0])
	if ski == "" {
		return map[string]any{"EEBusFailsafe": msg}
	}
	lpcEnts := entitiesFor(t.h.uclpc.RemoteEntitiesScenarios(), ski)
	lppEnts := entitiesFor(t.h.uclpp.RemoteEntitiesScenarios(), ski)
	var werr error
	sent := false
	var check func() bool
	switch art {
	case "bezug":
		if len(lpcEnts) > 0 {
			_, werr = t.h.uclpc.WriteFailsafeConsumptionActivePowerLimit(lpcEnts[0], val)
			sent = true
			check = func() bool {
				v, e := t.h.uclpc.FailsafeConsumptionActivePowerLimit(lpcEnts[0])
				return e == nil && math.Abs(v-val) < 0.5
			}
		}
	case "einspeisung":
		if len(lppEnts) > 0 {
			_, werr = t.h.uclpp.WriteFailsafeProductionActivePowerLimit(lppEnts[0], val)
			sent = true
			check = func() bool {
				v, e := t.h.uclpp.FailsafeProductionActivePowerLimit(lppEnts[0])
				return e == nil && math.Abs(math.Abs(v)-val) < 0.5
			}
		}
	case "dauer":
		d := time.Duration(val) * time.Second
		if len(lpcEnts) > 0 {
			_, werr = t.h.uclpc.WriteFailsafeDurationMinimum(lpcEnts[0], d)
			sent = true
			check = func() bool { v, e := t.h.uclpc.FailsafeDurationMinimum(lpcEnts[0]); return e == nil && v == d }
		} else if len(lppEnts) > 0 {
			_, werr = t.h.uclpp.WriteFailsafeDurationMinimum(lppEnts[0], d)
			sent = true
			check = func() bool { v, e := t.h.uclpp.FailsafeDurationMinimum(lppEnts[0]); return e == nil && v == d }
		}
	}
	unit := "W"
	if art == "dauer" {
		unit = "s"
	}
	t.mu.Lock()
	p := t.peer(ski)
	p.fsSeq++
	seq := p.fsSeq
	switch {
	case !sent:
		p.fsResult = "Geraet unterstuetzt das nicht"
	case werr != nil:
		p.fsResult = "nicht gesendet: " + werr.Error()
	default:
		p.fsResult = "gesendet"
	}
	t.mu.Unlock()
	if sent && werr == nil {
		go func() {
			ok := false
			for i := 0; i < 8 && !ok; i++ {
				time.Sleep(time.Second)
				ok = check()
			}
			t.mu.Lock()
			if p := t.peer(ski); p.fsSeq == seq {
				if ok {
					p.fsResult = fmt.Sprintf("BESTAETIGT %d %s", int64(val), unit)
				} else {
					p.fsResult = "nicht bestaetigt (Wert kam nicht zurueck)"
				}
			}
			t.mu.Unlock()
		}()
	}
	return map[string]any{"EEBusFailsafe": map[string]any{"Ip": t.ipOf(ski), "Art": art, "Wert": int64(val), "Einheit": unit,
		"Zustand": "vorgemerkt - Ergebnis in EEBusStatus (FsResult)"}}
}

// ---- connect / disconnect ----

func (t *tasmotaAPI) findMdns(sel string) *mdnsPeer {
	sel = strings.ToLower(strings.TrimSpace(sel))
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range t.mdns {
		if m.Ski == sel || (sel != "" && strings.HasPrefix(m.Ski, sel)) || m.Ip == sel {
			c := *m
			return &c
		}
	}
	return nil
}

func (t *tasmotaAPI) cmdConnect(arg string) any {
	m := t.findMdns(arg)
	if m == nil {
		return map[string]any{"EEBusConnect": "Geraet nicht in der Fundliste"}
	}
	t.h.myService.RegisterRemoteService(shipapi.NewServiceIdentity(m.Ski, "", m.Id))
	rememberPeer(m.Ski, m.Id, true)
	return map[string]any{"EEBusConnect": map[string]any{"Ski": m.Ski, "Ip": m.Ip, "Port": m.Port, "Zustand": "Verbindungsaufbau"}}
}

func (t *tasmotaAPI) cmdDisconnect(arg string) any {
	ski := ""
	if m := t.findMdns(arg); m != nil {
		ski = m.Ski
	} else if s, _ := t.resolve(arg); s != "" {
		ski = s
	}
	if ski == "" {
		return map[string]any{"EEBusDisconnect": "Geraet nicht gefunden"}
	}
	identity := t.identityOf(ski)
	t.h.myService.DisconnectService(identity, "user request")
	t.h.myService.UnregisterRemoteService(identity)
	rememberPeer(ski, "", false)
	t.mu.Lock()
	t.peer(ski).err = "getrennt (von Hand)"
	t.mu.Unlock()
	return map[string]any{"EEBusDisconnect": map[string]any{"Ski": ski, "Zustand": "getrennt"}}
}

// ---- devices to connect to at start (CONNECT_PEERS="ski[:shipid],...") ----

var peerFileMu sync.Mutex

func connectPeers() []shipapi.ServiceIdentity {
	out := []shipapi.ServiceIdentity{}
	for _, e := range strings.Split(os.Getenv("CONNECT_PEERS"), ",") {
		ski, id, _ := strings.Cut(strings.TrimSpace(e), ":")
		if ski != "" {
			out = append(out, shipapi.NewServiceIdentity(strings.ToLower(ski), "", id))
		}
	}
	return out
}

// rememberPeer keeps EEBusConnect / EEBusDisconnect across restarts.
func rememberPeer(ski, id string, add bool) {
	peerFileMu.Lock()
	defer peerFileMu.Unlock()
	env, err := godotenv.Read(".env")
	if err != nil {
		log.Printf("CONNECT_PEERS nicht gespeichert: %v", err)
		return
	}
	list := []string{}
	for _, e := range strings.Split(env["CONNECT_PEERS"], ",") {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		s, _, _ := strings.Cut(e, ":")
		if !strings.EqualFold(s, ski) {
			list = append(list, e)
		}
	}
	if add {
		e := strings.ToLower(ski)
		if id != "" {
			e += ":" + id
		}
		list = append(list, e)
	}
	if err := updateEnv(map[string]string{"CONNECT_PEERS": strings.Join(list, ",")}); err != nil {
		log.Printf("CONNECT_PEERS nicht gespeichert: %v", err)
	}
	_ = os.Setenv("CONNECT_PEERS", strings.Join(list, ","))
}

// ---- start ----

func startTasmotaUI(h *controlbox, port int) {
	mux := http.NewServeMux()
	tapi.routes(mux)
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) { serveWs(h, w, r) })
	go tapi.browse()
	go tapi.watchdog()
	addr := net.JoinHostPort("", strconv.Itoa(port))
	log.Printf("Steuerbox-Oberflaeche: http://<host>:%d/steuerbox", port)
	log.Fatal(http.ListenAndServe(addr, mux))
}
