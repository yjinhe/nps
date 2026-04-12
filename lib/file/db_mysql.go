package file

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"ehang.io/nps/lib/common"
	"ehang.io/nps/lib/crypt"
	"ehang.io/nps/lib/rate"

	_ "github.com/go-sql-driver/mysql"
	"github.com/astaxie/beego/logs"
)

type MysqlDb struct {
	db *sql.DB
	JsonDb *JsonDb
	mu sync.RWMutex
}

func InitMysql(dsn string) (*MysqlDb, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql connect error: %v", err)
	}
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("mysql ping error: %v", err)
	}

	m := &MysqlDb{
		db: db,
		JsonDb: NewJsonDb(common.GetRunPath()),
	}

	if err := m.createTables(); err != nil {
		return nil, fmt.Errorf("mysql create tables error: %v", err)
	}

	m.loadFromMysql()

	jsonDb := GetDb().JsonDb
	jsonDb.Clients = m.JsonDb.Clients
	jsonDb.Tasks = m.JsonDb.Tasks
	jsonDb.Hosts = m.JsonDb.Hosts
	jsonDb.Global = m.JsonDb.Global
	jsonDb.ClientIncreaseId = m.JsonDb.ClientIncreaseId
	jsonDb.TaskIncreaseId = m.JsonDb.TaskIncreaseId
	jsonDb.HostIncreaseId = m.JsonDb.HostIncreaseId

	return m, nil
}

func (m *MysqlDb) createTables() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS nps_client (
			id INT PRIMARY KEY,
			verify_key VARCHAR(255) NOT NULL DEFAULT '',
			addr VARCHAR(255) NOT NULL DEFAULT '',
			remark VARCHAR(255) NOT NULL DEFAULT '',
			status TINYINT NOT NULL DEFAULT 1,
			is_connect TINYINT NOT NULL DEFAULT 0,
			rate_limit INT NOT NULL DEFAULT 0,
			export_flow BIGINT NOT NULL DEFAULT 0,
			inlet_flow BIGINT NOT NULL DEFAULT 0,
			flow_limit BIGINT NOT NULL DEFAULT 0,
			max_conn INT NOT NULL DEFAULT 0,
			now_conn INT NOT NULL DEFAULT 0,
			web_username VARCHAR(255) NOT NULL DEFAULT '',
			web_password VARCHAR(255) NOT NULL DEFAULT '',
			config_conn_allow TINYINT NOT NULL DEFAULT 0,
			max_tunnel_num INT NOT NULL DEFAULT 0,
			version VARCHAR(64) NOT NULL DEFAULT '',
			black_ip_list TEXT,
			create_time VARCHAR(64) NOT NULL DEFAULT '',
			last_online_time VARCHAR(64) NOT NULL DEFAULT '',
			ip_white TINYINT NOT NULL DEFAULT 0,
			ip_white_pass VARCHAR(255) NOT NULL DEFAULT '',
			ip_white_list TEXT,
			cnf_u VARCHAR(255) NOT NULL DEFAULT '',
			cnf_p VARCHAR(255) NOT NULL DEFAULT '',
			cnf_compress TINYINT NOT NULL DEFAULT 0,
			cnf_crypt TINYINT NOT NULL DEFAULT 0,
			no_display TINYINT NOT NULL DEFAULT 0,
			INDEX idx_verify_key (verify_key),
			INDEX idx_web_username (web_username)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

		`CREATE TABLE IF NOT EXISTS nps_tunnel (
			id INT PRIMARY KEY,
			port INT NOT NULL DEFAULT 0,
			server_ip VARCHAR(64) NOT NULL DEFAULT '',
			mode VARCHAR(32) NOT NULL DEFAULT '',
			status TINYINT NOT NULL DEFAULT 1,
			run_status TINYINT NOT NULL DEFAULT 0,
			client_id INT NOT NULL DEFAULT 0,
			ports VARCHAR(255) NOT NULL DEFAULT '',
			export_flow BIGINT NOT NULL DEFAULT 0,
			inlet_flow BIGINT NOT NULL DEFAULT 0,
			flow_limit BIGINT NOT NULL DEFAULT 0,
			password VARCHAR(255) NOT NULL DEFAULT '',
			remark VARCHAR(255) NOT NULL DEFAULT '',
			target_addr VARCHAR(1024) NOT NULL DEFAULT '',
			local_path VARCHAR(512) NOT NULL DEFAULT '',
			strip_pre VARCHAR(255) NOT NULL DEFAULT '',
			proto_version VARCHAR(32) NOT NULL DEFAULT '',
			target TEXT,
			multi_account TEXT,
			health_check_timeout INT NOT NULL DEFAULT 0,
			health_max_fail INT NOT NULL DEFAULT 0,
			health_check_interval INT NOT NULL DEFAULT 0,
			http_health_url VARCHAR(512) NOT NULL DEFAULT '',
			health_check_type VARCHAR(32) NOT NULL DEFAULT '',
			health_check_target VARCHAR(512) NOT NULL DEFAULT '',
			INDEX idx_client_id (client_id),
			INDEX idx_mode (mode),
			INDEX idx_port (port)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

		`CREATE TABLE IF NOT EXISTS nps_host (
			id INT PRIMARY KEY,
			host VARCHAR(255) NOT NULL DEFAULT '',
			header_change TEXT,
			host_change VARCHAR(255) NOT NULL DEFAULT '',
			location VARCHAR(255) NOT NULL DEFAULT '/',
			remark VARCHAR(255) NOT NULL DEFAULT '',
			scheme VARCHAR(16) NOT NULL DEFAULT '',
			cert_file_path TEXT,
			key_file_path TEXT,
			is_close TINYINT NOT NULL DEFAULT 0,
			auto_https TINYINT NOT NULL DEFAULT 0,
			export_flow BIGINT NOT NULL DEFAULT 0,
			inlet_flow BIGINT NOT NULL DEFAULT 0,
			flow_limit BIGINT NOT NULL DEFAULT 0,
			client_id INT NOT NULL DEFAULT 0,
			target TEXT,
			local_proxy TINYINT NOT NULL DEFAULT 0,
			INDEX idx_host (host),
			INDEX idx_client_id (client_id),
			INDEX idx_scheme (scheme)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

		`CREATE TABLE IF NOT EXISTS nps_global (
			id INT PRIMARY KEY DEFAULT 1,
			black_ip_list TEXT,
			server_url VARCHAR(512) NOT NULL DEFAULT ''
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	}

	for _, stmt := range stmts {
		if _, err := m.db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (m *MysqlDb) loadFromMysql() {
	m.loadClientsFromMysql()
	m.loadTunnelsFromMysql()
	m.loadHostsFromMysql()
	m.loadGlobalFromMysql()
}

func (m *MysqlDb) loadClientsFromMysql() {
	rows, err := m.db.Query(`SELECT id, verify_key, addr, remark, status, is_connect, rate_limit,
		export_flow, inlet_flow, flow_limit, max_conn, now_conn, web_username, web_password,
		config_conn_allow, max_tunnel_num, version, black_ip_list, create_time, last_online_time,
		ip_white, ip_white_pass, ip_white_list, cnf_u, cnf_p, cnf_compress, cnf_crypt, no_display
		FROM nps_client ORDER BY id`)
	if err != nil {
		logs.Error("load clients from mysql error:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		c := &Client{Cnf: new(Config), Flow: new(Flow)}
		var status, isConnect, configConnAllow, cnfCompress, cnfCrypt, noDisplay, ipWhite int
		var blackIpList, ipWhiteList sql.NullString
		var version, lastOnlineTime sql.NullString

		err := rows.Scan(&c.Id, &c.VerifyKey, &c.Addr, &c.Remark, &status, &isConnect, &c.RateLimit,
			&c.Flow.ExportFlow, &c.Flow.InletFlow, &c.Flow.FlowLimit, &c.MaxConn, &c.NowConn,
			&c.WebUserName, &c.WebPassword, &configConnAllow, &c.MaxTunnelNum, &version,
			&blackIpList, &c.CreateTime, &lastOnlineTime, &ipWhite, &c.IpWhitePass,
			&ipWhiteList, &c.Cnf.U, &c.Cnf.P, &cnfCompress, &cnfCrypt, &noDisplay)
		if err != nil {
			logs.Error("scan client error:", err)
			continue
		}

		c.Status = status == 1
		c.IsConnect = isConnect == 1
		c.ConfigConnAllow = configConnAllow == 1
		c.Cnf.Compress = cnfCompress == 1
		c.Cnf.Crypt = cnfCrypt == 1
		c.NoDisplay = noDisplay == 1
		c.IpWhite = ipWhite == 1
		if version.Valid {
			c.Version = version.String
		}
		if lastOnlineTime.Valid {
			c.LastOnlineTime = lastOnlineTime.String
		}
		if blackIpList.Valid && blackIpList.String != "" {
			c.BlackIpList = strings.Split(blackIpList.String, ",")
		}
		if ipWhiteList.Valid && ipWhiteList.String != "" {
			c.IpWhiteList = strings.Split(ipWhiteList.String, ",")
		}

		if c.RateLimit > 0 {
			c.Rate = rate.NewRate(int64(c.RateLimit * 1024))
		} else {
			c.Rate = rate.NewRate((2 << 23) * 1024)
		}
		c.Rate.Start()

		m.JsonDb.Clients.Store(c.Id, c)
		if c.Id > int(m.JsonDb.ClientIncreaseId) {
			m.JsonDb.ClientIncreaseId = int32(c.Id)
		}
	}
}

func (m *MysqlDb) loadTunnelsFromMysql() {
	rows, err := m.db.Query(`SELECT id, port, server_ip, mode, status, run_status, client_id,
		ports, export_flow, inlet_flow, flow_limit, password, remark, target_addr,
		local_path, strip_pre, proto_version, target, multi_account,
		health_check_timeout, health_max_fail, health_check_interval,
		http_health_url, health_check_type, health_check_target
		FROM nps_tunnel ORDER BY id`)
	if err != nil {
		logs.Error("load tunnels from mysql error:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		t := &Tunnel{Flow: new(Flow), Target: new(Target)}
		var status, runStatus int
		var target, multiAccount sql.NullString
		var clientID int

		err := rows.Scan(&t.Id, &t.Port, &t.ServerIp, &t.Mode, &status, &runStatus, &clientID,
			&t.Ports, &t.Flow.ExportFlow, &t.Flow.InletFlow, &t.Flow.FlowLimit,
			&t.Password, &t.Remark, &t.TargetAddr, &t.LocalPath, &t.StripPre,
			&t.ProtoVersion, &target, &multiAccount,
			&t.HealthCheckTimeout, &t.HealthMaxFail, &t.HealthCheckInterval,
			&t.HttpHealthUrl, &t.HealthCheckType, &t.HealthCheckTarget)
		if err != nil {
			logs.Error("scan tunnel error:", err)
			continue
		}

		t.Status = status == 1
		t.RunStatus = runStatus == 1

		if v, ok := m.JsonDb.Clients.Load(clientID); ok {
			t.Client = v.(*Client)
		}

		if target.Valid && target.String != "" {
			var tgt Target
			if err := json.Unmarshal([]byte(target.String), &tgt); err == nil {
				t.Target = &tgt
			} else {
				t.Target = &Target{TargetStr: target.String}
			}
		}
		if t.Target == nil {
			t.Target = &Target{TargetStr: t.TargetAddr}
		}

		if multiAccount.Valid && multiAccount.String != "" {
			var ma MultiAccount
			if err := json.Unmarshal([]byte(multiAccount.String), &ma); err == nil {
				t.MultiAccount = &ma
			}
		}

		m.JsonDb.Tasks.Store(t.Id, t)
		if t.Id > int(m.JsonDb.TaskIncreaseId) {
			m.JsonDb.TaskIncreaseId = int32(t.Id)
		}
	}
}

func (m *MysqlDb) loadHostsFromMysql() {
	rows, err := m.db.Query(`SELECT id, host, header_change, host_change, location, remark,
		scheme, cert_file_path, key_file_path, is_close, auto_https,
		export_flow, inlet_flow, flow_limit, client_id, target, local_proxy
		FROM nps_host ORDER BY id`)
	if err != nil {
		logs.Error("load hosts from mysql error:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		h := &Host{Flow: new(Flow), Target: new(Target)}
		var isClose, autoHttps, localProxy int
		var headerChange, certFilePath, keyFilePath, target sql.NullString
		var clientID int

		err := rows.Scan(&h.Id, &h.Host, &headerChange, &h.HostChange, &h.Location, &h.Remark,
			&h.Scheme, &certFilePath, &keyFilePath, &isClose, &autoHttps,
			&h.Flow.ExportFlow, &h.Flow.InletFlow, &h.Flow.FlowLimit, &clientID, &target, &localProxy)
		if err != nil {
			logs.Error("scan host error:", err)
			continue
		}

		h.IsClose = isClose == 1
		h.AutoHttps = autoHttps == 1
		if headerChange.Valid {
			h.HeaderChange = headerChange.String
		}
		if certFilePath.Valid {
			h.CertFilePath = certFilePath.String
		}
		if keyFilePath.Valid {
			h.KeyFilePath = keyFilePath.String
		}

		if v, ok := m.JsonDb.Clients.Load(clientID); ok {
			h.Client = v.(*Client)
		}

		if target.Valid && target.String != "" {
			var tgt Target
			if err := json.Unmarshal([]byte(target.String), &tgt); err == nil {
				h.Target = &tgt
			} else {
				h.Target = &Target{TargetStr: target.String}
			}
		}
		if h.Target == nil {
			h.Target = &Target{}
		}
		h.Target.LocalProxy = localProxy == 1

		m.JsonDb.Hosts.Store(h.Id, h)
		if h.Id > int(m.JsonDb.HostIncreaseId) {
			m.JsonDb.HostIncreaseId = int32(h.Id)
		}
	}
}

func (m *MysqlDb) loadGlobalFromMysql() {
	row := m.db.QueryRow(`SELECT black_ip_list, server_url FROM nps_global WHERE id = 1`)
	g := &Glob{}
	var blackIpList sql.NullString
	if err := row.Scan(&blackIpList, &g.ServerUrl); err != nil {
		if err != sql.ErrNoRows {
			logs.Error("load global from mysql error:", err)
		}
		return
	}
	if blackIpList.Valid && blackIpList.String != "" {
		g.BlackIpList = strings.Split(blackIpList.String, ",")
	}
	m.JsonDb.Global = g
}

// --- Client CRUD ---

func (m *MysqlDb) NewClient(c *Client) error {
	if c.WebUserName != "" && !m.VerifyUserName(c.WebUserName, c.Id) {
		return fmt.Errorf("web login username duplicate, please reset")
	}
reset:
	if c.VerifyKey == "" {
		c.VerifyKey = crypt.GetVkey()
	}
	if !m.VerifyVkey(c.VerifyKey, c.Id) {
		c.VerifyKey = crypt.GetVkey()
		goto reset
	}
	if c.Id == 0 {
		c.Id = int(m.JsonDb.GetClientId())
	}
	if c.Flow == nil {
		c.Flow = new(Flow)
	}
	if c.RateLimit == 0 {
		c.Rate = rate.NewRate((2 << 23) * 1024)
	} else if c.Rate == nil {
		c.Rate = rate.NewRate(int64(c.RateLimit * 1024))
	}
	c.Rate.Start()

	_, err := m.db.Exec(`INSERT INTO nps_client (id, verify_key, remark, status, rate_limit,
		flow_limit, max_conn, web_username, web_password, config_conn_allow, max_tunnel_num,
		cnf_u, cnf_p, cnf_compress, cnf_crypt, no_display, create_time, black_ip_list,
		ip_white, ip_white_pass, ip_white_list, export_flow, inlet_flow)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.Id, c.VerifyKey, c.Remark, boolToInt(c.Status), c.RateLimit,
		c.Flow.FlowLimit, c.MaxConn, c.WebUserName, c.WebPassword,
		boolToInt(c.ConfigConnAllow), c.MaxTunnelNum,
		c.Cnf.U, c.Cnf.P, boolToInt(c.Cnf.Compress), boolToInt(c.Cnf.Crypt),
		boolToInt(c.NoDisplay), c.CreateTime, strings.Join(c.BlackIpList, ","),
		boolToInt(c.IpWhite), c.IpWhitePass, strings.Join(c.IpWhiteList, ","),
		c.Flow.ExportFlow, c.Flow.InletFlow)
	if err != nil {
		return err
	}

	m.JsonDb.Clients.Store(c.Id, c)
	return nil
}

func (m *MysqlDb) UpdateClient(c *Client) error {
	_, err := m.db.Exec(`UPDATE nps_client SET verify_key=?, addr=?, remark=?, status=?,
		rate_limit=?, flow_limit=?, max_conn=?, web_username=?, web_password=?,
		config_conn_allow=?, max_tunnel_num=?, version=?, black_ip_list=?,
		create_time=?, last_online_time=?, ip_white=?, ip_white_pass=?, ip_white_list=?,
		cnf_u=?, cnf_p=?, cnf_compress=?, cnf_crypt=?, no_display=?,
		export_flow=?, inlet_flow=? WHERE id=?`,
		c.VerifyKey, c.Addr, c.Remark, boolToInt(c.Status),
		c.RateLimit, c.Flow.FlowLimit, c.MaxConn, c.WebUserName, c.WebPassword,
		boolToInt(c.ConfigConnAllow), c.MaxTunnelNum, c.Version, strings.Join(c.BlackIpList, ","),
		c.CreateTime, c.LastOnlineTime, boolToInt(c.IpWhite), c.IpWhitePass,
		strings.Join(c.IpWhiteList, ","),
		c.Cnf.U, c.Cnf.P, boolToInt(c.Cnf.Compress), boolToInt(c.Cnf.Crypt),
		boolToInt(c.NoDisplay),
		c.Flow.ExportFlow, c.Flow.InletFlow, c.Id)
	if err != nil {
		return err
	}
	m.JsonDb.Clients.Store(c.Id, c)
	return nil
}

func (m *MysqlDb) DelClient(id int) error {
	_, err := m.db.Exec(`DELETE FROM nps_client WHERE id=?`, id)
	if err != nil {
		return err
	}
	m.JsonDb.Clients.Delete(id)
	return nil
}

func (m *MysqlDb) VerifyVkey(vkey string, id int) bool {
	res := true
	m.JsonDb.Clients.Range(func(key, value interface{}) bool {
		v := value.(*Client)
		if v.VerifyKey == vkey && v.Id != id {
			res = false
			return false
		}
		return true
	})
	return res
}

func (m *MysqlDb) VerifyUserName(username string, id int) bool {
	res := true
	m.JsonDb.Clients.Range(func(key, value interface{}) bool {
		v := value.(*Client)
		if v.WebUserName == username && v.Id != id {
			res = false
			return false
		}
		return true
	})
	return res
}

// --- Tunnel CRUD ---

func (m *MysqlDb) NewTask(t *Tunnel) error {
	m.JsonDb.Tasks.Range(func(key, value interface{}) bool {
		v := value.(*Tunnel)
		if (v.Mode == "secret" || v.Mode == "p2p") && v.Password == t.Password && t.Password != "" {
			return false
		}
		return true
	})

	t.Flow = new(Flow)
	targetJSON, _ := json.Marshal(t.Target)
	var multiAccountJSON []byte
	if t.MultiAccount != nil {
		multiAccountJSON, _ = json.Marshal(t.MultiAccount)
	}

	_, err := m.db.Exec(`INSERT INTO nps_tunnel (id, port, server_ip, mode, status, client_id,
		ports, flow_limit, password, remark, target_addr, local_path, strip_pre, proto_version,
		target, multi_account, health_check_timeout, health_max_fail, health_check_interval,
		http_health_url, health_check_type, health_check_target, export_flow, inlet_flow)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.Id, t.Port, t.ServerIp, t.Mode, boolToInt(t.Status), t.Client.Id,
		t.Ports, t.Flow.FlowLimit, t.Password, t.Remark, t.TargetAddr,
		t.LocalPath, t.StripPre, t.ProtoVersion, string(targetJSON),
		string(multiAccountJSON),
		t.HealthCheckTimeout, t.HealthMaxFail, t.HealthCheckInterval,
		t.HttpHealthUrl, t.HealthCheckType, t.HealthCheckTarget,
		t.Flow.ExportFlow, t.Flow.InletFlow)
	if err != nil {
		return err
	}

	m.JsonDb.Tasks.Store(t.Id, t)
	return nil
}

func (m *MysqlDb) UpdateTask(t *Tunnel) error {
	targetJSON, _ := json.Marshal(t.Target)
	var multiAccountJSON []byte
	if t.MultiAccount != nil {
		multiAccountJSON, _ = json.Marshal(t.MultiAccount)
	}

	_, err := m.db.Exec(`UPDATE nps_tunnel SET port=?, server_ip=?, mode=?, status=?,
		run_status=?, client_id=?, ports=?, flow_limit=?, password=?, remark=?,
		target_addr=?, local_path=?, strip_pre=?, proto_version=?, target=?, multi_account=?,
		health_check_timeout=?, health_max_fail=?, health_check_interval=?,
		http_health_url=?, health_check_type=?, health_check_target=?,
		export_flow=?, inlet_flow=? WHERE id=?`,
		t.Port, t.ServerIp, t.Mode, boolToInt(t.Status),
		boolToInt(t.RunStatus), t.Client.Id, t.Ports, t.Flow.FlowLimit,
		t.Password, t.Remark, t.TargetAddr, t.LocalPath, t.StripPre,
		t.ProtoVersion, string(targetJSON), string(multiAccountJSON),
		t.HealthCheckTimeout, t.HealthMaxFail, t.HealthCheckInterval,
		t.HttpHealthUrl, t.HealthCheckType, t.HealthCheckTarget,
		t.Flow.ExportFlow, t.Flow.InletFlow, t.Id)
	if err != nil {
		return err
	}
	m.JsonDb.Tasks.Store(t.Id, t)
	return nil
}

func (m *MysqlDb) DelTask(id int) error {
	_, err := m.db.Exec(`DELETE FROM nps_tunnel WHERE id=?`, id)
	if err != nil {
		return err
	}
	m.JsonDb.Tasks.Delete(id)
	return nil
}

// --- Host CRUD ---

func (m *MysqlDb) NewHost(h *Host) error {
	if h.Location == "" {
		h.Location = "/"
	}
	if m.IsHostExist(h) {
		return fmt.Errorf("host has exist")
	}
	h.Flow = new(Flow)
	targetJSON, _ := json.Marshal(h.Target)

	_, err := m.db.Exec(`INSERT INTO nps_host (id, host, header_change, host_change, location,
		remark, scheme, cert_file_path, key_file_path, is_close, auto_https,
		flow_limit, client_id, target, local_proxy, export_flow, inlet_flow)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		h.Id, h.Host, h.HeaderChange, h.HostChange, h.Location,
		h.Remark, h.Scheme, h.CertFilePath, h.KeyFilePath,
		boolToInt(h.IsClose), boolToInt(h.AutoHttps),
		h.Flow.FlowLimit, h.Client.Id, string(targetJSON),
		boolToInt(h.Target.LocalProxy), h.Flow.ExportFlow, h.Flow.InletFlow)
	if err != nil {
		return err
	}

	m.JsonDb.Hosts.Store(h.Id, h)
	return nil
}

func (m *MysqlDb) UpdateHost(h *Host) error {
	targetJSON, _ := json.Marshal(h.Target)

	_, err := m.db.Exec(`UPDATE nps_host SET host=?, header_change=?, host_change=?,
		location=?, remark=?, scheme=?, cert_file_path=?, key_file_path=?,
		is_close=?, auto_https=?, flow_limit=?, client_id=?, target=?,
		local_proxy=?, export_flow=?, inlet_flow=? WHERE id=?`,
		h.Host, h.HeaderChange, h.HostChange, h.Location,
		h.Remark, h.Scheme, h.CertFilePath, h.KeyFilePath,
		boolToInt(h.IsClose), boolToInt(h.AutoHttps),
		h.Flow.FlowLimit, h.Client.Id, string(targetJSON),
		boolToInt(h.Target.LocalProxy), h.Flow.ExportFlow, h.Flow.InletFlow, h.Id)
	if err != nil {
		return err
	}
	m.JsonDb.Hosts.Store(h.Id, h)
	return nil
}

func (m *MysqlDb) DelHost(id int) error {
	_, err := m.db.Exec(`DELETE FROM nps_host WHERE id=?`, id)
	if err != nil {
		return err
	}
	m.JsonDb.Hosts.Delete(id)
	return nil
}

func (m *MysqlDb) IsHostExist(h *Host) bool {
	var exist bool
	m.JsonDb.Hosts.Range(func(key, value interface{}) bool {
		v := value.(*Host)
		if v.Id != h.Id && v.Host == h.Host && h.Location == v.Location && (v.Scheme == "all" || v.Scheme == h.Scheme) {
			exist = true
			return false
		}
		return true
	})
	return exist
}

// --- Global ---

func (m *MysqlDb) SaveGlobal(g *Glob) error {
	_, err := m.db.Exec(`INSERT INTO nps_global (id, black_ip_list, server_url)
		VALUES (1, ?, ?) ON DUPLICATE KEY UPDATE black_ip_list=?, server_url=?`,
		strings.Join(g.BlackIpList, ","), g.ServerUrl,
		strings.Join(g.BlackIpList, ","), g.ServerUrl)
	if err != nil {
		return err
	}
	m.JsonDb.Global = g
	return nil
}

// --- Flow sync (periodic) ---

func (m *MysqlDb) SyncFlowToMysql() {
	m.JsonDb.Clients.Range(func(key, value interface{}) bool {
		c := value.(*Client)
		if c.NoStore {
			return true
		}
		m.db.Exec(`UPDATE nps_client SET export_flow=?, inlet_flow=?, addr=?, is_connect=?,
			now_conn=?, version=?, last_online_time=? WHERE id=?`,
			c.Flow.ExportFlow, c.Flow.InletFlow, c.Addr,
			boolToInt(c.IsConnect), c.NowConn, c.Version, c.LastOnlineTime, c.Id)
		return true
	})

	m.JsonDb.Tasks.Range(func(key, value interface{}) bool {
		t := value.(*Tunnel)
		if t.NoStore {
			return true
		}
		m.db.Exec(`UPDATE nps_tunnel SET export_flow=?, inlet_flow=?, run_status=? WHERE id=?`,
			t.Flow.ExportFlow, t.Flow.InletFlow, boolToInt(t.RunStatus), t.Id)
		return true
	})

	m.JsonDb.Hosts.Range(func(key, value interface{}) bool {
		h := value.(*Host)
		if h.NoStore {
			return true
		}
		m.db.Exec(`UPDATE nps_host SET export_flow=?, inlet_flow=?, is_close=? WHERE id=?`,
			h.Flow.ExportFlow, h.Flow.InletFlow, boolToInt(h.IsClose), h.Id)
		return true
	})
}

func (m *MysqlDb) StartFlowSync(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			m.SyncFlowToMysql()
		}
	}()
}

// --- Migrate from JSON ---

func MigrateFromJson(dsn string, runPath string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("mysql connect error: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("mysql ping error: %v", err)
	}

	m := &MysqlDb{db: db, JsonDb: NewJsonDb(runPath)}
	m.JsonDb.LoadClientFromJsonFile()
	m.JsonDb.LoadTaskFromJsonFile()
	m.JsonDb.LoadHostFromJsonFile()
	m.JsonDb.LoadGlobalFromJsonFile()

	if err := m.createTables(); err != nil {
		return fmt.Errorf("create tables error: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	m.JsonDb.Clients.Range(func(key, value interface{}) bool {
		c := value.(*Client)
		if c.NoStore {
			return true
		}
		_, err := tx.Exec(`INSERT IGNORE INTO nps_client (id, verify_key, addr, remark, status,
			rate_limit, flow_limit, max_conn, now_conn, web_username, web_password,
			config_conn_allow, max_tunnel_num, version, black_ip_list, create_time,
			last_online_time, ip_white, ip_white_pass, ip_white_list, cnf_u, cnf_p,
			cnf_compress, cnf_crypt, no_display, export_flow, inlet_flow)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			c.Id, c.VerifyKey, c.Addr, c.Remark, boolToInt(c.Status),
			c.RateLimit, c.Flow.FlowLimit, c.MaxConn, c.NowConn,
			c.WebUserName, c.WebPassword, boolToInt(c.ConfigConnAllow),
			c.MaxTunnelNum, c.Version, strings.Join(c.BlackIpList, ","),
			c.CreateTime, c.LastOnlineTime, boolToInt(c.IpWhite), c.IpWhitePass,
			strings.Join(c.IpWhiteList, ","), c.Cnf.U, c.Cnf.P,
			boolToInt(c.Cnf.Compress), boolToInt(c.Cnf.Crypt), boolToInt(c.NoDisplay),
			c.Flow.ExportFlow, c.Flow.InletFlow)
		if err != nil {
			logs.Error("migrate client %d error: %v", c.Id, err)
		}
		return true
	})

	m.JsonDb.Tasks.Range(func(key, value interface{}) bool {
		t := value.(*Tunnel)
		if t.NoStore {
			return true
		}
		targetJSON, _ := json.Marshal(t.Target)
		var multiAccountJSON []byte
		if t.MultiAccount != nil {
			multiAccountJSON, _ = json.Marshal(t.MultiAccount)
		}
		clientID := 0
		if t.Client != nil {
			clientID = t.Client.Id
		}
		_, err := tx.Exec(`INSERT IGNORE INTO nps_tunnel (id, port, server_ip, mode, status,
			run_status, client_id, ports, flow_limit, password, remark, target_addr,
			local_path, strip_pre, proto_version, target, multi_account,
			health_check_timeout, health_max_fail, health_check_interval,
			http_health_url, health_check_type, health_check_target, export_flow, inlet_flow)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			t.Id, t.Port, t.ServerIp, t.Mode, boolToInt(t.Status),
			boolToInt(t.RunStatus), clientID, t.Ports, t.Flow.FlowLimit,
			t.Password, t.Remark, t.TargetAddr, t.LocalPath, t.StripPre,
			t.ProtoVersion, string(targetJSON), string(multiAccountJSON),
			t.HealthCheckTimeout, t.HealthMaxFail, t.HealthCheckInterval,
			t.HttpHealthUrl, t.HealthCheckType, t.HealthCheckTarget,
			t.Flow.ExportFlow, t.Flow.InletFlow)
		if err != nil {
			logs.Error("migrate tunnel %d error: %v", t.Id, err)
		}
		return true
	})

	m.JsonDb.Hosts.Range(func(key, value interface{}) bool {
		h := value.(*Host)
		if h.NoStore {
			return true
		}
		targetJSON, _ := json.Marshal(h.Target)
		clientID := 0
		if h.Client != nil {
			clientID = h.Client.Id
		}
		_, err := tx.Exec(`INSERT IGNORE INTO nps_host (id, host, header_change, host_change,
			location, remark, scheme, cert_file_path, key_file_path, is_close, auto_https,
			flow_limit, client_id, target, local_proxy, export_flow, inlet_flow)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			h.Id, h.Host, h.HeaderChange, h.HostChange, h.Location,
			h.Remark, h.Scheme, h.CertFilePath, h.KeyFilePath,
			boolToInt(h.IsClose), boolToInt(h.AutoHttps),
			h.Flow.FlowLimit, clientID, string(targetJSON),
			boolToInt(h.Target.LocalProxy), h.Flow.ExportFlow, h.Flow.InletFlow)
		if err != nil {
			logs.Error("migrate host %d error: %v", h.Id, err)
		}
		return true
	})

	if m.JsonDb.Global != nil {
		_, err = tx.Exec(`INSERT INTO nps_global (id, black_ip_list, server_url)
			VALUES (1, ?, ?) ON DUPLICATE KEY UPDATE black_ip_list=?, server_url=?`,
			strings.Join(m.JsonDb.Global.BlackIpList, ","), m.JsonDb.Global.ServerUrl,
			strings.Join(m.JsonDb.Global.BlackIpList, ","), m.JsonDb.Global.ServerUrl)
		if err != nil {
			logs.Error("migrate global error: %v", err)
		}
	}

	return tx.Commit()
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
