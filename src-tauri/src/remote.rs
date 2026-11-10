use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use futures_util::{SinkExt, StreamExt};
use leagueaccounts::utils::app_data_dir;
use qrcode::{render::svg, QrCode};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, RwLock};

const INDEX_HTML: &str = include_str!("../remote_ui/index.html");
const STYLES_CSS: &str = include_str!("../remote_ui/styles.css");
const SW_SLOTS_URL: &str = "/lol-lobby/v1/lobby/members/localMember/player-slots";
const SW_ROLES: [&str; 5] = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LcuCreds {
    pub port: u16,
    pub pw: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteStatusUI {
    pub running: bool,
    pub url: String,
    pub pin: String,
    pub qr_svg: String,
    pub connected_clients: usize,
    pub lcu_connected: bool,
    pub phase: String,
}

pub struct RemoteState {
    pub pin: RwLock<String>,
    pub port: u16,
    pub creds: RwLock<Option<LcuCreds>>,
    pub http_client: reqwest::Client,
    pub init_data: RwLock<Value>,
    pub last_state: RwLock<Value>,
    pub tx: broadcast::Sender<String>,
    pub clients_count: RwLock<usize>,
    pub last_role_change: RwLock<Option<(Instant, Value)>>,
    pub rec_cache: RwLock<HashMap<String, Value>>,
    pub summoner_id: RwLock<Option<u64>>,
}

pub fn load_or_create_pin() -> String {
    let pin_path = app_data_dir().ok().map(|d| d.join("remote.pin"));
    if let Some(ref p) = pin_path {
        if let Ok(saved) = std::fs::read_to_string(p) {
            let trimmed = saved.trim().to_string();
            if trimmed.len() == 6 && trimmed.chars().all(|c| c.is_ascii_digit()) {
                return trimmed;
            }
        }
    }
    let new_pin = format!("{:06}", rand::thread_rng().gen_range(100000..=999999));
    if let Some(ref p) = pin_path {
        let _ = std::fs::create_dir_all(p.parent().unwrap_or(p));
        let _ = std::fs::write(p, &new_pin);
    }
    new_pin
}

impl RemoteState {
    pub fn new(port: u16) -> Arc<Self> {
        let (tx, _) = broadcast::channel(64);
        let http_client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(6))
            .build()
            .unwrap();

        Arc::new(Self {
            pin: RwLock::new(load_or_create_pin()),
            port,
            creds: RwLock::new(None),
            http_client,
            init_data: RwLock::new(json!({
                "t": "init",
                "version": null,
                "champs": {},
                "queues": default_queues(),
                "runes": null,
                "spells": {}
            })),
            last_state: RwLock::new(json!({ "connected": false, "phase": "Offline" })),
            tx,
            clients_count: RwLock::new(0),
            last_role_change: RwLock::new(None),
            rec_cache: RwLock::new(HashMap::new()),
            summoner_id: RwLock::new(None),
        })
    }

    pub async fn lcu(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> (u16, Value) {
        let creds = self.creds.read().await.clone();
        let Some(c) = creds else { return (0, Value::Null) };
        let url = format!("https://127.0.0.1:{}{}", c.port, path);
        let auth = format!("Basic {}", BASE64.encode(format!("riot:{}", c.pw)));

        let mut req = self
            .http_client
            .request(method, &url)
            .header("Authorization", auth)
            .header("Accept", "application/json");

        if let Some(b) = body {
            req = req.json(&b);
        }

        match req.send().await {
            Ok(res) => {
                let status = res.status().as_u16();
                let data = res.json::<Value>().await.unwrap_or(Value::Null);
                (status, data)
            }
            Err(_) => (0, Value::Null),
        }
    }

    pub async fn get_summoner_id(&self) -> Option<u64> {
        if let Some(id) = *self.summoner_id.read().await {
            return Some(id);
        }
        let (_, data) = self.lcu(reqwest::Method::GET, "/lol-summoner/v1/current-summoner", None).await;
        let id = data.get("summonerId").and_then(|x| x.as_u64());
        if id.is_some() {
            *self.summoner_id.write().await = id;
        }
        id
    }
}

fn default_queues() -> Value {
    json!([
        { "id": 450, "name": "ARAM" },
        { "id": 2400, "name": "ARAM: Caos" },
        { "id": 400, "name": "Normal Draft" },
        { "id": 430, "name": "Normal a ciegas" },
        { "id": 420, "name": "Ranked Solo/Duo" },
        { "id": 440, "name": "Ranked Flex" },
        { "id": 480, "name": "Swiftplay" },
        { "id": 1700, "name": "Arena" }
    ])
}

fn read_credentials() -> Option<LcuCreds> {
    for drive in 'C'..='Z' {
        let path = PathBuf::from(format!("{}:/Riot Games/League of Legends/lockfile", drive));
        if let Ok(content) = std::fs::read_to_string(path) {
            let parts: Vec<&str> = content.trim().split(':').collect();
            if parts.len() >= 4 {
                if let Ok(port) = parts[2].parse::<u16>() {
                    return Some(LcuCreds {
                        port,
                        pw: parts[3].to_string(),
                    });
                }
            }
        }
    }
    // Fallback PowerShell sin abrir ventana de consola (CREATE_NO_WINDOW = 0x08000000)
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        if let Ok(out) = Command::new("powershell")
            .args([
                "-NoProfile",
                "-Command",
                "(Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'LeagueClientUx.exe' }).CommandLine",
            ])
            .creation_flags(0x08000000)
            .output()
        {
            let text = String::from_utf8_lossy(&out.stdout);
            let mut port = None;
            let mut pw = None;
            for part in text.split('"') {
                if let Some(p) = part.strip_prefix("--app-port=") {
                    port = p.trim().parse::<u16>().ok();
                } else if let Some(t) = part.strip_prefix("--remoting-auth-token=") {
                    pw = Some(t.trim().to_string());
                }
            }
            if let (Some(port), Some(pw)) = (port, pw) {
                return Some(LcuCreds { port, pw });
            }
        }
    }
    None
}

pub fn spawn_remote_server(state: Arc<RemoteState>) {
    let s_ddragon = state.clone();
    tauri::async_runtime::spawn(async move {
        load_ddragon(s_ddragon).await;
    });

    let s_poller = state.clone();
    tauri::async_runtime::spawn(async move {
        lcu_poll_loop(s_poller).await;
    });

    let s_http = state.clone();
    tauri::async_runtime::spawn(async move {
        let app = Router::new()
            .route("/", get(serve_index))
            .route("/index.html", get(serve_index))
            .route("/styles.css", get(serve_css))
            .with_state(s_http.clone());

        let addr = SocketAddr::from(([0, 0, 0, 0], s_http.port));
        if let Ok(listener) = tokio::net::TcpListener::bind(addr).await {
            let _ = axum::serve(listener, app).await;
        }
    });
}

async fn serve_index(
    ws: Option<WebSocketUpgrade>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<RemoteState>>,
) -> Response {
    if let Some(ws_upgrade) = ws {
        let pin = params.get("pin").map(|s| s.as_str()).unwrap_or("");
        let current_pin = state.pin.read().await.clone();
        if pin != current_pin {
            return (StatusCode::UNAUTHORIZED, "PIN inválido").into_response();
        }
        return ws_upgrade.on_upgrade(move |socket| handle_ws(socket, state));
    }
    Html(INDEX_HTML).into_response()
}

async fn serve_css() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], STYLES_CSS)
}

async fn handle_ws(socket: WebSocket, state: Arc<RemoteState>) {
    {
        *state.clients_count.write().await += 1;
    }
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.tx.subscribe();

    let init_msg = state.init_data.read().await.to_string();
    let mut curr_state = state.last_state.read().await.clone();
    if let Some(obj) = curr_state.as_object_mut() {
        obj.insert("t".into(), json!("state"));
    }
    let _ = sender.send(Message::Text(init_msg)).await;
    let _ = sender.send(Message::Text(curr_state.to_string())).await;

    let send_task = tauri::async_runtime::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    let state_cmd = state.clone();
    while let Some(Ok(Message::Text(text))) = receiver.next().await {
        if let Ok(cmd_msg) = serde_json::from_str::<Value>(&text) {
            if let Some(reply) = execute_command(&state_cmd, &cmd_msg).await {
                let _ = state_cmd.tx.send(reply.to_string());
            }
            refresh_state(&state_cmd).await;
        }
    }

    send_task.abort();
    {
        let mut count = state.clients_count.write().await;
        *count = count.saturating_sub(1);
    }
}

async fn load_ddragon(state: Arc<RemoteState>) {
    loop {
        if let Ok(res) = state.http_client.get("https://ddragon.leagueoflegends.com/api/versions.json").send().await {
            if let Ok(versions) = res.json::<Vec<String>>().await {
                if let Some(v) = versions.first() {
                    let champ_url = format!("https://ddragon.leagueoflegends.com/cdn/{}/data/es_AR/champion.json", v);
                    let spell_url = format!("https://ddragon.leagueoflegends.com/cdn/{}/data/es_AR/summoner.json", v);

                    if let (Ok(c_res), Ok(s_res)) = (
                        state.http_client.get(&champ_url).send().await,
                        state.http_client.get(&spell_url).send().await,
                    ) {
                        let c_json: Value = c_res.json().await.unwrap_or_default();
                        let s_json: Value = s_res.json().await.unwrap_or_default();

                        let mut champs = serde_json::Map::new();
                        if let Some(data) = c_json.get("data").and_then(|d| d.as_object()) {
                            for val in data.values() {
                                if let (Some(k), Some(id), Some(name)) = (
                                    val.get("key").and_then(|x| x.as_str()),
                                    val.get("id").and_then(|x| x.as_str()),
                                    val.get("name").and_then(|x| x.as_str()),
                                ) {
                                    champs.insert(k.to_string(), json!([id, name]));
                                }
                            }
                        }

                        let mut spells = serde_json::Map::new();
                        if let Some(data) = s_json.get("data").and_then(|d| d.as_object()) {
                            for val in data.values() {
                                if let (Some(k), Some(img), Some(name), Some(modes)) = (
                                    val.get("key").and_then(|x| x.as_str()),
                                    val.pointer("/image/full").and_then(|x| x.as_str()),
                                    val.get("name").and_then(|x| x.as_str()),
                                    val.get("modes"),
                                ) {
                                    spells.insert(k.to_string(), json!([img, name, modes]));
                                }
                            }
                        }

                        let mut init = state.init_data.write().await;
                        init["version"] = json!(v);
                        init["champs"] = Value::Object(champs);
                        init["spells"] = Value::Object(spells);
                        let _ = state.tx.send(init.to_string());
                        break;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn lcu_poll_loop(state: Arc<RemoteState>) {
    let mut last_creds: Option<LcuCreds> = None;
    loop {
        let creds = tokio::task::spawn_blocking(read_credentials).await.unwrap_or(None);
        if creds != last_creds {
            last_creds = creds.clone();
            *state.creds.write().await = creds.clone();
            *state.summoner_id.write().await = None;
            if creds.is_some() {
                load_runes_and_queues(&state).await;
            }
        }
        refresh_state(&state).await;
        tokio::time::sleep(Duration::from_millis(600)).await;
    }
}

async fn load_runes_and_queues(state: &Arc<RemoteState>) {
    let (_, q_data) = state.lcu(reqwest::Method::GET, "/lol-game-queues/v1/queues", None).await;
    if let Some(arr) = q_data.as_array() {
        let avail: HashSet<i64> = arr
            .iter()
            .filter(|q| q["queueAvailability"].as_str() == Some("Available"))
            .filter_map(|q| q["id"].as_i64())
            .collect();
        if !avail.is_empty() {
            let filtered: Vec<Value> = default_queues()
                .as_array()
                .unwrap()
                .iter()
                .filter(|q| avail.contains(&q["id"].as_i64().unwrap_or(0)))
                .cloned()
                .collect();
            if !filtered.is_empty() {
                state.init_data.write().await["queues"] = Value::Array(filtered);
            }
        }
    }

    let (_, st) = state.lcu(reqwest::Method::GET, "/lol-perks/v1/styles", None).await;
    let (_, pk) = state.lcu(reqwest::Method::GET, "/lol-perks/v1/perks", None).await;
    if let (Some(st_arr), Some(pk_arr)) = (st.as_array(), pk.as_array()) {
        let mut styles = Vec::new();
        for s in st_arr {
            let slots: Vec<Value> = s["slots"]
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .filter(|x| x["type"] != "kStatMod")
                .map(|x| x["perks"].clone())
                .collect();
            styles.push(json!({
                "id": s["id"],
                "name": s["name"],
                "icon": s["iconPath"],
                "slots": slots
            }));
        }
        let mut perks = serde_json::Map::new();
        for p in pk_arr {
            if let Some(id) = p["id"].as_u64() {
                perks.insert(id.to_string(), json!([p["name"], p["iconPath"]]));
            }
        }
        let mut init = state.init_data.write().await;
        init["runes"] = json!({
            "styles": styles,
            "shards": [[5008, 5005, 5007], [5008, 5010, 5001], [5011, 5013, 5001]],
            "perks": perks
        });
        let _ = state.tx.send(init.to_string());
    }
}

async fn get_slots(state: &Arc<RemoteState>, lobby: Option<&Value>) -> Vec<Value> {
    let (status, data) = state.lcu(reqwest::Method::GET, SW_SLOTS_URL, None).await;
    if status == 200 {
        if let Some(arr) = data.as_array() {
            return arr.clone();
        }
    }
    if let Some(l) = lobby {
        if let Some(arr) = l.pointer("/localMember/playerSlots").and_then(|x| x.as_array()) {
            return arr.clone();
        }
    }
    vec![]
}

fn slot_view(s: &Value) -> Value {
    let champ = s["championId"].as_i64().unwrap_or(0);
    let pos = s["positionPreference"].as_str().unwrap_or("").to_uppercase();
    let ok_pos = if SW_ROLES.contains(&pos.as_str()) { pos } else { String::new() };
    let skin = s["skinId"].as_i64().unwrap_or(champ * 1000);
    let spell1 = s.get("spell1").or(s.get("spell1Id")).cloned().unwrap_or(json!(0));
    let spell2 = s.get("spell2").or(s.get("spell2Id")).cloned().unwrap_or(json!(0));

    let perks_obj = match &s["perks"] {
        Value::String(str_val) => serde_json::from_str::<Value>(str_val).ok(),
        Value::Object(_) => Some(s["perks"].clone()),
        _ => None,
    };
    let perks = perks_obj.and_then(|o| {
        let ids = o.get("perkIds").or(o.get("selectedPerkIds")).or(o.get("perks"))?.as_array()?.clone();
        let primary = o.get("perkStyle").or(o.get("primaryStyleId")).and_then(|x| x.as_i64()).unwrap_or(0);
        let sub = o.get("perkSubStyle").or(o.get("subStyleId")).and_then(|x| x.as_i64()).unwrap_or(0);
        Some(json!({ "primary": primary, "sub": sub, "ids": ids }))
    });

    json!({
        "champ": champ,
        "pos": ok_pos,
        "recs": [],
        "diag": [],
        "skin": skin,
        "spell1": spell1,
        "spell2": spell2,
        "perks": perks
    })
}

pub async fn refresh_state(state: &Arc<RemoteState>) {
    let has_creds = state.creds.read().await.is_some();
    let mut st = json!({ "connected": has_creds, "phase": "Offline" });

    if has_creds {
        let (_, ph) = state.lcu(reqwest::Method::GET, "/lol-gameflow/v1/gameflow-phase", None).await;
        let phase = ph.as_str().unwrap_or("None").to_string();
        st["phase"] = json!(phase);

        if ["Lobby", "Matchmaking", "ReadyCheck"].contains(&phase.as_str()) {
            let (_, l) = state.lcu(reqwest::Method::GET, "/lol-lobby/v2/lobby", None).await;
            if l.is_object() && l.get("errorCode").is_none() {
                let queue_id = l.pointer("/gameConfig/queueId").and_then(|q| q.as_i64()).unwrap_or(0);
                let mut positions = Value::Null;
                if [400, 420, 440].contains(&queue_id) {
                    let cached = state.last_role_change.read().await.clone();
                    if let Some((inst, val)) = cached {
                        if inst.elapsed() < Duration::from_secs(2) {
                            positions = val;
                        }
                    }
                    if positions.is_null() {
                        let (_, pos_data) = state
                            .lcu(reqwest::Method::GET, "/lol-lobby/v2/lobby/members/localMember/position-preferences", None)
                            .await;
                        positions = json!({
                            "firstPreference": pos_data.get("firstPreference").and_then(|x| x.as_str()).unwrap_or("TOP"),
                            "secondPreference": pos_data.get("secondPreference").and_then(|x| x.as_str()).unwrap_or("MIDDLE"),
                            "priorities": ["BOTTOM", "UTILITY"]
                        });
                    }
                }
                let is_swiftplay = queue_id == 480;
                let slots = if is_swiftplay {
                    let raw = get_slots(state, Some(&l)).await;
                    Value::Array(raw.iter().map(slot_view).collect())
                } else {
                    Value::Null
                };

                let members: Vec<Value> = l["members"]
                    .as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .map(|m| {
                        json!({
                            "name": m.get("gameName").or(m.get("summonerName")).and_then(|x| x.as_str()).unwrap_or("?"),
                            "leader": m.get("isLeader").and_then(|x| x.as_bool()).unwrap_or(false)
                        })
                    })
                    .collect();

                st["lobby"] = json!({
                    "queueId": queue_id,
                    "members": members,
                    "canStart": l.pointer("/localMember/allowedStartActivity").and_then(|x| x.as_bool()).unwrap_or(false),
                    "positions": positions,
                    "isSwiftplay": is_swiftplay,
                    "slots": slots
                });
            }
        }

        if phase == "Matchmaking" {
            let (_, s) = state.lcu(reqwest::Method::GET, "/lol-matchmaking/v1/search", None).await;
            st["search"] = json!({
                "timeInQueue": s.get("timeInQueue").and_then(|x| x.as_f64()).unwrap_or(0.0),
                "estimated": s.get("estimatedQueueTime").and_then(|x| x.as_f64()).unwrap_or(0.0)
            });
        } else if phase == "ReadyCheck" {
            let (_, r) = state.lcu(reqwest::Method::GET, "/lol-matchmaking/v1/ready-check", None).await;
            st["ready"] = json!({ "response": r.get("playerResponse") });
        } else if phase == "ChampSelect" {
            let (code, s) = state.lcu(reqwest::Method::GET, "/lol-champ-select/v1/session", None).await;
            if code == 200 {
                let local_cell = s["localPlayerCellId"].as_i64().unwrap_or(-1);
                let mut my_action = None;
                if let Some(actions) = s["actions"].as_array() {
                    for group in actions {
                        if let Some(arr) = group.as_array() {
                            for a in arr {
                                if a["actorCellId"].as_i64() == Some(local_cell) && !a["completed"].as_bool().unwrap_or(false) {
                                    if a["isInProgress"].as_bool().unwrap_or(false) || my_action.is_none() {
                                        my_action = Some(json!({
                                            "id": a["id"],
                                            "type": a["type"],
                                            "champ": a["championId"],
                                            "active": a["isInProgress"]
                                        }));
                                    }
                                }
                            }
                        }
                    }
                }
                let pool = if let Some(ref act) = my_action {
                    let kind = if act["type"].as_str() == Some("ban") { "bannable" } else { "pickable" };
                    let (_, p) = state.lcu(reqwest::Method::GET, &format!("/lol-champ-select/v1/{}-champion-ids", kind), None).await;
                    p
                } else {
                    json!([])
                };

                let map_team = |team: &Value| -> Vec<Value> {
                    team.as_array()
                        .unwrap_or(&vec![])
                        .iter()
                        .map(|p| {
                            let c_id = p["championId"].as_i64().unwrap_or(0);
                            let intent = p["championPickIntent"].as_i64().unwrap_or(0);
                            json!({
                                "champ": if c_id > 0 { c_id } else { intent },
                                "me": p["cellId"].as_i64() == Some(local_cell)
                            })
                        })
                        .collect()
                };
                let me_obj = s["myTeam"]
                    .as_array()
                    .and_then(|t| t.iter().find(|p| p["cellId"].as_i64() == Some(local_cell)))
                    .map(|me| {
                        let c_id = me["championId"].as_i64().unwrap_or(0);
                        let intent = me["championPickIntent"].as_i64().unwrap_or(0);
                        json!({
                            "champ": if c_id > 0 { c_id } else { intent },
                            "spell1": me["spell1Id"],
                            "spell2": me["spell2Id"],
                            "pos": me["assignedPosition"].as_str().unwrap_or(""),
                            "skin": me["selectedSkinId"].as_i64().unwrap_or(0)
                        })
                    });

                let (_, pg) = state.lcu(reqwest::Method::GET, "/lol-perks/v1/pages", None).await;
                let pages: Vec<Value> = pg
                    .as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .filter(|p| p["isEditable"].as_bool().unwrap_or(false))
                    .map(|p| json!({ "id": p["id"], "name": p["name"], "current": p["current"].as_bool().unwrap_or(false) }))
                    .collect();

                st["cs"] = json!({
                    "phase": s.pointer("/timer/phase"),
                    "msLeft": s.pointer("/timer/adjustedTimeLeftInPhase").and_then(|x| x.as_i64()).unwrap_or(0),
                    "my": map_team(&s["myTeam"]),
                    "their": map_team(&s["theirTeam"]),
                    "bans": {
                        "my": s.pointer("/bans/myTeamBans").cloned().unwrap_or(json!([])),
                        "their": s.pointer("/bans/theirTeamBans").cloned().unwrap_or(json!([]))
                    },
                    "bench": s["benchChampions"].as_array().unwrap_or(&vec![]).iter().map(|b| b["championId"].clone()).collect::<Vec<_>>(),
                    "benchEnabled": s["benchEnabled"].as_bool().unwrap_or(false),
                    "rerolls": s["rerollsRemaining"].as_i64().unwrap_or(0),
                    "canReroll": s["allowRerolling"].as_bool().unwrap_or(false),
                    "action": my_action,
                    "pool": pool,
                    "me": me_obj,
                    "mapId": 11,
                    "gameMode": "CLASSIC",
                    "pages": pages,
                    "recs": []
                });
            }
        }
    }

    *state.last_state.write().await = st.clone();
    if let Some(obj) = st.as_object_mut() {
        obj.insert("t".into(), json!("state"));
    }
    let _ = state.tx.send(st.to_string());
}

async fn edit_slot(state: &Arc<RemoteState>, slot_idx: usize, patch: impl FnOnce(&mut Value)) -> (u16, Value) {
    let mut slots = get_slots(state, None).await;
    if slot_idx >= slots.len() {
        return (400, json!({ "message": "No encontré ese casillero de Swiftplay" }));
    }
    patch(&mut slots[slot_idx]);
    state.lcu(reqwest::Method::PUT, SW_SLOTS_URL, Some(Value::Array(slots))).await
}

async fn execute_command(state: &Arc<RemoteState>, m: &Value) -> Option<Value> {
    let cmd = m.get("cmd")?.as_str()?;
    let to_i64 = |k: &str| m.get(k).and_then(|v| v.as_i64().or_else(|| v.as_str()?.parse().ok())).unwrap_or(0);

    let (status, data) = match cmd {
        "createLobby" => state.lcu(reqwest::Method::POST, "/lol-lobby/v2/lobby", Some(json!({ "queueId": to_i64("queueId") }))).await,
        "leaveLobby" => state.lcu(reqwest::Method::DELETE, "/lol-lobby/v2/lobby", None).await,
        "search" => state.lcu(reqwest::Method::POST, "/lol-lobby/v2/lobby/matchmaking/search", None).await,
        "cancelSearch" => state.lcu(reqwest::Method::DELETE, "/lol-lobby/v2/lobby/matchmaking/search", None).await,
        "accept" => state.lcu(reqwest::Method::POST, "/lol-matchmaking/v1/ready-check/accept", None).await,
        "decline" => state.lcu(reqwest::Method::POST, "/lol-matchmaking/v1/ready-check/decline", None).await,
        "setRole" => {
            let first = m.get("first").and_then(|x| x.as_str()).unwrap_or("TOP").to_uppercase();
            let mut second = m.get("second").and_then(|x| x.as_str()).unwrap_or("MIDDLE").to_uppercase();
            if first == second {
                second = if first == "TOP" { "MIDDLE".into() } else { "TOP".into() };
            }
            *state.last_role_change.write().await = Some((
                Instant::now(),
                json!({ "firstPreference": first, "secondPreference": second, "priorities": ["BOTTOM", "UTILITY"] }),
            ));
            state.lcu(reqwest::Method::PUT, "/lol-lobby/v2/lobby/members/localMember/position-preferences", Some(json!({
                "firstPreference": first,
                "secondPreference": second
            }))).await
        }
        "slotChampion" => {
            let c_id = to_i64("championId");
            edit_slot(state, to_i64("slot") as usize, |s| {
                s["championId"] = json!(c_id);
                s["skinId"] = json!(c_id * 1000);
            }).await
        }
        "slotRole" => {
            let role = m.get("role").and_then(|x| x.as_str()).unwrap_or("TOP").to_uppercase();
            edit_slot(state, to_i64("slot") as usize, |s| {
                s["positionPreference"] = json!(role);
            }).await
        }
        "slotSpells" => {
            let s1 = to_i64("spell1Id");
            let s2 = to_i64("spell2Id");
            edit_slot(state, to_i64("slot") as usize, |s| {
                if s.get("spell1Id").is_some() && s.get("spell1").is_none() {
                    s["spell1Id"] = json!(s1);
                    s["spell2Id"] = json!(s2);
                } else {
                    s["spell1"] = json!(s1);
                    s["spell2"] = json!(s2);
                }
            }).await
        }
        "slotRunes" => {
            let perks: Vec<i64> = match m.get("perks") {
                Some(Value::Array(arr)) => arr.iter().filter_map(|x| x.as_i64()).collect(),
                Some(Value::String(st)) => st.split(',').filter_map(|x| x.trim().parse().ok()).collect(),
                _ => vec![],
            };
            let primary = to_i64("primary");
            let sub = to_i64("sub");
            let (code, res) = edit_slot(state, to_i64("slot") as usize, |s| {
                let p_json = json!({ "perkIds": perks, "perkStyle": primary, "perkSubStyle": sub });
                if s["perks"].is_string() {
                    s["perks"] = json!(p_json.to_string());
                } else {
                    s["perks"] = p_json;
                }
            }).await;
            if code < 400 {
                return Some(json!({ "t": "toast", "msg": "Runas aplicadas", "ok": true }));
            }
            (code, res)
        }
        "skins" => {
            let champ = to_i64("championId");
            let mut list = Vec::new();
            if let Some(sid) = state.get_summoner_id().await {
                let (_, r) = state
                    .lcu(reqwest::Method::GET, &format!("/lol-champions/v1/inventories/{}/champions/{}/skins", sid, champ), None)
                    .await;
                if let Some(arr) = r.as_array() {
                    for x in arr {
                        let is_base = x["isBase"].as_bool().unwrap_or(false);
                        let unlocked = x["unlocked"].as_bool().unwrap_or(false)
                            || x.pointer("/ownership/owned").and_then(|v| v.as_bool()).unwrap_or(false)
                            || x.pointer("/ownership/rental/rented").and_then(|v| v.as_bool()).unwrap_or(false);
                        if is_base || unlocked {
                            let id = x["id"].as_i64().unwrap_or(0);
                            list.push(json!({
                                "id": id,
                                "num": id % 1000,
                                "name": x["name"].as_str().unwrap_or(""),
                                "base": is_base,
                                "tile": x["tilePath"].as_str().unwrap_or("")
                            }));
                        }
                    }
                }
            }
            return Some(json!({ "t": "skins", "championId": champ, "list": list }));
        }
        "setSkin" => {
            let id = to_i64("skinId");
            if m.get("slot").is_some() {
                edit_slot(state, to_i64("slot") as usize, |s| {
                    s["skinId"] = json!(id);
                }).await
            } else {
                state.lcu(reqwest::Method::PATCH, "/lol-champ-select/v1/session/my-selection", Some(json!({ "selectedSkinId": id }))).await
            }
        }
        "choose" | "lock" => {
            let (_, s) = state.lcu(reqwest::Method::GET, "/lol-champ-select/v1/session", None).await;
            let local_cell = s["localPlayerCellId"].as_i64().unwrap_or(-1);
            let mut act = None;
            if let Some(groups) = s["actions"].as_array() {
                for g in groups {
                    for a in g.as_array().unwrap_or(&vec![]) {
                        if a["actorCellId"].as_i64() == Some(local_cell) && !a["completed"].as_bool().unwrap_or(false) {
                            if a["isInProgress"].as_bool().unwrap_or(false) || act.is_none() {
                                act = Some(a.clone());
                            }
                        }
                    }
                }
            }
            let Some(a) = act else {
                return Some(json!({ "t": "toast", "msg": "Todavía no es tu turno" }));
            };
            let id = a["id"].as_i64().unwrap_or(0);
            if cmd == "choose" {
                state.lcu(reqwest::Method::PATCH, &format!("/lol-champ-select/v1/session/actions/{}", id), Some(json!({ "championId": to_i64("championId") }))).await
            } else {
                let c_id = a["championId"].as_i64().unwrap_or(0);
                let url = format!("/lol-champ-select/v1/session/actions/{}", id);
                let (st1, d1) = state.lcu(reqwest::Method::PATCH, &url, Some(json!({ "championId": c_id, "completed": true }))).await;
                if !(200..300).contains(&st1) {
                    state.lcu(reqwest::Method::POST, &format!("{}/complete", url), Some(json!({ "championId": c_id }))).await
                } else {
                    (st1, d1)
                }
            }
        }
        "swap" => state.lcu(reqwest::Method::POST, &format!("/lol-champ-select/v1/session/bench/swap/{}", to_i64("championId")), None).await,
        "reroll" => state.lcu(reqwest::Method::POST, "/lol-champ-select/v1/session/my-selection/reroll", None).await,
        "setSpells" => state.lcu(reqwest::Method::PATCH, "/lol-champ-select/v1/session/my-selection", Some(json!({ "spell1Id": to_i64("spell1Id"), "spell2Id": to_i64("spell2Id") }))).await,
        "selectPage" => state.lcu(reqwest::Method::PUT, "/lol-perks/v1/currentpage", Some(json!(to_i64("id")))).await,
        "applyRunes" => {
            let perks: Vec<i64> = match m.get("perks") {
                Some(Value::Array(arr)) => arr.iter().filter_map(|x| x.as_i64()).collect(),
                Some(Value::String(st)) => st.split(',').filter_map(|x| x.trim().parse().ok()).collect(),
                _ => vec![],
            };
            if perks.len() != 9 {
                return Some(json!({ "t": "toast", "msg": "La página de runas está incompleta" }));
            }
            let body = json!({
                "name": "LoL Remote",
                "primaryStyleId": to_i64("primary"),
                "subStyleId": to_i64("sub"),
                "selectedPerkIds": perks,
                "current": true
            });
            let (_, list) = state.lcu(reqwest::Method::GET, "/lol-perks/v1/pages", None).await;
            let editable: Vec<Value> = list
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .filter(|p| p["isEditable"].as_bool().unwrap_or(false))
                .cloned()
                .collect();
            if let Some(ours) = editable.iter().find(|p| p["name"].as_str().unwrap_or("").starts_with("LoL Remote")) {
                if let Some(id) = ours["id"].as_i64() {
                    let _ = state.lcu(reqwest::Method::DELETE, &format!("/lol-perks/v1/pages/{}", id), None).await;
                }
            }
            let (mut code, mut res) = state.lcu(reqwest::Method::POST, "/lol-perks/v1/pages", Some(body.clone())).await;
            if code >= 400 {
                if let Some(victim) = editable.iter().find(|p| p["current"].as_bool().unwrap_or(false)).or_else(|| editable.first()) {
                    if let Some(id) = victim["id"].as_i64() {
                        let _ = state.lcu(reqwest::Method::DELETE, &format!("/lol-perks/v1/pages/{}", id), None).await;
                        let (c2, r2) = state.lcu(reqwest::Method::POST, "/lol-perks/v1/pages", Some(body)).await;
                        code = c2;
                        res = r2;
                    }
                }
            }
            if code < 400 {
                return Some(json!({ "t": "toast", "msg": "Runas aplicadas", "ok": true }));
            }
            (code, res)
        }
        _ => (200, Value::Null),
    };

    if status >= 400 {
        let msg = data.get("message").and_then(|x| x.as_str()).unwrap_or("Error en el cliente de LoL");
        return Some(json!({ "t": "toast", "msg": msg }));
    }
    None
}

#[tauri::command]
pub async fn get_remote_status(state: tauri::State<'_, Arc<RemoteState>>) -> Result<RemoteStatusUI, String> {
    let ip = local_ip_address::local_ip().map(|i| i.to_string()).unwrap_or_else(|_| "127.0.0.1".into());
    let pin = state.pin.read().await.clone();
    let url = format!("http://{}:{}/?pin={}", ip, state.port, pin);
    let code = QrCode::new(url.as_bytes()).map_err(|e| e.to_string())?;
    let qr_svg = code
        .render::<svg::Color>()
        .min_dimensions(180, 180)
        .dark_color(svg::Color("#c8aa6e"))
        .light_color(svg::Color("#0a1428"))
        .build();

    let last = state.last_state.read().await;
    Ok(RemoteStatusUI {
        running: true,
        url,
        pin,
        qr_svg,
        connected_clients: *state.clients_count.read().await,
        lcu_connected: last["connected"].as_bool().unwrap_or(false),
        phase: last["phase"].as_str().unwrap_or("Offline").to_string(),
    })
}