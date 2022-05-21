use anyhow::{anyhow, bail, Result};
use futures::{channel::mpsc, StreamExt};
use fxhash::{FxHashMap, FxHashSet};
use gstreamer::prelude::*;
use log::{debug, error, info, warn};
use netidx::{
    chars::Chars,
    pack::Pack,
    path::Path as NPath,
    protocol::value::{FromValue, Typ},
    publisher::{Publisher, UpdateBatch, Val, Value},
    utils::pack,
};
use netidx_container::{Container, Datum, Db, Params as ContainerParams, Txn};
use netidx_protocols::rpc::server as rpc;
use netidx_tools::ClientParams;
use regex::Regex;
use std::{
    collections::{HashMap, HashSet},
    convert::Into,
    fs::{self, File},
    io::Read,
    path::PathBuf,
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use structopt::StructOpt;
use tokio::task::block_in_place;

#[derive(StructOpt, Debug)]
struct Params {
    #[structopt(flatten)]
    container_config: ContainerParams,
    #[structopt(flatten)]
    client_params: ClientParams,
    #[structopt(long = "library", help = "path to the music library")]
    library_path: String,
    #[structopt(long = "base", help = "base path of the app in netidx")]
    base: String,
}

enum ToPlayer {
    Play(String),
    Stop,
    Terminate,
}

struct PlayerInner {
    to: glib::Sender<ToPlayer>,
}

impl Drop for PlayerInner {
    fn drop(&mut self) {
        let _ = self.to.send(ToPlayer::Terminate);
    }
}

#[derive(Clone)]
struct Player(Arc<PlayerInner>);

impl Player {
    fn task(rx: glib::Receiver<ToPlayer>) -> Result<()> {
        gstreamer::init()?;
        let main_loop = glib::MainLoop::new(None, false);
        let dispatcher = gstreamer_player::PlayerGMainContextSignalDispatcher::new(None);
        let player = gstreamer_player::Player::new(
            gstreamer_player::PlayerVideoRenderer::NONE,
            Some(&dispatcher.upcast::<gstreamer_player::PlayerSignalDispatcher>()),
        );
        player.connect_end_of_stream(|player| {
            info!("player finished playing");
            player.stop();
        });
        player.connect_error(|player, error| {
            error!("player error: {}", error);
            player.stop();
        });
        let _main_loop = main_loop.clone();
        rx.attach(None, move |m| match m {
            ToPlayer::Play(s) => {
                info!("player now playing {}", &s);
                player.set_uri(Some(&s));
                player.play();
                glib::Continue(true)
            }
            ToPlayer::Stop => {
                info!("player stopped");
                player.set_uri(None);
                player.stop();
                glib::Continue(true)
            }
            ToPlayer::Terminate => {
                info!("player shutting down");
                _main_loop.quit();
                glib::Continue(false)
            }
        });
        main_loop.run();
        Ok(())
    }

    fn new() -> Self {
        let (tx, rx) = glib::MainContext::channel(glib::PRIORITY_LOW);
        thread::spawn(move || match Self::task(rx) {
            Ok(()) => info!("player task stopped"),
            Err(e) => error!("player task stopped with error: {}", e),
        });
        Player(Arc::new(PlayerInner { to: tx }))
    }

    fn play(&self, file: &str) -> Result<()> {
        let uri = format!("file://{}", file);
        Ok(self.0.to.send(ToPlayer::Play(uri))?)
    }

    fn stop(&self) -> Result<()> {
        Ok(self.0.to.send(ToPlayer::Stop)?)
    }
}

struct RpcApi {
    _play: rpc::Proc,
    _stop: rpc::Proc,
}

impl RpcApi {
    fn new(
        api_path: NPath,
        publisher: &Publisher,
        player: Player,
        db: Db,
    ) -> Result<Self> {
        let _play = Self::start_play_rpc(&api_path, publisher, player.clone(), db)?;
        let _stop = Self::start_stop_rpc(&api_path, publisher, player.clone())?;
        Ok(RpcApi { _play, _stop })
    }

    fn err(s: &'static str) -> Value {
        Value::Error(Chars::from(s))
    }

    fn start_stop_rpc(
        base_path: &NPath,
        publisher: &Publisher,
        player: Player,
    ) -> Result<rpc::Proc> {
        rpc::Proc::new(
            publisher,
            base_path.append("rpcs/stop"),
            Value::from("stop playing"),
            HashMap::default(),
            Arc::new(move |_addr, _args| {
                let player = player.clone();
                Box::pin(async move {
                    match player.stop() {
                        Ok(()) => Value::Ok,
                        Err(e) => Value::Error(Chars::from(e.to_string())),
                    }
                })
            }),
        )
    }

    fn start_play_rpc(
        base_path: &NPath,
        publisher: &Publisher,
        player: Player,
        db: Db,
    ) -> Result<rpc::Proc> {
        rpc::Proc::new(
            publisher,
            base_path.append("rpcs/play"),
            Value::from("play a track from the library"),
            vec![(
                Arc::from("track"),
                (Value::Null, Value::from("the md5 sum of the track to play")),
            )]
            .into_iter()
            .collect(),
            Arc::new(move |_addr, mut args| {
                let player = player.clone();
                let db = db.clone();
                Box::pin(async move {
                    let track = match args.remove("track") {
                        None => return Self::err("expected a track id"),
                        Some(vs) => match &vs[..] {
                            [Value::String(s)] => s.clone(),
                            _ => return Self::err("track id must be a single string"),
                        },
                    };
                    let id = match db.lookup_value(&format!("{}/id", track)) {
                        Some(Value::String(id)) => id.clone(),
                        None | Some(_) => track,
                    };
                    let file = format!("{}/file", &*id);
                    let file = match db.lookup_value(&file) {
                        Some(Value::String(f)) => f,
                        None | Some(_) => return Self::err("track not found"),
                    };
                    match player.play(&*file) {
                        Ok(()) => Value::Ok,
                        Err(e) => Value::Error(Chars::from(e.to_string())),
                    }
                })
            }),
        )
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct Digest(md5::Digest);

impl Digest {
    fn compute_from_bytes<S: AsRef<[u8]>>(s: S) -> Self {
        let mut ctx = md5::Context::new();
        ctx.consume(&s.as_ref());
        Self(ctx.compute())
    }

    fn compute_from_file(path: &str) -> Result<Self> {
        const BUF: usize = 32384;
        let mut ctx = md5::Context::new();
        let mut fd = File::open(path)?;
        let mut contents = [0u8; BUF];
        loop {
            let n = fd.read(&mut contents[0..])?;
            if n > 0 {
                ctx.consume(&contents[0..n])
            } else {
                break;
            }
        }
        Ok(Self(ctx.compute()))
    }

    fn from_str(s: &str) -> Result<Self> {
        let mut digest = [0u8; 16];
        hex::decode_to_slice(s, &mut digest)?;
        Ok(Self(md5::Digest(digest)))
    }

    fn to_string(&self) -> String {
        hex::encode(&(self.0).0)
    }
}

impl FromValue for Digest {
    fn from_value(v: Value) -> Result<Self> {
        let mut digest = [0u8; 16];
        v.cast(Typ::String).ok_or_else(|| anyhow!("can't cast")).and_then(|v| match v {
            Value::String(c) => {
                hex::decode_to_slice(&*c, &mut digest)?;
                Ok(Digest(md5::Digest(digest)))
            }
            _ => bail!("can't cast"),
        })
    }

    fn get(v: Value) -> Option<Self> {
        let mut digest = [0u8; 16];
        match v {
            Value::String(c) => {
                hex::decode_to_slice(&*c, &mut digest).ok()?;
                Some(Digest(md5::Digest(digest)))
            }
            _ => None,
        }
    }
}

impl Into<Value> for Digest {
    fn into(self) -> Value {
        Value::String(Chars::from(hex::encode(&(self.0).0)))
    }
}

struct Track {
    album: Option<Chars>,
    artist: Option<Chars>,
    genre: Option<Chars>,
    title: Option<Chars>,
    id: NPath,
}

impl Track {
    fn album_id(db: &Db, path: &NPath) -> Option<Digest> {
        db.lookup_value(&*path.append("album"))
            .and_then(|v| v.get_as::<Chars>())
            .map(|c| Digest::compute_from_bytes(&*c))
    }

    fn artist_id(db: &Db, path: &NPath) -> Option<Digest> {
        db.lookup_value(&*path.append("artist"))
            .and_then(|v| v.get_as::<Chars>())
            .map(|c| Digest::compute_from_bytes(&*c))
    }
}

struct Artist {
    tracks: FxHashSet<Digest>,
    albums: FxHashSet<Digest>,
}

impl Artist {
    fn new() -> Self {
        Self { tracks: HashSet::default(), albums: HashSet::default() }
    }

    fn merge_from(&mut self, other: &mut Artist) {
        self.tracks.extend(other.tracks.drain());
        self.albums.extend(other.albums.drain());
    }

    fn load(db: &Db, path: &NPath) -> Artist {
        let tracks = db
            .lookup_value(&*path.append("tracks"))
            .and_then(|v| v.cast_to::<FxHashSet<Digest>>().ok())
            .unwrap_or_else(HashSet::default);
        let albums = db
            .lookup_value(&*path.append("albums"))
            .and_then(|v| v.cast_to::<FxHashSet<Digest>>().ok())
            .unwrap_or_else(HashSet::default);
        Artist { tracks, albums }
    }
}

struct Album {
    tracks: FxHashSet<Digest>,
    artists: FxHashSet<Digest>,
}

impl Album {
    fn new() -> Self {
        Self { tracks: HashSet::default(), artists: HashSet::default() }
    }

    fn merge_from(&mut self, other: &mut Album) {
        self.tracks.extend(other.tracks.drain());
        self.artists.extend(other.artists.drain());
    }

    fn load(db: &Db, path: &NPath) -> Album {
        let tracks = db
            .lookup_value(&*path.append("tracks"))
            .and_then(|v| v.cast_to::<FxHashSet<Digest>>().ok())
            .unwrap_or_else(HashSet::default);
        let artists = db
            .lookup_value(&*path.append("artists"))
            .and_then(|v| v.cast_to::<FxHashSet<Digest>>().ok())
            .unwrap_or_else(HashSet::default);
        Album { tracks, artists }
    }
}

struct Display {
    artists: FxHashSet<Digest>,
    albums: FxHashSet<Digest>,
    tracks: FxHashSet<Digest>,
    selected_artists: Val,
    selected_albums: Val,
    filter: Val,
    artists_filter: Val,
    albums_filter: Val,
    tracks_filter: Val,
    db: Db,
    publisher: Publisher,
    base: NPath,
    albums_path: NPath,
    tracks_path: NPath,
}

impl Display {
    fn iter_tracks(db: &Db, base: &NPath) -> impl Iterator<Item = Track> {
        let mut album = None;
        let mut artist = None;
        let mut genre = None;
        db.iter_prefix(base.append("tracks")).filter_map(move |r| {
            let (path, _, data) = r.ok()?;
            let decode = || {
                <Datum as Pack>::decode(&mut &*data).ok().and_then(|v| match v {
                    Datum::Data(v) => v.cast_to::<Chars>().ok(),
                    Datum::Deleted | Datum::Formula(_, _) => None,
                })
            };
            let column = NPath::basename(&path)?;
            match column {
                "album" => {
                    album = decode();
                    None
                }
                "artist" => {
                    artist = decode();
                    None
                }
                "file" => None,
                "genre" => {
                    genre = decode();
                    None
                }
                "title" => Some(Track {
                    album: album.take(),
                    artist: artist.take(),
                    genre: genre.take(),
                    title: decode(),
                    id: path,
                }),
                _ => None,
            }
        })
    }

    fn apply_filter(&mut self, filter: Option<&Regex>) {
        self.albums.clear();
        self.artists.clear();
        self.tracks.clear();
        let matching_tracks = Self::iter_tracks(&self.db, &self.base)
            .filter(|t| {
                filter
                    .map(|f| {
                        t.album.as_ref().map(|a| f.is_match(a)).unwrap_or(false)
                            || t.artist.as_ref().map(|a| f.is_match(a)).unwrap_or(false)
                            || t.genre.as_ref().map(|g| f.is_match(g)).unwrap_or(false)
                            || t.title.as_ref().map(|t| f.is_match(t)).unwrap_or(false)
                    })
                    .unwrap_or(true)
            })
            .filter_map(|t| {
                if let Some(artist) = &t.artist {
                    self.artists.insert(Digest::compute_from_bytes(&**artist));
                }
                if let Some(album) = &t.album {
                    self.albums.insert(Digest::compute_from_bytes(&**album));
                }
                NPath::dirname(&t.id)
                    .and_then(|p| NPath::basename(p))
                    .and_then(|s| Digest::from_str(s).ok())
            });
        self.tracks.extend(matching_tracks);
    }

    fn update(
        &mut self,
        batch: &mut UpdateBatch,
        selected_artists: &FxHashSet<Digest>,
        selected_albums: &FxHashSet<Digest>,
        filter: Option<Option<&Regex>>,
    ) -> Result<()> {
        if let Some(filter) = filter {
            self.apply_filter(filter);
        }
        let visible_albums = if selected_artists.is_empty() {
            None
        } else {
            let visible = self
                .albums
                .iter()
                .filter_map(|d| {
                    let album =
                        Album::load(&self.db, &self.albums_path.append(&d.to_string()));
                    if selected_artists.iter().any(|a| album.artists.contains(a)) {
                        Some(*d)
                    } else {
                        None
                    }
                })
                .collect::<FxHashSet<Digest>>();
            Some(visible)
        };
        let visible_tracks = if selected_albums.is_empty() && selected_artists.is_empty()
        {
            None
        } else {
            let visible = self
                .tracks
                .iter()
                .copied()
                .filter(|d| {
                    let path = self.tracks_path.append(&d.to_string());
                    let album = Track::album_id(&self.db, &path);
                    let artist = Track::artist_id(&self.db, &path);
                    let matched_album = selected_albums.is_empty()
                        || album
                            .as_ref()
                            .map(|a| selected_albums.contains(a))
                            .unwrap_or(false);
                    let matched_artist = selected_artists.is_empty()
                        || artist
                            .as_ref()
                            .map(|a| selected_artists.contains(a))
                            .unwrap_or(false);
                    matched_album && matched_artist
                })
                .collect::<FxHashSet<Digest>>();
            Some(visible)
        };
        let filter =
            |visible: Option<FxHashSet<Digest>>, default: &FxHashSet<Digest>| -> Value {
                match visible {
                    Some(visible) => {
                        let v = Value::from(visible);
                        Value::from(vec![Value::from("include"), v])
                    }
                    None => {
                        let v = Value::from(default.clone());
                        Value::from(vec![Value::from("include"), v])
                    }
                }
            };
        self.tracks_filter.update_changed(batch, filter(visible_tracks, &self.tracks));
        self.albums_filter.update_changed(batch, filter(visible_albums, &self.albums));
        self.artists_filter.update_changed(batch, filter(None, &self.artists));
        Ok(())
    }

    async fn run(mut self) {
        let (w_tx, mut w_rx) = mpsc::channel(3);
        self.publisher.writes(self.selected_albums.id(), w_tx.clone());
        self.publisher.writes(self.selected_artists.id(), w_tx.clone());
        self.publisher.writes(self.filter.id(), w_tx);
        let mut selected_artists: FxHashSet<Digest> = HashSet::default();
        let mut selected_albums: FxHashSet<Digest> = HashSet::default();
        let mut filter: Option<Regex> = None;
        while let Some(mut batch) = w_rx.next().await {
            let mut filter_changed = false;
            let mut updates = self.publisher.start_batch();
            for req in batch.drain(..) {
                match req.id {
                    id if id == self.selected_albums.id() => {
                        selected_albums.clear();
                        match req.value.clone().cast_to::<Vec<Chars>>() {
                            Ok(set) => {
                                self.selected_albums
                                    .update_changed(&mut updates, req.value);
                                selected_albums.extend(
                                    set.iter()
                                        .filter_map(|p| NPath::dirname(p))
                                        .filter_map(|p| NPath::basename(p))
                                        .filter_map(|p| Digest::from_str(p).ok()),
                                );
                            }
                            Err(_) => {
                                let m = "expected a list of albums";
                                let e = Value::Error(Chars::from(m));
                                self.selected_albums.update_changed(&mut updates, e);
                            }
                        }
                    }
                    id if id == self.selected_artists.id() => {
                        selected_artists.clear();
                        match req.value.clone().cast_to::<Vec<Chars>>() {
                            Ok(set) => {
                                self.selected_artists
                                    .update_changed(&mut updates, req.value);
                                selected_artists.extend(
                                    set.iter()
                                        .filter_map(|p| NPath::dirname(p))
                                        .filter_map(|p| NPath::basename(p))
                                        .filter_map(|p| Digest::from_str(p).ok()),
                                );
                            }
                            Err(_) => {
                                let m = "expected a list of artists";
                                let e = Value::Error(Chars::from(m));
                                self.selected_artists.update_changed(&mut updates, e);
                            }
                        }
                    }
                    id if id == self.filter.id() => {
                        match req.value.clone().cast_to::<Chars>().and_then(|s| {
                            if s.trim() == "" {
                                Ok(None)
                            } else if s.starts_with("#r") {
                                let s = s
                                    .strip_prefix("#r")
                                    .ok_or_else(|| anyhow!("missing prefix"))?;
                                Ok(Some(Regex::new(s)?))
                            } else {
                                Ok(Some(Regex::new(&format!("(?i).*{}.*", &*s))?))
                            }
                        }) {
                            Ok(re) => {
                                filter_changed = true;
                                self.filter.update_changed(&mut updates, req.value);
                                filter = re;
                            }
                            Err(_) => {
                                let e = Value::Error(Chars::from("expected a regex"));
                                self.filter.update_changed(&mut updates, e);
                            }
                        }
                    }
                    id => warn!("unknown write id {:?}", id),
                }
            }
            let filter = if filter_changed { Some(filter.as_ref()) } else { None };
            let ts = Instant::now();
            debug!("Display::run: starting update");
            let r = block_in_place(|| {
                self.update(&mut updates, &selected_artists, &selected_albums, filter)
            });
            if let Err(e) = r {
                error!("update display failed {}", e)
            }
            debug!("Display::run: finished update in {}s", ts.elapsed().as_secs_f32());
            updates.commit(None).await;
        }
    }

    async fn new(base: NPath, db: Db, publisher: Publisher) -> Result<Self> {
        let filter = publisher.publish(base.append("filter"), Value::from(""))?;
        let selected_albums =
            publisher.publish(base.append("selected-albums"), Value::Null)?;
        let selected_artists =
            publisher.publish(base.append("selected-artists"), Value::Null)?;
        let tracks_filter =
            publisher.publish(base.append("tracks-filter"), Value::Null)?;
        let artists_filter =
            publisher.publish(base.append("artists-filter"), Value::Null)?;
        let albums_filter =
            publisher.publish(base.append("albums-filter"), Value::Null)?;
        let albums_path = base.append("albums");
        let tracks_path = base.append("tracks");
        let mut t = Self {
            artists: HashSet::default(),
            albums: HashSet::default(),
            tracks: HashSet::default(),
            tracks_filter,
            albums_filter,
            artists_filter,
            selected_artists,
            selected_albums,
            filter,
            db,
            publisher,
            base,
            albums_path,
            tracks_path,
        };
        let mut batch = t.publisher.start_batch();
        block_in_place(|| {
            t.update(&mut batch, &HashSet::default(), &HashSet::default(), Some(None))
        })?;
        batch.commit(None).await;
        Ok(t)
    }
}

fn dirs(path: &str) -> Result<FxHashMap<String, SystemTime>> {
    fn inner(path: PathBuf, res: &mut FxHashMap<String, SystemTime>) -> Result<()> {
        let md = fs::metadata(&path)?;
        if !md.is_dir() {
            bail!("base isn't a directory");
        }
        let mut rd = fs::read_dir(&path)?;
        res.insert(path.to_string_lossy().into_owned(), md.modified()?);
        while let Some(ent) = rd.next() {
            let ent = ent?;
            let typ = ent.file_type()?;
            if typ.is_dir() {
                inner(ent.path(), res)?
            } else if typ.is_symlink() {
                let path = ent.path();
                let md = fs::metadata(&path)?;
                if md.is_dir() {
                    inner(path, res)?;
                }
            }
        }
        Ok(())
    }
    let mut res = HashMap::default();
    inner(PathBuf::from(path), &mut res)?;
    Ok(res)
}

fn dirs_to_scan(
    dirs: &FxHashMap<String, SystemTime>,
    db: &sled::Tree,
) -> Result<FxHashSet<String>> {
    let mut res = HashSet::default();
    for (dir, modified) in dirs {
        match db.get(&*dir)? {
            None => {
                res.insert(dir.clone());
                let d = modified.duration_since(UNIX_EPOCH)?;
                db.insert(&*dir, &*pack(&d)?)?;
            }
            Some(v) => {
                let d = <Duration as Pack>::decode(&mut &*v)?;
                let ts = UNIX_EPOCH
                    .checked_add(d)
                    .ok_or_else(|| anyhow!("invalid timestamp"))?;
                if &ts != modified {
                    res.insert(dir.clone());
                    let d = modified.duration_since(UNIX_EPOCH)?;
                    db.insert(&*dir, &*pack(&d)?)?;
                }
            }
        }
    }
    for r in db.iter().keys() {
        let k = r?;
        let s = std::str::from_utf8(&*k)?;
        if !dirs.contains_key(s) {
            db.remove(&*k)?;
        }
    }
    Ok(res)
}

fn scan_track(
    artists: &mut FxHashMap<Chars, Artist>,
    albums: &mut FxHashMap<Chars, Album>,
    txn: &mut Txn,
    path: &str,
    base: &NPath,
) -> Result<()> {
    use lofty::{read_from_path, Accessor};
    let hash = Digest::compute_from_file(&path)?;
    let track = base.append(&format!("tracks/{:x}", (hash.0)));
    let mut set = |name, val| {
        let key = track.append(name);
        let val = match val {
            None => Value::Null,
            Some(val) => Value::from(String::from(val)),
        };
        txn.set_data(true, key, val, None);
    };
    let tagged = read_from_path(path, false)?;
    set("file", Some(path));
    if let Some(tag) = tagged.primary_tag() {
        set("artist", tag.artist());
        if let Some(artist) = tag.artist() {
            let a = artists
                .entry(Chars::from(String::from(artist)))
                .or_insert_with(Artist::new);
            a.tracks.insert(hash);
            if let Some(album) = tag.album() {
                a.albums.insert(Digest::compute_from_bytes(album));
            }
        }
        set("title", tag.title());
        set("album", tag.album());
        if let Some(album) = tag.album() {
            let a =
                albums.entry(Chars::from(String::from(album))).or_insert_with(Album::new);
            a.tracks.insert(hash);
            if let Some(artist) = tag.artist() {
                a.artists.insert(Digest::compute_from_bytes(artist));
            }
        }
        set("genre", tag.genre());
    } else {
        set("artist", None);
        set("title", None);
        set("album", None);
        set("genre", None);
    }
    Ok(())
}

fn scan_dir(
    dir: &str,
    base: &NPath,
    container: &Container,
) -> Result<(FxHashMap<Chars, Artist>, FxHashMap<Chars, Album>)> {
    use rayon::prelude::*;
    let tracks = fs::read_dir(dir)?
        .filter_map(|r| {
            let ent = r.ok()?;
            let ft = ent.file_type().ok()?;
            if ft.is_dir() {
                None
            } else {
                Some(ent.path().to_string_lossy().into_owned())
            }
        })
        .collect::<Vec<_>>();
    let (_, artists, albums) = tracks
        .par_iter()
        .fold(
            || (Txn::new(), HashMap::default(), HashMap::default()),
            |(mut txn, mut artists, mut albums), track| {
                let _ = scan_track(&mut artists, &mut albums, &mut txn, &track, base);
                (txn, artists, albums)
            },
        )
        .reduce(
            || (Txn::new(), HashMap::default(), HashMap::default()),
            |(txn0, mut art0, mut alb0), (txn1, mut art1, mut alb1)| {
                let _ = container.commit_unbounded(txn0);
                let _ = container.commit_unbounded(txn1);
                for (k, mut v) in art1.drain() {
                    art0.entry(k).or_insert_with(Artist::new).merge_from(&mut v);
                }
                for (k, mut v) in alb1.drain() {
                    alb0.entry(k).or_insert_with(Album::new).merge_from(&mut v);
                }
                (Txn::new(), art0, alb0)
            },
        );
    Ok((artists, albums))
}

fn scan_dirs(
    dirs: &FxHashSet<String>,
    base: &NPath,
    container: &Container,
    db: &Db,
) -> Result<()> {
    use rayon::prelude::*;
    let (artists, albums) =
        dirs.par_iter().map(|dir| scan_dir(dir, base, container)).reduce(
            || Ok((HashMap::default(), HashMap::default())),
            |r0, r1| match (r0, r1) {
                (_, Err(e)) | (Err(e), _) => Err(e),
                (Ok((mut art0, mut alb0)), Ok((mut art1, mut alb1))) => {
                    for (k, mut v) in art1.drain() {
                        art0.entry(k).or_insert_with(Artist::new).merge_from(&mut v);
                    }
                    for (k, mut v) in alb1.drain() {
                        alb0.entry(k).or_insert_with(Album::new).merge_from(&mut v);
                    }
                    Ok((art0, alb0))
                }
            },
        )?;
    let mut txn = Txn::new();
    for (name, mut artist) in artists.into_iter() {
        let hash = Digest::compute_from_bytes(&*name);
        let path = base.append(&format!("artists/{:x}", hash.0));
        txn.set_data(true, path.append("artist"), name.into(), None);
        let mut existing = Artist::load(db, &path);
        artist.merge_from(&mut existing);
        txn.set_data(true, path.append("tracks"), artist.tracks.into(), None);
        txn.set_data(true, path.append("albums"), artist.albums.into(), None);
    }
    for (name, mut album) in albums.into_iter() {
        let hash = Digest::compute_from_bytes(&*name);
        let path = base.append(&format!("albums/{:x}", hash.0));
        txn.set_data(true, path.append("album"), name.into(), None);
        let mut existing = Album::load(db, &path);
        album.merge_from(&mut existing);
        txn.set_data(true, path.append("tracks"), album.tracks.into(), None);
        txn.set_data(true, path.append("artists"), album.artists.into(), None);
    }
    container.commit_unbounded(txn)?;
    Ok(())
}

// only scan tracks if their containing directory has been modified
fn scan_modified(
    path: &str,
    base: &NPath,
    container: &Container,
    db: &Db,
    dirs_tree: &sled::Tree,
) -> Result<()> {
    let dirs = dirs(path)?;
    let to_scan = dirs_to_scan(&dirs, &dirs_tree)?;
    Ok(scan_dirs(&to_scan, &base, container, db)?)
}

// scan every track in the library
fn scan_everything(
    path: &str,
    base: &NPath,
    container: &Container,
    db: &Db,
    dirs_tree: &sled::Tree,
) -> Result<()> {
    let dirs = dirs(path)?;
    let _ = dirs_to_scan(&dirs, &dirs_tree)?; // store the dirs mod timestamps
    let to_scan = dirs.into_iter().map(|(k, _)| k).collect::<FxHashSet<String>>();
    Ok(scan_dirs(&to_scan, &base, container, db)?)
}

async fn init_library(
    library_path: &str,
    base: NPath,
    container: &Container,
) -> Result<()> {
    let db = container.db().await?;
    let roots = db.roots().collect::<Result<Vec<_>>>()?;
    let dirs_tree = db.open_tree("dirs")?;
    if roots.contains(&base) {
        block_in_place(|| {
            scan_modified(library_path, &base, container, &db, &dirs_tree)
        })?;
    } else {
        block_in_place(|| {
            dirs_tree.clear()?;
            db.clear()?;
            Ok::<(), anyhow::Error>(())
        })?;
        let mut txn = Txn::new();
        txn.add_root(base.clone(), None);
        txn.set_locked(base.clone(), None);
        container.commit(txn).await?;
        block_in_place(|| {
            scan_everything(library_path, &base, container, &db, &dirs_tree)
        })?;
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Params::from_args();
    env_logger::init();
    let base = NPath::from(&args.base.clone());
    let api_path = NPath::from(args.container_config.api_path.clone());
    let (config, desired_auth) = args.client_params.load();
    let container = Container::start(config, desired_auth, args.container_config).await?;
    let publisher = container.publisher().await?;
    let db = container.db().await?;
    init_library(&args.library_path, base.clone(), &container).await?;
    let player = Player::new();
    let _rpcs = RpcApi::new(api_path, &publisher, player, db.clone())?;
    Display::new(base, db, publisher).await?.run().await;
    Ok(())
}
