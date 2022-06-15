#[macro_use]
extern crate lazy_static;
use anyhow::{anyhow, bail, Result};
use futures::{channel::mpsc, select_biased, StreamExt};
use fxhash::{FxBuildHasher, FxHashMap, FxHashSet};
use glib::clone;
use gstreamer::prelude::*;
use indexmap::{IndexMap, IndexSet};
use log::{error, info, warn};
use netidx::{
    chars::Chars,
    pack::Pack,
    path::Path as NPath,
    protocol::value::{FromValue, Typ},
    publisher::{Publisher, UpdateBatch, Val, Value, WriteRequest},
    utils::pack,
};
use netidx_container::{Container, Datum, Db, Params as ContainerParams, Txn};
use netidx_tools::ClientParams;
use parking_lot::Mutex;
use rand::prelude::*;
use regex::Regex;
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    convert::Into,
    fs::{self, File},
    io::Read,
    path::PathBuf,
    sync::Arc,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use structopt::StructOpt;
use tokio::task::block_in_place;

static DEFAULT_VIEW: &str = include_str!("main.view");

#[derive(StructOpt, Debug)]
struct Params {
    #[structopt(flatten)]
    container_config: ContainerParams,
    #[structopt(flatten)]
    client_params: ClientParams,
    #[structopt(long = "library", help = "path to the music library")]
    library_path: Option<String>,
    #[structopt(
        long = "base",
        help = "base path of the app in netidx",
        default_value = "/local/music"
    )]
    base: String,
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

enum ToPlayer {
    Play(Option<Digest>),
    Pause,
    Stop,
    Terminate,
}

enum FromPlayer {
    Finished,
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
    fn lookup_uri(db: &Db, base: &NPath, track: Digest) -> Option<String> {
        let path = base.append(&format!("tracks/{:x}/file", track.0));
        db.lookup_value(&*path)
            .and_then(|v| v.cast_to::<Chars>().ok())
            .map(|c| format!("file://{}", &*c))
    }

    fn task(
        db: Db,
        base: NPath,
        tx: mpsc::UnboundedSender<FromPlayer>,
        rx: glib::Receiver<ToPlayer>,
    ) -> Result<()> {
        gstreamer::init()?;
        let main_loop = glib::MainLoop::new(None, false);
        let dispatcher = gstreamer_player::PlayerGMainContextSignalDispatcher::new(None);
        let player = gstreamer_player::Player::new(
            gstreamer_player::PlayerVideoRenderer::NONE,
            Some(&dispatcher.upcast::<gstreamer_player::PlayerSignalDispatcher>()),
        );
        let current: Arc<Mutex<Option<Digest>>> = Arc::new(Mutex::new(None));
        player.connect_end_of_stream(clone!(@strong current => move |player| {
            info!("player finished playing");
            player.stop();
            if let Some(_) = current.lock().take() {
                let _ = tx.unbounded_send(FromPlayer::Finished);
            }
        }));
        player.connect_error(|player, error| {
            error!("player error: {}", error);
            player.stop();
        });
        let _main_loop = main_loop.clone();
        rx.attach(
            None,
            clone!(@strong current => move |m| {
                match m {
                    ToPlayer::Play(Some(track)) => match Self::lookup_uri(&db, &base, track) {
                        None => warn!("track {:x} not found", track.0),
                        Some(uri) => {
                            info!("player now playing {:x}, {}", track.0, uri);
                            player.set_uri(Some(uri.as_str()));
                            player.play();
                            *current.lock() = Some(track);
                        }
                    }
                    ToPlayer::Play(None) => match &*current.lock() {
                        None => warn!("not paused, nothing to resume"),
                        Some(track) => {
                            info!("resumimg {:x}", track.0);
                            player.play();
                        }
                    }
                    ToPlayer::Pause => match &*current.lock() {
                        None => warn!("not playing, can't pause"),
                        Some(track) => {
                            info!("pausing {:x}", track.0);
                            player.pause();
                        }
                    }
                    ToPlayer::Stop => {
                        info!("player stopped");
                        player.stop();
                        *current.lock() = None;
                    }
                    ToPlayer::Terminate => {
                        info!("player shutting down");
                        _main_loop.quit();
                        return glib::Continue(false)
                    }
                };
                glib::Continue(true)
            }),
        );
        main_loop.run();
        Ok(())
    }

    fn new(db: Db, base: NPath, from: mpsc::UnboundedSender<FromPlayer>) -> Self {
        let (tx, rx) = glib::MainContext::channel(glib::PRIORITY_LOW);
        thread::spawn(move || match Self::task(db, base, from, rx) {
            Ok(()) => info!("player task stopped"),
            Err(e) => error!("player task stopped with error: {}", e),
        });
        Player(Arc::new(PlayerInner { to: tx }))
    }

    fn play(&self, track: Option<Digest>) -> Result<()> {
        Ok(self.0.to.send(ToPlayer::Play(track))?)
    }

    fn pause(&self) -> Result<()> {
        Ok(self.0.to.send(ToPlayer::Pause)?)
    }

    fn stop(&self) -> Result<()> {
        Ok(self.0.to.send(ToPlayer::Stop)?)
    }
}

struct Track {
    _track: Option<Chars>,
    album: Option<Chars>,
    artist: Option<Chars>,
    genre: Option<Chars>,
    title: Option<Chars>,
    id: NPath,
}

impl Track {
    fn load_track(db: &Db, path: &NPath) -> Option<Chars> {
        db.lookup_value(&*path.append("track")).and_then(|v| v.get_as::<Chars>())
    }

    fn load_title(db: &Db, path: &NPath) -> Option<Chars> {
        db.lookup_value(&*path.append("title")).and_then(|v| v.get_as::<Chars>())
    }

    fn load_album(db: &Db, path: &NPath) -> Option<Chars> {
        db.lookup_value(&*path.append("album")).and_then(|v| v.get_as::<Chars>())
    }

    fn album_id(db: &Db, path: &NPath) -> Option<Digest> {
        Self::load_album(db, path).map(|c| Digest::compute_from_bytes(&*c))
    }

    fn load_artist(db: &Db, path: &NPath) -> Option<Chars> {
        db.lookup_value(&*path.append("artist")).and_then(|v| v.get_as::<Chars>())
    }

    fn artist_id(db: &Db, path: &NPath) -> Option<Digest> {
        Self::load_artist(db, path).map(|c| Digest::compute_from_bytes(&*c))
    }

    fn load_genre(db: &Db, path: &NPath) -> Option<Chars> {
        db.lookup_value(&*path.append("genre")).and_then(|v| v.get_as::<Chars>())
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SortDir {
    Ascending,
    Descending,
}

impl Into<Value> for SortDir {
    fn into(self) -> Value {
        Value::from(match self {
            Self::Ascending => "ascending",
            Self::Descending => "descending",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SortCol {
    Track,
    Title,
    Album,
    Artist,
    Genre,
}

impl Into<Value> for SortCol {
    fn into(self) -> Value {
        Value::from(match self {
            Self::Track => "track",
            Self::Title => "title",
            Self::Album => "album",
            Self::Artist => "artist",
            Self::Genre => "genre",
        })
    }
}

enum PlayStatus {
    Stopped,
    Playing(Digest),
    Paused(Digest),
}

struct Display {
    albums_filter: Val,
    albums: FxHashSet<Digest>,
    albums_path: NPath,
    artists_filter: Val,
    artists: FxHashSet<Digest>,
    base: NPath,
    db: Db,
    filter_changed: bool,
    filter: Option<Regex>,
    filter_val: Val,
    next_track_val: Val,
    pause_val: Val,
    play: PlayStatus,
    play_queue: IndexSet<Digest, FxBuildHasher>,
    play_val: Val,
    prev_track_val: Val,
    publisher: Publisher,
    repeat: bool,
    repeat_val: Val,
    selected_albums: FxHashSet<Digest>,
    selected_albums_val: Val,
    selected_artists: FxHashSet<Digest>,
    selected_artists_val: Val,
    shuffle_seed: u64,
    shuffle: bool,
    shuffle_val: Val,
    sort_column: IndexMap<SortCol, SortDir, FxBuildHasher>,
    sort_column_val: Val,
    stop_val: Val,
    tracks_filter: Val,
    tracks_path: NPath,
    tracks: Vec<Digest>,
}

impl Display {
    fn iter_tracks(db: &Db, base: &NPath) -> impl Iterator<Item = Track> {
        let mut track = None;
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
                "track" => {
                    track = decode();
                    None
                }
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
                    _track: track.take(),
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

    fn apply_filter(&mut self) {
        self.albums.clear();
        self.artists.clear();
        self.tracks.clear();
        let filter = self.filter.as_ref();
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

    fn compute_visible_tracks(&self) -> Option<Vec<Digest>> {
        if self.selected_albums.is_empty()
            && self.selected_artists.is_empty()
            && self.sort_column.is_empty()
        {
            None
        } else {
            let mut visible = self
                .tracks
                .iter()
                .copied()
                .filter(|d| {
                    let path = self.tracks_path.append(&d.to_string());
                    let album = Track::album_id(&self.db, &path);
                    let artist = Track::artist_id(&self.db, &path);
                    let matched_album = self.selected_albums.is_empty()
                        || album
                            .as_ref()
                            .map(|a| self.selected_albums.contains(a))
                            .unwrap_or(false);
                    let matched_artist = self.selected_artists.is_empty()
                        || artist
                            .as_ref()
                            .map(|a| self.selected_artists.contains(a))
                            .unwrap_or(false);
                    matched_album && matched_artist
                })
                .collect::<Vec<Digest>>();
            if !self.sort_column.is_empty() {
                visible.sort_by(|v0, v1| {
                    let path_v0 = self.tracks_path.append(&v0.to_string());
                    let path_v1 = self.tracks_path.append(&v1.to_string());
                    let cmp = |f: fn(&Db, &NPath) -> Option<Chars>, dir: &SortDir| {
                        let v0 = f(&self.db, &path_v0);
                        let v1 = f(&self.db, &path_v1);
                        match dir {
                            SortDir::Descending => v0.cmp(&v1),
                            SortDir::Ascending => v1.cmp(&v0),
                        }
                    };
                    for (col, dir) in &self.sort_column {
                        let r = match col {
                            SortCol::Track => cmp(Track::load_track, dir),
                            SortCol::Title => cmp(Track::load_title, dir),
                            SortCol::Album => cmp(Track::load_album, dir),
                            SortCol::Artist => cmp(Track::load_artist, dir),
                            SortCol::Genre => cmp(Track::load_genre, dir),
                        };
                        match r {
                            o @ (Ordering::Greater | Ordering::Less) => return o,
                            Ordering::Equal => (),
                        }
                    }
                    Ordering::Equal
                })
            }
            Some(visible)
        }
    }

    fn compute_play_queue(&mut self, visible: Option<&Vec<Digest>>) {
        let visible = visible.unwrap_or(&self.tracks);
        self.play_queue.clear();
        self.play_queue.extend(visible);
        if self.shuffle {
            let mut r = StdRng::seed_from_u64(self.shuffle_seed);
            let l = self.play_queue.len();
            for i in 0..l {
                self.play_queue.swap_indices(i, r.gen_range(i..l));
            }
        }
    }

    fn update(&mut self, batch: &mut UpdateBatch) -> Result<()> {
        if self.filter_changed {
            self.filter_changed = false;
            self.apply_filter();
        }
        let visible_albums = if self.selected_artists.is_empty() {
            None
        } else {
            let visible = self
                .albums
                .iter()
                .filter_map(|d| {
                    let album =
                        Album::load(&self.db, &self.albums_path.append(&d.to_string()));
                    if self.selected_artists.iter().any(|a| album.artists.contains(a)) {
                        Some(*d)
                    } else {
                        None
                    }
                })
                .collect::<FxHashSet<Digest>>();
            Some(visible)
        };
        let visible_tracks = self.compute_visible_tracks();
        self.compute_play_queue(visible_tracks.as_ref());
        fn to_v<V: Into<Value> + Clone>(visible: Option<V>, default: &V) -> Value {
            match visible {
                Some(v) => Value::from(vec![Value::from("include"), v.into()]),
                None => Value::from(vec![Value::from("include"), default.clone().into()]),
            }
        }
        self.tracks_filter.update_changed(batch, to_v(visible_tracks, &self.tracks));
        self.albums_filter.update_changed(batch, to_v(visible_albums, &self.albums));
        self.artists_filter.update_changed(batch, to_v(None, &self.artists));
        Ok(())
    }

    fn handle_select_albums(&mut self, updates: &mut UpdateBatch, req: WriteRequest) {
        self.selected_albums.clear();
        match req.value.clone().cast_to::<Vec<Chars>>() {
            Ok(set) => {
                self.selected_albums_val.update_changed(updates, req.value);
                self.selected_albums.extend(
                    set.iter()
                        .filter_map(|p| NPath::dirname(p))
                        .filter_map(|p| NPath::basename(p))
                        .filter_map(|p| Digest::from_str(p).ok()),
                );
            }
            Err(_) => {
                let m = "expected a list of albums";
                let e = Value::Error(Chars::from(m));
                self.selected_albums_val.update_changed(updates, e);
            }
        }
    }

    fn handle_select_artists(&mut self, updates: &mut UpdateBatch, req: WriteRequest) {
        self.selected_artists.clear();
        match req.value.clone().cast_to::<Vec<Chars>>() {
            Ok(set) => {
                self.selected_artists_val.update_changed(updates, req.value);
                self.selected_artists.extend(
                    set.iter()
                        .filter_map(|p| NPath::dirname(p))
                        .filter_map(|p| NPath::basename(p))
                        .filter_map(|p| Digest::from_str(p).ok()),
                );
            }
            Err(_) => {
                let m = "expected a list of artists";
                let e = Value::Error(Chars::from(m));
                self.selected_artists_val.update_changed(updates, e);
            }
        }
    }

    fn handle_filter(&mut self, updates: &mut UpdateBatch, req: WriteRequest) {
        let re = req.value.clone().cast_to::<Chars>().and_then(|s| {
            if s.trim() == "" {
                Ok(None)
            } else if s.starts_with("#r") {
                let s = s.strip_prefix("#r").ok_or_else(|| anyhow!("missing prefix"))?;
                Ok(Some(Regex::new(s)?))
            } else {
                Ok(Some(Regex::new(&format!("(?i).*{}.*", &*s))?))
            }
        });
        match re {
            Ok(re) => {
                self.filter_val.update_changed(updates, req.value);
                self.filter = re;
                self.filter_changed = true;
            }
            Err(_) => {
                let e = Value::Error(Chars::from("expected a regex"));
                self.filter_val.update_changed(updates, e);
            }
        }
    }

    fn handle_sort_column(&mut self, updates: &mut UpdateBatch, req: WriteRequest) {
        use indexmap::map::Entry;
        let col = match req.value.cast_to::<Chars>() {
            Err(_) => return,
            Ok(c) if &*c == "track" => SortCol::Track,
            Ok(c) if &*c == "title" => SortCol::Title,
            Ok(c) if &*c == "artist" => SortCol::Artist,
            Ok(c) if &*c == "album" => SortCol::Album,
            Ok(c) if &*c == "genre" => SortCol::Genre,
            Ok(_) => return,
        };
        match self.sort_column.entry(col) {
            Entry::Vacant(e) => {
                e.insert(SortDir::Descending);
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                d @ SortDir::Descending => {
                    *d = SortDir::Ascending;
                }
                SortDir::Ascending => {
                    e.remove();
                }
            },
        }
        let v = (false, self.sort_column.clone());
        self.sort_column_val.update_changed(updates, v.into());
    }

    fn play(&mut self, up: &mut UpdateBatch, player: &Player, req: WriteRequest) {
        match req.value.cast_to::<Digest>().ok() {
            Some(track) => {
                self.pause_val.update_changed(up, Value::False);
                self.stop_val.update_changed(up, Value::False);
                self.play_val.update_changed(up, track.into());
                let _ = player.play(Some(track)); // best effort
                self.play = PlayStatus::Playing(track);
            }
            None => match self.play {
                PlayStatus::Playing(track) => {
                    self.pause_val.update_changed(up, Value::True);
                    self.stop_val.update_changed(up, Value::False);
                    self.play_val.update_changed(up, track.into());
                    let _ = player.pause();
                    self.play = PlayStatus::Paused(track);
                }
                PlayStatus::Paused(track) => {
                    self.pause_val.update_changed(up, Value::False);
                    self.stop_val.update_changed(up, Value::False);
                    self.play_val.update_changed(up, track.into());
                    let _ = player.play(None);
                    self.play = PlayStatus::Playing(track);
                }
                PlayStatus::Stopped => match self.play_queue.first() {
                    None => (), // nothing to play
                    Some(track) => {
                        self.pause_val.update_changed(up, Value::False);
                        self.stop_val.update_changed(up, Value::False);
                        self.play_val.update_changed(up, track.into());
                        let _ = player.play(Some(*track));
                        self.play = PlayStatus::Playing(*track);
                    }
                },
            },
        }
    }

    fn pause(&mut self, up: &mut UpdateBatch, player: &Player, _req: WriteRequest) {
        match self.play {
            PlayStatus::Playing(track) => {
                self.pause_val.update_changed(up, Value::True);
                self.stop_val.update_changed(up, Value::False);
                self.play_val.update_changed(up, track.into());
                let _ = player.pause();
                self.play = PlayStatus::Paused(track);
            }
            PlayStatus::Paused(_) | PlayStatus::Stopped => (),
        }
    }

    fn stop(&mut self, up: &mut UpdateBatch, player: &Player, _req: WriteRequest) {
        self.pause_val.update_changed(up, Value::False);
        self.stop_val.update_changed(up, Value::True);
        self.play_val.update_changed(up, Value::Null);
        let _ = player.stop();
        self.play = PlayStatus::Stopped;
    }

    fn repeat(&mut self, up: &mut UpdateBatch, req: WriteRequest) {
        self.repeat = req.value.cast_to::<bool>().unwrap_or(!self.repeat);
        self.repeat_val.update_changed(up, self.repeat.into());
    }

    fn shuffle(&mut self, up: &mut UpdateBatch, req: WriteRequest) {
        if !self.shuffle {
            self.shuffle_seed = random();
        }
        self.shuffle = req.value.cast_to::<bool>().unwrap_or(!self.shuffle);
        self.shuffle_val.update_changed(up, self.shuffle.into());
    }

    fn next_track(&mut self, up: &mut UpdateBatch, player: &Player, rev: bool) {
        match self.play {
            PlayStatus::Stopped => (),
            PlayStatus::Paused(_) => (),
            PlayStatus::Playing(track) => {
                let next = match self.play_queue.get_full(&track) {
                    None => self.play_queue.first(),
                    Some((i, _)) => {
                        let i = if rev && i == 0 {
                            self.play_queue.len() - 1
                        } else if rev && i > 0 {
                            i - 1
                        } else {
                            i + 1
                        };
                        match self.play_queue.get_index(i) {
                            Some(t) => Some(t),
                            None => {
                                if self.repeat {
                                    self.play_queue.first()
                                } else {
                                    None
                                }
                            }
                        }
                    }
                };
                self.pause_val.update_changed(up, Value::False);
                match next {
                    None => {
                        self.stop_val.update_changed(up, Value::True);
                        self.play_val.update_changed(up, Value::Null);
                        let _ = player.stop();
                        self.play = PlayStatus::Stopped;
                    }
                    Some(track) => {
                        self.stop_val.update_changed(up, Value::False);
                        self.play_val.update_changed(up, track.into());
                        let _ = player.play(Some(*track));
                        self.play = PlayStatus::Playing(*track);
                    }
                }
            }
        }
    }

    async fn run(mut self) {
        let (p_tx, mut p_rx) = mpsc::unbounded();
        let (w_tx, mut w_rx) = mpsc::channel(3);
        let player = Player::new(self.db.clone(), self.base.clone(), p_tx);
        self.publisher.writes(self.selected_albums_val.id(), w_tx.clone());
        self.publisher.writes(self.selected_artists_val.id(), w_tx.clone());
        self.publisher.writes(self.filter_val.id(), w_tx.clone());
        self.publisher.writes(self.sort_column_val.id(), w_tx.clone());
        self.publisher.writes(self.pause_val.id(), w_tx.clone());
        self.publisher.writes(self.play_val.id(), w_tx.clone());
        self.publisher.writes(self.repeat_val.id(), w_tx.clone());
        self.publisher.writes(self.shuffle_val.id(), w_tx.clone());
        self.publisher.writes(self.stop_val.id(), w_tx.clone());
        self.publisher.writes(self.next_track_val.id(), w_tx.clone());
        self.publisher.writes(self.prev_track_val.id(), w_tx);
        loop {
            let mut updates = self.publisher.start_batch();
            select_biased! {
                mut batch = w_rx.select_next_some() => {
                    for req in batch.drain(..) {
                        match req.id {
                            id if id == self.selected_albums_val.id() => {
                                self.handle_select_albums(&mut updates, req)
                            }
                            id if id == self.selected_artists_val.id() => {
                                self.handle_select_artists(&mut updates, req)
                            }
                            id if id == self.filter_val.id() => {
                                self.handle_filter(&mut updates, req)
                            }
                            id if id == self.sort_column_val.id() => {
                                self.handle_sort_column(&mut updates, req)
                            }
                            id if id == self.play_val.id() => {
                                self.play(&mut updates, &player, req)
                            }
                            id if id == self.pause_val.id() => {
                                self.pause(&mut updates, &player, req)
                            }
                            id if id == self.stop_val.id() => {
                                self.stop(&mut updates, &player, req)
                            }
                            id if id == self.repeat_val.id() => {
                                self.repeat(&mut updates, req)
                            }
                            id if id == self.shuffle_val.id() => {
                                self.shuffle(&mut updates, req)
                            }
                            id if id == self.next_track_val.id() => {
                                self.next_track(&mut updates, &player, false)
                            }
                            id if id == self.prev_track_val.id() => {
                                self.next_track(&mut updates, &player, true)
                            }
                            id => warn!("unknown write id {:?}", id),
                        }
                    }
                    let r = block_in_place(|| self.update(&mut updates));
                    if let Err(e) = r {
                        error!("update display failed {}", e)
                    }
                },
                m = p_rx.select_next_some() => match m {
                    FromPlayer::Finished => self.next_track(&mut updates, &player, false),
                },
                complete => break,
            }
            updates.commit(None).await;
        }
    }

    async fn new(base: NPath, db: Db, publisher: Publisher) -> Result<Self> {
        let filter_val = publisher.publish(base.append("filter"), Value::from(""))?;
        let empty = Value::Array(Arc::from([]));
        let selected_albums_val =
            publisher.publish(base.append("selected-albums"), empty.clone())?;
        let selected_artists_val =
            publisher.publish(base.append("selected-artists"), empty.clone())?;
        let tracks_filter =
            publisher.publish(base.append("tracks-filter"), Value::Null)?;
        let artists_filter =
            publisher.publish(base.append("artists-filter"), Value::Null)?;
        let albums_filter =
            publisher.publish(base.append("albums-filter"), Value::Null)?;
        let default_sort = [
            (SortCol::Artist, SortDir::Descending),
            (SortCol::Album, SortDir::Descending),
            (SortCol::Track, SortDir::Descending)
        ]
        .into_iter()
        .collect::<IndexMap<_, _, FxBuildHasher>>();
        let sort_column_val = publisher
            .publish(base.append("sort-column"), (false, default_sort.clone()).into())?;
        let shuffle_val = publisher.publish(base.append("shuffle"), Value::False)?;
        let repeat_val = publisher.publish(base.append("repeat"), Value::False)?;
        let play_val = publisher.publish(base.append("play"), Value::False)?;
        let pause_val = publisher.publish(base.append("pause"), Value::False)?;
        let stop_val = publisher.publish(base.append("stop"), Value::False)?;
        let next_track_val = publisher.publish(base.append("next"), Value::Null)?;
        let prev_track_val = publisher.publish(base.append("prev"), Value::Null)?;
        let albums_path = base.append("albums");
        let tracks_path = base.append("tracks");
        let mut t = Self {
            albums_filter,
            albums: HashSet::default(),
            albums_path,
            artists_filter,
            artists: HashSet::default(),
            base,
            db,
            filter_changed: true,
            filter: None,
            filter_val,
            next_track_val,
            pause_val,
            play: PlayStatus::Stopped,
            play_queue: IndexSet::default(),
            play_val,
            prev_track_val,
            publisher,
            repeat: false,
            repeat_val,
            selected_albums: HashSet::default(),
            selected_albums_val,
            selected_artists: HashSet::default(),
            selected_artists_val,
            shuffle_seed: random(),
            shuffle: false,
            shuffle_val,
            sort_column: default_sort,
            sort_column_val,
            stop_val,
            tracks_filter,
            tracks_path,
            tracks: Vec::new(),
        };
        let mut batch = t.publisher.start_batch();
        block_in_place(|| t.update(&mut batch))?;
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

struct TaggedTrack {
    track_number: Chars,
    length: Chars,
    title: Chars,
    artist: Chars,
    album: Chars,
    genre: Chars,
}

impl TaggedTrack {
    fn read(library: &str, path: &str) -> Result<TaggedTrack> {
        use lofty::{read_from_path, Accessor, ItemKey, ItemValue};
        use std::path::Path;
        lazy_static! {
            static ref TRACK_NR: Regex =
                Regex::new("^\\s*([0-9]+)\\s*|\\s*([0-9]+)\\s*$").unwrap();
            static ref CLEANUP: Regex =
                Regex::new("^[[:^alpha:]0-9\\s]*|[[:^alpha:]0-9\\s]*$").unwrap();
            static ref EMPTY: Regex = Regex::new("^\\s*$").unwrap();
        }
        let tags = read_from_path(path, false)?;
        let mut track = Self {
            track_number: Chars::from(""),
            length: Chars::from(""),
            title: Chars::from(""),
            artist: Chars::from(""),
            album: Chars::from(""),
            genre: Chars::from(""),
        };
        macro_rules! set {
            ($field:expr, $val:expr) => {
                if let Some(val) = $val {
                    if val.len() > $field.len() {
                        $field = Chars::from(String::from(val));
                    }
                }
            };
        }
        for tag in tags.tags() {
            set!(track.title, tag.title());
            set!(track.artist, tag.artist());
            set!(track.album, tag.album());
            set!(track.genre, tag.genre());
            for item in tag.items() {
                match item.key() {
                    ItemKey::TrackNumber => match item.value() {
                        ItemValue::Text(s) => set!(track.track_number, Some(s.as_str())),
                        ItemValue::Binary(_) | ItemValue::Locator(_) => (),
                    },
                    ItemKey::Length => match item.value() {
                        ItemValue::Text(s) => set!(track.length, Some(s.as_str())),
                        ItemValue::Binary(_) | ItemValue::Locator(_) => (),
                    },
                    _ => (),
                }
            }
        }
        let path = Path::new(path);
        let ext = path
            .extension()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow!("no extension"))?;
        fn part<'a>(library: &str, path: &'a Path, n: usize) -> Option<&'a str> {
            path.ancestors()
                .nth(n)
                .and_then(|p| {
                    if p.to_str() != Some(library) && p.starts_with(library) {
                        p.file_name()
                    } else {
                        None
                    }
                })
                .and_then(|p| p.to_str())
        }
        if ext != "m4a"
            && ext != "m4p"
            && ext != "mp3"
            && ext != "wav"
            && ext != "ogg"
            && ext != "flac"
            && ext != "opus"
            && ext != "aiff"
            && ext != "webm"
        {
            bail!("bad file type")
        }
        if EMPTY.is_match(&*track.track_number) {
            let nr = path
                .file_stem()
                .and_then(|f| f.to_str())
                .and_then(|f| TRACK_NR.captures(f))
                .and_then(|c| c.get(1).or_else(|| c.get(2)))
                .map(|c| c.as_str());
            if let Some(nr) = nr {
                track.track_number = Chars::from(String::from(nr));
            }
        }
        if EMPTY.is_match(&*track.artist) {
            if let Some(artist) = part(library, path, 2) {
                track.artist = Chars::from(String::from(artist));
            }
        }
        if EMPTY.is_match(&*track.album) {
            if let Some(album) = part(library, path, 1) {
                track.album = Chars::from(String::from(album));
            }
        }
        if EMPTY.is_match(&*track.title) {
            let title = path.file_stem().and_then(|p| p.to_str());
            if let Some(title) = title {
                let title = CLEANUP.replace_all(title, "");
                track.title = Chars::from(String::from(title));
            }
        }
        Ok(track)
    }
}

fn scan_track(
    artists: &mut FxHashMap<Chars, Artist>,
    albums: &mut FxHashMap<Chars, Album>,
    txn: &mut Txn,
    library: &str,
    path: &str,
    base: &NPath,
) -> Result<()> {
    let tag = TaggedTrack::read(library, path)?;
    let hash = Digest::compute_from_file(&path)?;
    let track = base.append(&format!("tracks/{:x}", (hash.0)));
    let mut set = |name, val: Chars| {
        let key = track.append(name);
        txn.set_data(true, key, Value::from(val), None);
    };
    set("file", Chars::from(String::from(path)));
    set("track", tag.track_number);
    set("length", tag.length);
    set("artist", tag.artist.clone());
    let a = artists.entry(tag.artist.clone()).or_insert_with(Artist::new);
    a.tracks.insert(hash);
    a.albums.insert(Digest::compute_from_bytes(&*tag.album));
    set("title", tag.title);
    set("album", tag.album.clone());
    let a = albums.entry(tag.album).or_insert_with(Album::new);
    a.tracks.insert(hash);
    a.artists.insert(Digest::compute_from_bytes(&*tag.artist));
    set("genre", tag.genre);
    Ok(())
}

fn scan_dir(
    library: &str,
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
                let _ = scan_track(
                    &mut artists,
                    &mut albums,
                    &mut txn,
                    library,
                    &track,
                    base,
                );
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
    library: &str,
    dirs: &FxHashSet<String>,
    base: &NPath,
    container: &Container,
    db: &Db,
) -> Result<()> {
    use rayon::prelude::*;
    let (artists, albums) =
        dirs.par_iter().map(|dir| scan_dir(library, dir, base, container)).reduce(
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
    Ok(scan_dirs(path, &to_scan, &base, container, db)?)
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
    Ok(scan_dirs(path, &to_scan, &base, container, db)?)
}

async fn setup_default_view(container: &Container, db: &Db, base: &NPath) -> Result<()> {
    let view_path = base.append(".view");
    if db.lookup_value(&*view_path).is_none() {
        let mut txn = Txn::new();
        txn.set_data(true, view_path, Value::from(DEFAULT_VIEW), None);
        Ok(container.commit(txn).await?)
    } else {
        Ok(())
    }
}

async fn init_library(
    library_path: &str,
    base: NPath,
    container: &Container,
) -> Result<()> {
    let db = container.db().await?;
    let roots = db.roots().collect::<Result<Vec<_>>>()?;
    let dirs_tree = db.open_tree("dirs")?;
    setup_default_view(container, &db, &base).await?;
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

fn params() -> Result<Params> {
    let mut args = Params::from_args();
    if args.container_config.db.is_none() {
        let path = dirs::data_dir()
            .map(|mut p| {
                p.push("demoscene");
                p
            })
            .ok_or_else(|| {
                anyhow!("db dir not specified and the default couldn't be determined")
            })?;
        args.container_config.db = Some(path.to_string_lossy().into_owned());
    }
    if args.library_path.is_none() {
        let path = dirs::audio_dir().ok_or_else(|| {
            anyhow!(
                "music library path not specified and the default can't be determined"
            )
        })?;
        args.library_path = Some(path.to_string_lossy().into_owned());
    }
    Ok(args)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = params()?;
    env_logger::init();
    let base = NPath::from(&args.base);
    let (config, desired_auth) = args.client_params.load();
    let container = Container::start(config, desired_auth, args.container_config).await?;
    let publisher = container.publisher().await?;
    let db = container.db().await?;
    init_library(&args.library_path.as_ref().unwrap(), base.clone(), &container).await?;
    Display::new(base, db, publisher).await?.run().await;
    Ok(())
}
