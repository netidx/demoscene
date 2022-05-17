use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use futures::{channel::mpsc, StreamExt};
use fxhash::{FxHashMap, FxHashSet};
use gstreamer::prelude::*;
use log::{debug, error, info, warn};
use netidx::{
    chars::Chars,
    pack::Pack,
    path::Path as NPath,
    publisher::{Publisher, UpdateBatch, Val, Value},
    utils::pack,
};
use netidx_container::{Container, Datum, Db, Params as ContainerParams, Txn};
use netidx_protocols::rpc::server as rpc;
use netidx_tools::ClientParams;
use regex::Regex;
use std::{
    collections::{HashMap, HashSet},
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
        dbg!(gstreamer::init()?);
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
        dbg!(());
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
        dbg!(());
        main_loop.run();
        dbg!(());
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
                dbg!(&args);
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
                    let file = match db.lookup_value(dbg!(&file)) {
                        Some(Value::String(f)) => dbg!(f),
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

struct Track {
    album: Option<Chars>,
    artist: Option<Chars>,
    genre: Option<Chars>,
    title: Option<Chars>,
    id: NPath,
}

struct Artist {
    tracks: FxHashSet<NPath>,
    albums: FxHashSet<Chars>,
}

impl Artist {
    fn new() -> Self {
        Self { tracks: HashSet::default(), albums: HashSet::default() }
    }
}

struct Album {
    tracks: FxHashSet<NPath>,
    artists: FxHashSet<Chars>,
}

impl Album {
    fn new() -> Self {
        Self { tracks: HashSet::default(), artists: HashSet::default() }
    }
}

struct Display {
    artists: FxHashMap<Chars, Artist>,
    albums: FxHashMap<Chars, Album>,
    tracks: Vec<Track>,
    selected_artists: Val,
    selected_albums: Val,
    filter: Val,
    artists_filter: Val,
    albums_filter: Val,
    tracks_filter: Val,
    db: Db,
    container: Container,
    publisher: Publisher,
    base: NPath,
    artists_path: NPath,
    albums_path: NPath,
    tracks_path: NPath,
    width: usize,
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
                "title" => {
                    let id = NPath::from(ArcStr::from(NPath::dirname(&path)?));
                    Some(Track {
                        album: album.take(),
                        artist: artist.take(),
                        genre: genre.take(),
                        title: decode(),
                        id,
                    })
                }
                _ => None,
            }
        })
    }

    // CR estokes: I'm not sure I don't need this
    #[allow(dead_code)]
    fn clear_prefix(&self, txn: &mut Txn, prefix: NPath) -> Result<()> {
        for r in self.db.iter_prefix(prefix) {
            let (path, _, _) = r?;
            txn.set_data(true, path, Value::Null, None);
        }
        Ok(())
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
            .map(|t| {
                if let Some(artist) = &t.artist {
                    let a =
                        self.artists.entry(artist.clone()).or_insert_with(Artist::new);
                    a.tracks.insert(t.id.clone());
                    if let Some(album) = &t.album {
                        a.albums.insert(album.clone());
                    }
                }
                if let Some(album) = &t.album {
                    let a = self.albums.entry(album.clone()).or_insert_with(Album::new);
                    a.tracks.insert(t.id.clone());
                    if let Some(artist) = &t.artist {
                        a.artists.insert(artist.clone());
                    }
                }
                t
            });
        self.tracks.extend(matching_tracks);
    }

    fn update(
        &mut self,
        batch: &mut UpdateBatch,
        selected_artists: &FxHashSet<Chars>,
        selected_albums: &FxHashSet<Chars>,
        filter: Option<Option<&Regex>>,
    ) -> Result<Txn> {
        let mut txn = Txn::new();
        if let Some(filter) = filter {
            debug!("Display::update: applying filter {:?}", filter);
            let ts = Instant::now();
            self.apply_filter(filter);
            debug!("Display::update: applied filter in {}s", ts.elapsed().as_secs_f32());
        }
        let mut visible_artists = Vec::new();
        for (i, artist) in self.artists.keys().enumerate() {
            let artist_nr = format!("{:0w$}", i, w = self.width);
            let path = self.artists_path.append(&artist_nr).append("0");
            visible_artists.push(Value::from(artist_nr));
            txn.set_data(true, path, Value::String(artist.clone()), None);
        }
        debug!("Display::update: {} visible artists", visible_artists.len());
        let albums = self
            .albums
            .iter()
            .filter_map(|(name, album)| {
                let is_match = selected_artists.is_empty()
                    || selected_artists.iter().any(|a| album.artists.contains(a));
                if is_match {
                    Some(name)
                } else {
                    None
                }
            })
            .enumerate();
        let mut visible_albums = Vec::new();
        for (i, album) in albums {
            let album_nr = format!("{:0w$}", i, w = self.width);
            let path = self.albums_path.append(&album_nr).append("0");
            visible_albums.push(Value::from(album_nr));
            txn.set_data(true, path, Value::String(album.clone()), None);
        }
        debug!("Display::update: {} visible albums", visible_albums.len());
        let tracks = self
            .tracks
            .iter()
            .filter(|track| {
                let matched_album = selected_albums.is_empty()
                    || track
                        .album
                        .as_ref()
                        .map(|a| selected_albums.contains(a))
                        .unwrap_or(false);
                let matched_artist = selected_artists.is_empty()
                    || track
                        .artist
                        .as_ref()
                        .map(|a| selected_artists.contains(a))
                        .unwrap_or(false);
                matched_album && matched_artist
            })
            .enumerate();
        let mut visible_tracks = Vec::new();
        for (i, track) in tracks {
            let track_nr = format!("{:0w$}", i, w = self.width);
            let base = self.tracks_path.append(&track_nr);
            visible_tracks.push(Value::from(track_nr));
            let title = track
                .title
                .as_ref()
                .map(|v| Value::String(v.clone()))
                .unwrap_or(Value::Null);
            txn.set_data(true, base.append("title"), title, None);
            let artist = track
                .artist
                .as_ref()
                .map(|v| Value::String(v.clone()))
                .unwrap_or(Value::Null);
            txn.set_data(true, base.append("artist"), artist, None);
            let genre = track
                .genre
                .as_ref()
                .map(|v| Value::String(v.clone()))
                .unwrap_or(Value::Null);
            txn.set_data(true, base.append("genre"), genre, None);
            let album = track
                .album
                .as_ref()
                .map(|v| Value::String(v.clone()))
                .unwrap_or(Value::Null);
            txn.set_data(true, base.append("album"), album, None);
            let id = Value::String(Chars::from(String::from(&*track.id)));
            txn.set_data(true, base.append("id"), id, None);
        }
        debug!("Display::update: {} visible tracks", visible_tracks.len());
        let v = Value::from(vec![Value::from("include"), Value::from(visible_tracks)]);
        self.tracks_filter.update_changed(batch, v);
        let v = Value::from(vec![Value::from("include"), Value::from(visible_albums)]);
        self.albums_filter.update_changed(batch, v);
        let v = Value::from(vec![Value::from("include"), Value::from(visible_artists)]);
        self.artists_filter.update_changed(batch, v);
        Ok(txn)
    }

    async fn run(mut self) {
        let (w_tx, mut w_rx) = mpsc::channel(3);
        self.publisher.writes(self.selected_albums.id(), w_tx.clone());
        self.publisher.writes(self.selected_artists.id(), w_tx.clone());
        self.publisher.writes(self.filter.id(), w_tx);
        let mut selected_artists: FxHashSet<Chars> = HashSet::default();
        let mut selected_albums: FxHashSet<Chars> = HashSet::default();
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
                                let selected = set.into_iter().filter_map(|p| {
                                    self.db
                                        .lookup_value(&*p)
                                        .and_then(|v| v.cast_to::<Chars>().ok())
                                });
                                selected_albums.extend(selected);
                            }
                            Err(_) => {
                                let e = Value::Error(Chars::from(
                                    "expected a list of albums",
                                ));
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
                                let selected = set.into_iter().filter_map(|p| {
                                    self.db
                                        .lookup_value(&*p)
                                        .and_then(|v| v.cast_to::<Chars>().ok())
                                });
                                selected_artists.extend(selected);
                            }
                            Err(_) => {
                                let e = Value::Error(Chars::from(
                                    "expected a list of artists",
                                ));
                                self.selected_artists.update_changed(&mut updates, e);
                            }
                        }
                    }
                    id if id == self.filter.id() => {
                        match req.value.clone().cast_to::<Chars>().and_then(|s| {
                            if s.trim() == "" {
                                Ok(None)
                            } else if s.starts_with("#") {
                                Ok(Some(Regex::new(&*s)?))
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
            debug!("Display::run: finished update in {}s", ts.elapsed().as_secs_f32());
            updates.commit(None).await;
            match r {
                Ok(txn) => {
                    if let Err(e) = self.container.commit(txn).await {
                        error!("txn commit failed {}", e);
                        break;
                    }
                }
                Err(e) => {
                    error!("update display failed {}", e)
                }
            }
        }
    }

    async fn new(
        base: NPath,
        db: Db,
        container: Container,
        publisher: Publisher,
    ) -> Result<Self> {
        let filter = publisher.publish(base.append("filter"), Value::Null)?;
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
        let artists_path = base.append("artists");
        let albums_path = base.append("albums");
        let tracks_path = base.append("filtered-tracks");
        let n = (db.prefix_len(&base.append("tracks")) as f32) / 5.;
        let width = 1 + n.log10() as usize;
        let n = n as usize;
        let mut txn = Txn::new();
        let rows = (0..n)
            .into_iter()
            .map(|i| Chars::from(format!("{:0w$}", i, w = width)))
            .collect::<Vec<_>>();
        let cols = vec![
            Chars::from("title"),
            Chars::from("album"),
            Chars::from("artist"),
            Chars::from("genre"),
            Chars::from("id"),
        ];
        txn.create_table(tracks_path.clone(), rows, cols, true, None);
        txn.create_sheet(albums_path.clone(), n, 1, n, 1, true, None);
        txn.create_sheet(artists_path.clone(), n, 1, n, 1, true, None);
        container.commit(txn).await?;
        let mut t = Self {
            artists: HashMap::default(),
            albums: HashMap::default(),
            tracks: Vec::new(),
            tracks_filter,
            albums_filter,
            artists_filter,
            selected_artists,
            selected_albums,
            filter,
            db,
            container,
            publisher,
            base,
            artists_path,
            albums_path,
            tracks_path,
            width,
        };
        let mut batch = t.publisher.start_batch();
        let txn = block_in_place(|| {
            t.update(&mut batch, &HashSet::default(), &HashSet::default(), Some(None))
        })?;
        batch.commit(None).await;
        t.container.commit(txn).await?;
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

fn hash_file(path: &str) -> Result<md5::Digest> {
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
    Ok(ctx.compute())
}

fn scan_track(txn: &mut Txn, path: &str, base: &NPath) -> Result<()> {
    use lofty::{read_from_path, Accessor};
    let hash = hash_file(&path)?;
    let mut set = |name, val| {
        let key = base.append(&format!("tracks/{:x}/{}", hash, name));
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
        set("title", tag.title());
        set("album", tag.album());
        set("genre", tag.genre());
    } else {
        set("artist", None);
        set("title", None);
        set("album", None);
        set("genre", None);
    }
    Ok(())
}

fn scan_dir(dir: &str, base: &NPath, container: &Container) -> Result<()> {
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
    tracks
        .par_iter()
        .fold(Txn::new, |mut txn, track| {
            let _ = scan_track(&mut txn, &track, base);
            txn
        })
        .for_each(|txn| {
            let _ = container.commit_unbounded(txn);
        });
    Ok(())
}

fn scan_dirs(
    dirs: &FxHashSet<String>,
    base: &NPath,
    container: &Container,
) -> Result<()> {
    use rayon::prelude::*;
    dirs.par_iter().map(|dir| scan_dir(dir, base, container)).collect::<Result<()>>()
}

// only scan tracks if their containing directory has been modified
fn scan_modified(
    path: &str,
    base: &NPath,
    container: &Container,
    dirs_tree: &sled::Tree,
) -> Result<()> {
    let dirs = dirs(path)?;
    let to_scan = dirs_to_scan(&dirs, &dirs_tree)?;
    Ok(scan_dirs(&to_scan, &base, container)?)
}

// scan every track in the library
fn scan_everything(
    path: &str,
    base: &NPath,
    container: &Container,
    dirs_tree: &sled::Tree,
) -> Result<()> {
    let dirs = dirs(path)?;
    let _ = dirs_to_scan(&dirs, &dirs_tree)?; // store the dirs mod timestamps
    let to_scan = dirs.into_iter().map(|(k, _)| k).collect::<FxHashSet<String>>();
    Ok(scan_dirs(&to_scan, &base, container)?)
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
        block_in_place(|| scan_modified(library_path, &base, &container, &dirs_tree))?;
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
        block_in_place(|| scan_everything(library_path, &base, container, &dirs_tree))?;
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
    Display::new(base, db, container, publisher).await?.run().await;
    Ok(())
}
