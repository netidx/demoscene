use anyhow::{anyhow, bail, Result};
use futures::future;
use fxhash::{FxHashMap, FxHashSet};
use gstreamer::prelude::*;
use log::{error, info};
use netidx::{
    chars::Chars,
    pack::Pack,
    path::Path as NPath,
    publisher::{Publisher, Value},
    utils::pack,
};
use netidx_container::{Container, Datum, Db, Params as ContainerParams, Txn};
use netidx_protocols::rpc::server as rpc;
use netidx_tools::ClientParams;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::Read,
    path::PathBuf,
    sync::Arc,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH}, hash::Hash,
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
                    let file = format!("{}/file", &*track);
                    let file = match db.lookup(dbg!(&file)) {
                        Ok(Some(Datum::Data(Value::String(f)))) => dbg!(f),
                        Ok(Some(_)) | Ok(None) | Err(_) => {
                            return Self::err("track not found")
                        }
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
    init_library(&args.library_path, base, &container).await?;
    let player = Player::new();
    let _rpcs = RpcApi::new(api_path, &publisher, player, db)?;
    Ok(future::pending().await) // don't quit until we are killed
}
