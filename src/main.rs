use anyhow::{anyhow, bail, Result};
use futures::future;
use fxhash::{FxHashMap, FxHashSet};
use netidx::{pack::Pack, path::Path as NPath, publisher::Value, utils::pack};
use netidx_container::{Container, Params as ContainerParams, Txn};
use netidx_tools::ClientParams;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::Read,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
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

fn set_str(
    txn: &mut Txn,
    hash: &md5::Digest,
    base: &NPath,
    name: &str,
    val: Option<&str>,
) {
    let key = base.append(&format!("{:x}/{}", hash, name));
    let val = match val {
        None => Value::Null,
        Some(val) => Value::from(String::from(val)),
    };
    txn.set_data(true, key, val, None);
}

fn scan_track(txn: &mut Txn, path: &str, base: &NPath) -> Result<()> {
    use lofty::{read_from_path, Accessor};
    let hash = hash_file(&path)?;
    let tagged = read_from_path(path, false)?;
    set_str(txn, &hash, base, "file", Some(path));
    if let Some(tag) = tagged.primary_tag() {
        set_str(txn, &hash, base, "artist", tag.artist());
        set_str(txn, &hash, base, "title", tag.title());
        set_str(txn, &hash, base, "album", tag.album());
        set_str(txn, &hash, base, "genre", tag.genre());
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
/*
fn scan_everything(
    path: &str,
    base: &NPath,
    db: &Db,
    dirs_tree: &sled::Tree,
) -> Result<()> {
    let dirs = dirs(path)?;
    let _ = dirs_to_scan(&dirs, &dirs_tree)?; // store the dirs mod timestamps
    let to_scan = dirs.into_iter().map(|(k, _)| k).collect::<FxHashSet<String>>();
    Ok(scan_dirs(&to_scan, &base, &db)?)
}
*/

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Params::from_args();
    let base = NPath::from(args.base);
    let (config, desired_auth) = args.client_params.load();
    let container = Container::start(config, desired_auth, args.container_config).await?;
    //let publisher = container.publisher().await?;
    let db = container.db().await?;
    let dirs_tree = db.open_tree("dirs")?;
    block_in_place(|| scan_modified(&args.library_path, &base, &container, &dirs_tree))?;
    Ok(future::pending().await) // don't quit until we are killed
}
