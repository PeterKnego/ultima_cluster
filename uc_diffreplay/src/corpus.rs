// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! A corpus is a `uc2ctl backup` artifact plus a `CORPUS` manifest naming the
//! row, the origin P (exclusive frontier — the artifact to install), the end
//! Q, and the FSM version that built the artifact (spec §6.1).

use std::path::{Path, PathBuf};

use anyhow::{Context, bail};

const MANIFEST: &str = "CORPUS";
const FORMAT: &str = "uc2-corpus-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorpusManifest {
    pub app_id: String,
    pub row: u8,
    pub origin: u64,
    pub end: u64,
    pub version: u32,
}

impl CorpusManifest {
    /// `key=value` lines, the same hand-formatted style `uc_node::backup`'s
    /// `MANIFEST` uses (no serde on this file: it must stay greppable).
    pub fn write(&self, dir: &Path) -> anyhow::Result<()> {
        let text = format!(
            "format={FORMAT}\napp_id={}\nrow={}\norigin={}\nend={}\nversion={:#x}\n",
            self.app_id, self.row, self.origin, self.end, self.version
        );
        std::fs::write(dir.join(MANIFEST), text).context("write CORPUS")
    }

    pub fn read(dir: &Path) -> anyhow::Result<CorpusManifest> {
        let text = std::fs::read_to_string(dir.join(MANIFEST)).context("read CORPUS")?;
        let mut app_id = None;
        let (mut row, mut origin, mut end, mut version) = (None, None, None, None);
        for line in text.lines() {
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            match k {
                "format" if v != FORMAT => bail!("CORPUS format {v:?}, expected {FORMAT:?}"),
                "app_id" => app_id = Some(v.to_string()),
                "row" => row = Some(v.parse()?),
                "origin" => origin = Some(v.parse()?),
                "end" => end = Some(v.parse()?),
                "version" => version = Some(u32::from_str_radix(v.trim_start_matches("0x"), 16)?),
                _ => {}
            }
        }
        Ok(CorpusManifest {
            app_id: app_id.context("CORPUS: app_id")?,
            row: row.context("CORPUS: row")?,
            origin: origin.context("CORPUS: origin")?,
            end: end.context("CORPUS: end")?,
            version: version.context("CORPUS: version")?,
        })
    }
}

#[derive(Debug)]
pub struct Corpus {
    pub dir: PathBuf,
    pub manifest: CorpusManifest,
}

impl Corpus {
    pub fn open(dir: &Path) -> anyhow::Result<Corpus> {
        let manifest = CorpusManifest::read(dir)?;
        let c = Corpus {
            dir: dir.to_path_buf(),
            manifest,
        };
        if !c.journal_dir().is_dir() {
            bail!("corpus has no journal/ at {}", c.journal_dir().display());
        }
        Ok(c)
    }

    pub fn journal_dir(&self) -> PathBuf {
        self.dir.join("journal")
    }

    pub fn artifact(&self) -> PathBuf {
        self.dir
            .join("snapshots")
            .join(self.manifest.row.to_string())
            .join(format!("snap-{}.ultsnap", self.manifest.origin))
    }

    /// `uc2ctl backup` into `out`, then stamp the CORPUS manifest. The node
    /// must be stopped (the backup verbs are offline — `uc_node::backup`).
    pub fn export(
        instance_dir: &Path,
        app_id: &str,
        row: u8,
        origin: u64,
        end: u64,
        version: u32,
        out: &Path,
    ) -> anyhow::Result<Corpus> {
        uc_node::backup::backup_instance(instance_dir, out)
            .map_err(|e| anyhow::anyhow!("backup: {e}"))?;
        let m = CorpusManifest {
            app_id: app_id.into(),
            row,
            origin,
            end,
            version,
        };
        m.write(out)?;
        let c = Corpus::open(out)?;
        if !c.artifact().is_file() {
            bail!(
                "no artifact for row {row} at origin {origin}: {}",
                c.artifact().display()
            );
        }
        Ok(c)
    }

    /// Spec §6.1 `--around <pos>`: the newest complete artifact at or below
    /// `pos` is the origin; the end is `pos` itself (the caller widens it if
    /// the trigger needs a tail).
    pub fn export_around(
        instance_dir: &Path,
        app_id: &str,
        row: u8,
        pos: u64,
        version: u32,
        out: &Path,
    ) -> anyhow::Result<Corpus> {
        let store = uc_service::snapshots::SnapshotStore::open(instance_dir, row)?;
        let (origin, _) = store
            .newest(pos)?
            .with_context(|| format!("no complete artifact for row {row} at or below {pos}"))?;
        Corpus::export(instance_dir, app_id, row, origin, pos, version, out)
    }
}
