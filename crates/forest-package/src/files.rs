use crate::{Error, MANIFEST_FILE, MANIFEST_TEMP, error::io};
use cap_std::fs::Dir;
use forest_protocol::{Schema, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path};

pub(crate) fn relative(path: &str) -> Result<(), Error> {
    if path.is_empty()
        || path.contains(['\\', ':'])
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(Error::InvalidPath(path.into()));
    }
    Ok(())
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) struct Files {
    pub directory: Dir,
    pub checksums: BTreeMap<String, String>,
    cache: BTreeMap<String, Vec<u8>>,
}

impl Files {
    pub fn new(directory: Dir, checksums: BTreeMap<String, String>) -> Result<Self, Error> {
        for (path, value) in &checksums {
            relative(path)?;
            if path == MANIFEST_FILE || path == MANIFEST_TEMP {
                return Err(Error::InvalidPath(path.clone()));
            }
            if value.len() != 64 || !value.bytes().all(|c| c.is_ascii_hexdigit()) {
                return Err(Error::InvalidChecksum {
                    path: path.clone(),
                    value: value.clone(),
                });
            }
        }
        Ok(Self {
            directory,
            checksums,
            cache: BTreeMap::new(),
        })
    }

    pub fn require(&self, path: &str) -> Result<(), Error> {
        relative(path)?;
        if self.checksums.contains_key(path) {
            Ok(())
        } else {
            Err(Error::MissingChecksum(path.into()))
        }
    }

    pub fn read(&mut self, path: &str) -> Result<&[u8], Error> {
        self.require(path)?;
        if !self.cache.contains_key(path) {
            let bytes = self.directory.read(path).map_err(|error| io(path, error))?;
            let actual = digest(&bytes);
            let expected = &self.checksums[path];
            if !expected.eq_ignore_ascii_case(&actual) {
                return Err(Error::ChecksumMismatch {
                    path: path.into(),
                    expected: expected.clone(),
                    actual,
                });
            }
            self.cache.insert(path.into(), bytes);
        }
        Ok(&self.cache[path])
    }

    fn text(&mut self, path: &str) -> Result<&str, Error> {
        std::str::from_utf8(self.read(path)?).map_err(|source| Error::Utf8 {
            path: path.into(),
            source,
        })
    }

    pub fn schema(&mut self, path: &str) -> Result<Schema, Error> {
        forest_serde::parser::parse_schema(self.text(path)?).map_err(|source| Error::Schema {
            path: path.into(),
            source,
        })
    }

    pub fn value(&mut self, path: &str) -> Result<Value, Error> {
        forest_serde::parser::parse_value(self.text(path)?).map_err(|source| Error::Value {
            path: path.into(),
            source,
        })
    }

    pub fn module(mut self, path: &str) -> Result<Vec<u8>, Error> {
        for file in self.checksums.keys().cloned().collect::<Vec<_>>() {
            self.read(&file)?;
        }
        Ok(self
            .cache
            .remove(path)
            .expect("declared module was verified"))
    }
}

pub(crate) fn put(directory: &Dir, path: &str, bytes: &[u8]) -> Result<(), Error> {
    relative(path)?;
    if let Some(parent) = Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        directory
            .create_dir_all(parent)
            .map_err(|error| io(parent, error))?;
    }
    directory
        .write(path, bytes)
        .map_err(|error| io(path, error))
}

pub(crate) fn checksums(directory: &Dir) -> Result<BTreeMap<String, String>, Error> {
    fn walk(
        directory: &Dir,
        prefix: &str,
        result: &mut BTreeMap<String, String>,
    ) -> Result<(), Error> {
        for entry in directory.entries().map_err(|error| io(prefix, error))? {
            let entry = entry.map_err(|error| io(prefix, error))?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| Error::NonUtf8Path(name.clone().into()))?;
            let path = format!("{prefix}{name}");
            if path == MANIFEST_FILE || path == MANIFEST_TEMP {
                continue;
            }
            relative(&path)?;
            let kind = entry.file_type().map_err(|error| io(&path, error))?;
            if kind.is_dir() {
                let child = directory.open_dir(name).map_err(|error| io(&path, error))?;
                walk(&child, &format!("{path}/"), result)?;
            } else if kind.is_file() {
                result.insert(
                    path.clone(),
                    digest(&directory.read(name).map_err(|error| io(&path, error))?),
                );
            } else {
                return Err(Error::UnsupportedEntry(path));
            }
        }
        Ok(())
    }
    let mut result = BTreeMap::new();
    walk(directory, "", &mut result)?;
    Ok(result)
}
