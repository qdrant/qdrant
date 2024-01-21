use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum SnapshotFile {
    Normal {
        name: String,
        collection: Option<String>,
        shard: Option<u32>,
    },
    OutOfPlace(PathBuf),
}

impl SnapshotFile {
    pub fn new_full(name: impl Into<String>) -> Self {
        SnapshotFile::Normal {
            name: name.into(),
            collection: None,
            shard: None,
        }
    }

    pub fn new_collection(name: impl Into<String>, collection: impl Into<String>) -> Self {
        SnapshotFile::Normal {
            name: name.into(),
            collection: Some(collection.into()),
            shard: None,
        }
    }

    pub fn new_shard(
        name: impl Into<String>,
        collection: impl Into<String>,
        shard: impl Into<u32>,
    ) -> Self {
        SnapshotFile::Normal {
            name: name.into(),
            collection: Some(collection.into()),
            shard: Some(shard.into()),
        }
    }

    pub fn new_oop(
        path: impl Into<PathBuf>,
    ) -> Self {
        SnapshotFile::OutOfPlace(path.into())
    }

    pub fn name(&self) -> String {
        match &self {
            Self::Normal { name, .. } => name.clone(),
            Self::OutOfPlace(path) => path.file_name().unwrap().to_string_lossy().to_string(),
        }
    }

    pub fn collection(&self) -> Option<String> {
        if let Self::Normal { collection, .. } = &self {
            collection.clone()
        } else {
            None
        }
    }

    pub fn has_collection(&self) -> bool {
        if let Self::Normal { collection, .. } = &self {
            collection.is_some()
        } else {
            false
        }
    }

    pub(super) fn get_directory(&self, base: impl Into<PathBuf>) -> PathBuf {
        match &self {
            Self::Normal { collection, shard, .. } => {
                let path: PathBuf = base.into();
    
                let path =
                    if let Some(collection) = &collection {
                        path.join(collection)
                    } else {
                        path
                    };
    
                if let Some(shard) = &shard {
                    path.join(format!("shards/{}", shard))
                } else {
                    path
                }
            },
            Self::OutOfPlace(path) => path.parent().unwrap().to_path_buf(),
        }
    }

    pub fn get_path(&self, base: impl Into<PathBuf>) -> PathBuf {
        let path = self.get_directory(base);
        path.join(self.name())
    }

    pub fn get_checksum_path(&self, base: impl Into<PathBuf>) -> PathBuf {
        let path = self.get_directory(base);
        path.join(self.name() + ".checksum")
    }
}
