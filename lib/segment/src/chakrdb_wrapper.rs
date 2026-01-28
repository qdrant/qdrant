use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};

// Re-export constants to match RocksDB wrapper
pub const DB_VECTOR_CF: &str = "vector";
pub const DB_PAYLOAD_CF: &str = "payload";
pub const DB_MAPPING_CF: &str = "mapping";
pub const DB_VERSIONS_CF: &str = "version";
pub const DB_DEFAULT_CF: &str = "default";

// ChakrDB wrapper using the chakrdb-wrapper crate
#[cfg(feature = "chakrdb")]
use chakrdb_wrapper::{ChakrDbClient, ChakrDbOptions};

#[derive(Clone)]
#[cfg(feature = "chakrdb")]
pub struct DatabaseColumnWrapper {
    client: Arc<RwLock<ChakrDbClient>>,
    column_name: String,
}

#[cfg(feature = "chakrdb")]
impl Debug for DatabaseColumnWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseColumnWrapper")
            .field("column_name", &self.column_name)
            .finish()
    }
}

#[cfg(feature = "chakrdb")]
pub struct DatabaseColumnIterator {
    // TODO: Implement iterator using ChakrDB Scan API
    // For now, this is a placeholder
    _phantom: std::marker::PhantomData<()>,
}

#[cfg(feature = "chakrdb")]
pub struct LockedDatabaseColumnWrapper<'a> {
    guard: parking_lot::RwLockReadGuard<'a, ChakrDbClient>,
    column_name: &'a str,
}

/// Open a ChakrDB database
#[cfg(feature = "chakrdb")]
pub fn open_db<T: AsRef<str>>(
    path: &Path,
    vector_paths: &[T],
) -> Result<Arc<RwLock<ChakrDbClient>>, OperationError> {
    let mut column_families = vec![DB_PAYLOAD_CF, DB_DEFAULT_CF];
    
    // Check if database exists
    let exists = check_db_exists(path);
    
    // Add column families that may exist
    if exists {
        column_families.extend([DB_MAPPING_CF, DB_VERSIONS_CF]);
    }
    
    // Add vector column families
    for vector_path in vector_paths {
        column_families.push(vector_path.as_ref());
    }
    
    // Create ChakrDB options
    let options = ChakrDbOptions {
        path: path.to_string_lossy().to_string(),
        cache_size: 10 * 1024 * 1024, // 10 MB
        max_open_files: 256,
    };
    
    // Open embedded ChakrDB client
    let client = ChakrDbClient::new_embedded(path, options)
        .map_err(|e| OperationError::service_error(format!("ChakrDB open error: {}", e)))?;
    
    // Create column families if they don't exist
    // Note: This would need to be done via the client API
    // For now, we assume they're created automatically
    
    Ok(Arc::new(RwLock::new(client)))
}

#[cfg(feature = "chakrdb")]
pub fn check_db_exists(path: &Path) -> bool {
    // Check for ChakrDB-specific marker files
    // Similar to RocksDB's CURRENT file check
    // TODO: Implement actual file existence check for ChakrDB
    path.exists()
}

#[cfg(feature = "chakrdb")]
pub fn open_db_with_existing_cf(path: &Path) -> Result<Arc<RwLock<ChakrDbClient>>, OperationError> {
    // For now, use the same logic as open_db
    // TODO: Implement proper handling of existing column families
    open_db(path, &[])
}

#[cfg(feature = "chakrdb")]
impl DatabaseColumnWrapper {
    pub fn new(client: Arc<RwLock<ChakrDbClient>>, column_name: &str) -> Self {
        Self {
            client,
            column_name: column_name.to_string(),
        }
    }

    pub fn put<K, V>(&self, key: K, value: V) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let client = self.client.read();
        client
            .put(&self.column_name, key.as_ref(), value.as_ref())
            .map_err(|err| OperationError::service_error(format!("ChakrDB put error: {}", err)))?;
        Ok(())
    }

    pub fn get<K>(&self, key: K) -> OperationResult<Vec<u8>>
    where
        K: AsRef<[u8]>,
    {
        let client = self.client.read();
        client
            .get(&self.column_name, key.as_ref())
            .map_err(|err| OperationError::service_error(format!("ChakrDB get error: {}", err)))?
            .ok_or_else(|| OperationError::service_error("ChakrDB get error: key not found"))
    }

    pub fn get_opt<K>(&self, key: K) -> OperationResult<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let client = self.client.read();
        client
            .get(&self.column_name, key.as_ref())
            .map_err(|err| OperationError::service_error(format!("ChakrDB get error: {}", err)))
    }

    pub fn get_pinned<T, F>(&self, key: &[u8], f: F) -> OperationResult<Option<T>>
    where
        F: FnOnce(&[u8]) -> T,
    {
        // For ChakrDB, we'll read the value and apply the function
        // This is not as efficient as RocksDB's pinned reads, but functional
        match self.get_opt(key)? {
            Some(value) => Ok(Some(f(&value))),
            None => Ok(None),
        }
    }

    pub fn remove<K>(&self, key: K) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
    {
        let client = self.client.read();
        client
            .delete(&self.column_name, key.as_ref())
            .map_err(|err| OperationError::service_error(format!("ChakrDB delete error: {}", err)))?;
        Ok(())
    }

    pub fn lock_db(&self) -> LockedDatabaseColumnWrapper<'_> {
        LockedDatabaseColumnWrapper {
            guard: self.client.read(),
            column_name: &self.column_name,
        }
    }

    pub fn flusher(&self) -> Flusher {
        let client = self.client.clone();
        let column_name = self.column_name.clone();
        Box::new(move || {
            let client_guard = client.read();
            client_guard
                .flush(&column_name)
                .map_err(|err| OperationError::service_error(format!("ChakrDB flush error: {}", err)))?;
            Ok(())
        })
    }

    pub fn create_column_family_if_not_exists(&self) -> OperationResult<()> {
        // For ChakrDB, column families are created automatically on first use
        // or can be created explicitly via the client API
        // TODO: Implement explicit column family creation if needed
        Ok(())
    }

    pub fn recreate_column_family(&self) -> OperationResult<()> {
        self.remove_column_family()?;
        self.create_column_family_if_not_exists()
    }

    pub fn remove_column_family(&self) -> OperationResult<()> {
        // TODO: Implement column family removal via ChakrDB API
        // For now, this is a no-op
        Ok(())
    }

    pub fn has_column_family(&self) -> OperationResult<bool> {
        // For ChakrDB, we assume column families exist if the client is initialized
        // TODO: Implement proper column family existence check
        Ok(true)
    }

    pub fn get_database(&self) -> Arc<RwLock<ChakrDbClient>> {
        self.client.clone()
    }

    pub fn get_column_name(&self) -> &str {
        &self.column_name
    }

    /// Get the size of the storage in bytes
    ///
    /// TODO: Implement actual storage size calculation for ChakrDB
    pub fn get_storage_size_bytes(&self) -> OperationResult<usize> {
        // Placeholder - would need to query ChakrDB for actual size
        Ok(0)
    }
}

#[cfg(feature = "chakrdb")]
impl LockedDatabaseColumnWrapper<'_> {
    pub fn iter(&self) -> OperationResult<DatabaseColumnIterator> {
        // TODO: Implement iterator using ChakrDB Scan API
        // For now, return a placeholder
        Ok(DatabaseColumnIterator {
            _phantom: std::marker::PhantomData,
        })
    }
}

#[cfg(feature = "chakrdb")]
impl Iterator for DatabaseColumnIterator {
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: Implement iterator using ChakrDB Scan API
        // For now, return None
        None
    }
}
