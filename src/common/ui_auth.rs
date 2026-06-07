use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::common::strings::ct_eq;
use crate::settings::ServiceConfig;

pub const DEFAULT_USERNAME: &str = "admin";
pub const DEFAULT_PASSWORD: &str = "qdrant";
pub const UI_AUTH_FILE: &str = "ui_auth.json";
pub const SESSION_COOKIE: &str = "qdrant_ui_session";
pub const SESSION_DURATION_SECS: u64 = 60 * 60 * 24 * 7;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredUiAuth {
    username: String,
    password_hash: String,
    salt: String,
    session_secret: String,
}

#[derive(Debug, thiserror::Error)]
pub enum UiAuthError {
    #[error("Invalid username or password")]
    InvalidCredentials,
    #[error("Invalid session")]
    InvalidSession,
    #[error("UI authentication is disabled")]
    Disabled,
    #[error("{0}")]
    Internal(#[from] anyhow::Error),
}

#[derive(Clone)]
pub struct UiAuthState {
    inner: Arc<RwLock<StoredUiAuth>>,
    path: PathBuf,
    pub enabled: bool,
}

impl UiAuthState {
    pub fn disabled() -> Self {
        Self {
            inner: Arc::new(RwLock::new(
                StoredUiAuth::new(DEFAULT_USERNAME.into(), DEFAULT_PASSWORD.into())
                    .expect("default UI auth credentials must be valid"),
            )),
            path: PathBuf::from("."),
            enabled: false,
        }
    }

    pub fn load_or_create(storage_path: &Path, service: &ServiceConfig) -> Result<Self> {
        let enabled = service.ui_auth_enabled();
        let path = storage_path.join(UI_AUTH_FILE);

        if path.exists() {
            let stored: StoredUiAuth = serde_json::from_str(
                &fs_err::read_to_string(&path)
                    .with_context(|| format!("Failed to read {}", path.display()))?,
            )
            .context("Failed to parse UI auth file")?;
            log::info!(
                "Loaded Web UI credentials for user '{}'",
                stored.username
            );
            return Ok(Self {
                inner: Arc::new(RwLock::new(stored)),
                path,
                enabled,
            });
        }

        let username = service
            .ui_username
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_USERNAME.to_string());
        let password = service
            .ui_password
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_PASSWORD.to_string());

        if username == DEFAULT_USERNAME && password == DEFAULT_PASSWORD {
            log::warn!(
                "Web UI is using default credentials ({DEFAULT_USERNAME} / {DEFAULT_PASSWORD}). \
                 Change them at /dashboard/login after signing in, or set \
                 QDRANT__SERVICE__UI_USERNAME and QDRANT__SERVICE__UI_PASSWORD before starting."
            );
        }

        let stored = StoredUiAuth::new(username, password)?;
        Self::write_file(&path, &stored)?;
        log::info!(
            "Initialized Web UI credentials for user '{}'",
            stored.username
        );

        Ok(Self {
            inner: Arc::new(RwLock::new(stored)),
            path,
            enabled,
        })
    }

    pub fn username(&self) -> String {
        self.inner.read().expect("ui auth lock poisoned").username.clone()
    }

    pub fn verify_credentials(&self, username: &str, password: &str) -> bool {
        let stored = self.inner.read().expect("ui auth lock poisoned");
        stored.username == username && stored.verify_password(password)
    }

    pub fn create_session_token(&self) -> String {
        let stored = self.inner.read().expect("ui auth lock poisoned");
        let exp = chrono::Utc::now().timestamp() + SESSION_DURATION_SECS as i64;
        let payload = format!("{}:{}", stored.username, exp);
        let sig = sign_message(&stored.session_secret, payload.as_bytes());
        format!("{payload}.{}", to_hex(&sig))
    }

    pub fn validate_session_token(&self, token: &str) -> bool {
        let Some((payload, sig_hex)) = token.rsplit_once('.') else {
            return false;
        };
        let Some(sig) = from_hex(sig_hex) else {
            return false;
        };
        let Some((username, exp)) = payload.rsplit_once(':') else {
            return false;
        };
        let Ok(exp) = exp.parse::<i64>() else {
            return false;
        };
        if chrono::Utc::now().timestamp() > exp {
            return false;
        }

        let stored = self.inner.read().expect("ui auth lock poisoned");
        if stored.username != username {
            return false;
        }

        let expected = sign_message(&stored.session_secret, payload.as_bytes());
        ct_eq_bytes(&expected, &sig)
    }

    pub fn update_credentials(
        &self,
        current_password: &str,
        new_username: &str,
        new_password: &str,
    ) -> Result<(), UiAuthError> {
        if !self.enabled {
            return Err(UiAuthError::Disabled);
        }
        if new_username.is_empty()
            || new_password.is_empty()
            || new_username.contains(':')
        {
            return Err(UiAuthError::InvalidCredentials);
        }

        let mut stored = self.inner.write().expect("ui auth lock poisoned");
        if !stored.verify_password(current_password) {
            return Err(UiAuthError::InvalidCredentials);
        }

        let updated = StoredUiAuth::new(new_username.to_string(), new_password.to_string())?;
        Self::write_file(&self.path, &updated)?;
        *stored = updated;
        Ok(())
    }

    fn write_file(path: &Path, stored: &StoredUiAuth) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs_err::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(stored)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.write_all(json.as_bytes())?;
        file.write_all(b"\n")?;
        Ok(())
    }
}

impl StoredUiAuth {
    fn new(username: String, password: String) -> Result<Self> {
        if username.is_empty() || password.is_empty() || username.contains(':') {
            anyhow::bail!("Invalid UI auth username or password");
        }

        let salt = uuid::Uuid::new_v4().into_bytes().to_vec();
        let mut session_secret = uuid::Uuid::new_v4().into_bytes().to_vec();
        session_secret.extend(uuid::Uuid::new_v4().into_bytes());

        Ok(Self {
            username,
            password_hash: hash_password(&salt, password.as_bytes()),
            salt: to_hex(&salt),
            session_secret: to_hex(&session_secret),
        })
    }

    fn verify_password(&self, password: &str) -> bool {
        let Some(salt) = from_hex(&self.salt) else {
            return false;
        };
        let expected = hash_password(&salt, password.as_bytes());
        ct_eq(&expected, &self.password_hash)
    }
}

fn hash_password(salt: &[u8], password: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(salt);
    hasher.update(password);
    to_hex(&hasher.finalize())
}

fn sign_message(secret_hex: &str, message: &[u8]) -> Vec<u8> {
    let secret = from_hex(secret_hex).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(secret);
    hasher.update(message);
    hasher.finalize().to_vec()
}

fn ct_eq_bytes(lhs: &[u8], rhs: &[u8]) -> bool {
    constant_time_eq::constant_time_eq(lhs, rhs)
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn from_hex(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|idx| u8::from_str_radix(&hex[idx..idx + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credentials_roundtrip() {
        let stored = StoredUiAuth::new("alice".into(), "secret".into()).unwrap();
        assert!(stored.verify_password("secret"));
        assert!(!stored.verify_password("wrong"));
    }

    #[test]
    fn test_session_token() {
        let state = UiAuthState {
            inner: Arc::new(RwLock::new(
                StoredUiAuth::new("alice".into(), "secret".into()).unwrap(),
            )),
            path: PathBuf::from("/tmp/ui_auth.json"),
            enabled: true,
        };
        let token = state.create_session_token();
        assert!(state.validate_session_token(&token));
        assert!(!state.validate_session_token("invalid.token"));
    }
}
