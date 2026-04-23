// secret_store.rs

pub struct SecretStoreAdapter;

impl SecretStoreAdapter {
    pub fn get_secret(&self, _key: String) -> String {
        "secret".into()
    }
}
