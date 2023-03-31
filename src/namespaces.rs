use std::error::Error;

use crate::inner_http_client::InnerHttpClient;
use crate::url_constants::URL_NAMESPACES;

pub struct Namespaces<'a> {
    inner_http_client: &'a InnerHttpClient,
}

impl<'a> Namespaces<'a> {
    pub fn new(inner_http_client: &'a InnerHttpClient) -> Self {
        Namespaces { inner_http_client }
    }

    pub async fn get_namespaces(&self, tenant: &str) -> Result<Vec<String>, Box<dyn Error>> {
        let url_path = format!("{}/{}", URL_NAMESPACES, tenant);
        let url = self.inner_http_client.base_url.join(&url_path).unwrap();
        let response = self.inner_http_client.client.get(url).send().await?.text().await?;
        let namespaces: Vec<String> = serde_json::from_str(&response)?;
        Ok(namespaces)
    }
}

#[cfg(test)]
mod tests {
    use crate::PulsarAdmin;

    const PULSAR_HOST: &str = "127.0.0.1";
    const PULSAR_PORT: u16 = 8080;

    #[tokio::test]
    async fn test_get_namespaces() {
        let pulsar_admin = PulsarAdmin::new(PULSAR_HOST, PULSAR_PORT, None);
        let namespaces_api = pulsar_admin.namespaces();
        let namespaces = namespaces_api.get_namespaces("pulsar").await.unwrap();
        assert!(!namespaces.is_empty(), "Namespaces list should not be empty");
    }
}
