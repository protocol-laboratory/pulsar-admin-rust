use std::error::Error;

use crate::inner_http_client::InnerHttpClient;
use crate::url_constants::URL_TENANTS;

pub struct Tenants<'a> {
    inner_http_client: &'a InnerHttpClient,
}

impl<'a> Tenants<'a> {
    pub fn new(inner_http_client: &'a InnerHttpClient) -> Self {
        Tenants { inner_http_client }
    }

    pub async fn get_tenants(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let response = self.inner_http_client.get(URL_TENANTS).await?;
        let tenants: Vec<String> = serde_json::from_str(&response)?;
        Ok(tenants)
    }
}

#[cfg(test)]
mod tests {
    use crate::PulsarAdmin;

    const PULSAR_HOST: &str = "127.0.0.1";
    const PULSAR_PORT: u16 = 8080;

    #[tokio::test]
    async fn test_get_tenants() {
        let pulsar_admin = PulsarAdmin::new(PULSAR_HOST, PULSAR_PORT, None);
        let tenants_api = pulsar_admin.tenants();
        let tenants = tenants_api.get_tenants().await.unwrap();
        assert!(!tenants.is_empty(), "Tenants list should not be empty");
    }
}
