use std::error::Error;

use serde::{Deserialize, Serialize};

use crate::inner_http_client::InnerHttpClient;
use crate::url_constants::URL_LOOKUP;

pub struct Lookup<'a> {
    inner_http_client: &'a InnerHttpClient,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LookupData {
    #[serde(rename = "brokerUrl")]
    pub broker_url: Option<String>,
    #[serde(rename = "brokerUrlTls")]
    pub broker_url_tls: Option<String>,
    #[serde(rename = "httpUrl")]
    pub http_url: Option<String>,
    #[serde(rename = "httpUrlTls")]
    pub http_url_tls: Option<String>,
    #[serde(rename = "nativeUrl")]
    pub native_url: Option<String>,
}

impl<'a> Lookup<'a> {
    pub fn new(inner_http_client: &'a InnerHttpClient) -> Self {
        Lookup { inner_http_client }
    }

    pub async fn lookup_persistent_topic(&self, tenant: &str, namespace: &str, topic: &str) -> Result<LookupData, Box<dyn Error>> {
        let url = format!("{}/persistent/{}/{}/{}", URL_LOOKUP, tenant, namespace, topic);
        let response = self.inner_http_client.get(&url).await?;
        let lookup_data: LookupData = serde_json::from_str(response.as_str())?;
        Ok(lookup_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::{PulsarAdmin, util};

    const PULSAR_HOST: &str = "127.0.0.1";
    const PULSAR_PORT: u16 = 8080;

    #[tokio::test]
    async fn test_lookup_persistent_topic() {
        let pulsar_admin = PulsarAdmin::new(PULSAR_HOST, PULSAR_PORT, None);
        let namespaces = pulsar_admin.namespaces();
        let persistent_topics = pulsar_admin.persistent_topics();
        let lookup = pulsar_admin.lookup();
        let tenant = "public";
        let namespace = util::rand_str(8);
        let topic = util::rand_str(8);
        println!("test_lookup_persistent_topic namespace: {:?} topic: {:?}", namespace, topic);
        let result = namespaces.create_namespace(tenant, namespace.as_str()).await;
        assert!(result.is_ok());
        let result = persistent_topics.create_non_partitioned_topic(tenant, namespace.as_str(), topic.as_str()).await;
        assert!(result.is_ok());
        let result = lookup.lookup_persistent_topic(tenant, namespace.as_str(), topic.as_str()).await;
        assert!(result.is_ok());
        let lookup_data = result.unwrap();
        assert!(lookup_data.broker_url.unwrap().starts_with("pulsar://"));
        let result = persistent_topics.delete_non_partitioned_topic(tenant, namespace.as_str(), topic.as_str()).await;
        assert!(result.is_ok());
        let result = namespaces.delete_namespace(tenant, namespace.as_str()).await;
        assert!(result.is_ok());
    }
}
