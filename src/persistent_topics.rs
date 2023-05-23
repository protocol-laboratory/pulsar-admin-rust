use std::error::Error;
use crate::inner_http_client::InnerHttpClient;
use crate::url_constants::URL_PERSISTENT;

pub struct PersistentTopics<'a> {
    inner_http_client: &'a InnerHttpClient,
}

impl<'a> PersistentTopics<'a> {
    pub fn new(inner_http_client: &'a InnerHttpClient) -> Self {
        PersistentTopics { inner_http_client }
    }

    pub async fn create_partitioned_topic(
        &self,
        tenant: &str,
        namespace: &str,
        topic: &str,
        num_partitions: i32,
    ) -> Result<(), Box<dyn Error>> {
        let url_path = format!("{}/{}/{}/{}/partitions", URL_PERSISTENT, tenant, namespace, topic);
        self.inner_http_client.put(url_path.as_str(), num_partitions.to_string()).await
    }

    pub async fn delete_partitioned_topic(
        &self,
        tenant: &str,
        namespace: &str,
        topic: &str,
    ) -> Result<(), Box<dyn Error>> {
        let url_path = format!("{}/{}/{}/{}/partitions", URL_PERSISTENT, tenant, namespace, topic);
        self.inner_http_client.delete(url_path.as_str()).await
    }

    pub async fn list_partitioned_topic(
        &self,
        tenant: &str,
        namespace: &str,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let url_path = format!("{}/{}/{}/partitioned", URL_PERSISTENT, tenant, namespace);
        let response = self.inner_http_client.get(url_path.as_str()).await?;
        let topics: Vec<String> = serde_json::from_str(&response)?;
        Ok(topics)
    }
}

#[cfg(test)]
mod tests {
    use crate::{PulsarAdmin, util};

    const PULSAR_HOST: &str = "127.0.0.1";
    const PULSAR_PORT: u16 = 8080;

    #[tokio::test]
    async fn test_partitioned_topic() {
        let pulsar_admin = PulsarAdmin::new(PULSAR_HOST, PULSAR_PORT, None);
        let namespaces = pulsar_admin.namespaces();
        let persistent_topics = pulsar_admin.persistent_topics();
        let tenant = "public";
        let namespace = util::rand_str(8);
        println!("test_partitioned_topic namespace: {:?}", namespace);
        let result = namespaces.create_namespace(tenant, namespace.as_str()).await;
        assert!(result.is_ok());
        let topic = "test_partitioned_topic";
        let num_partitions = 3;
        let result = persistent_topics.create_partitioned_topic(tenant, namespace.as_str(), topic, num_partitions).await;
        assert!(result.is_ok());
        let result = persistent_topics.list_partitioned_topic(tenant, namespace.as_str()).await;
        assert!(result.is_ok());
        let topics = result.unwrap();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0], format!("persistent://{}/{}/{}", tenant, namespace, topic));
        let result = persistent_topics.delete_partitioned_topic(tenant, namespace.as_str(), topic).await;
        assert!(result.is_ok());
        let result = namespaces.delete_namespace(tenant, namespace.as_str()).await;
        assert!(result.is_ok());
    }
}
