use std::collections::HashMap;
use std::error::Error;

use serde::{Deserialize, Serialize};

use crate::inner_http_client::InnerHttpClient;
use crate::url_constants::URL_PERSISTENT;

pub struct PersistentTopics<'a> {
    inner_http_client: &'a InnerHttpClient,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TopicStats {
    #[serde(rename = "msgRateIn")]
    msg_rate_in: f64,
    #[serde(rename = "msgThroughputIn")]
    msg_throughput_in: f64,
    #[serde(rename = "msgRateOut")]
    msg_rate_out: f64,
    #[serde(rename = "msgThroughputOut")]
    msg_throughput_out: f64,
    #[serde(rename = "bytesInCounter")]
    bytes_in_counter: u64,
    #[serde(rename = "msgInCounter")]
    msg_in_counter: u64,
    #[serde(rename = "bytesOutCounter")]
    bytes_out_counter: u64,
    #[serde(rename = "msgOutCounter")]
    msg_out_counter: u64,
    #[serde(rename = "averageMsgSize")]
    average_msg_size: f64,
    #[serde(rename = "msgChunkPublished")]
    msg_chunk_published: bool,
    #[serde(rename = "storageSize")]
    storage_size: u64,
    #[serde(rename = "backlogSize")]
    backlog_size: u64,
    #[serde(rename = "publishRateLimitedTimes")]
    publish_rate_limited_times: u64,
    #[serde(rename = "earliestMsgPublishTimeInBacklogs")]
    earliest_msg_publish_time_in_backlogs: u64,
    #[serde(rename = "offloadedStorageSize")]
    offloaded_storage_size: u64,
    #[serde(rename = "lastOffloadLedgerId")]
    last_offload_ledger_id: u64,
    #[serde(rename = "lastOffloadSuccessTimeStamp")]
    last_offload_success_time_stamp: u64,
    #[serde(rename = "lastOffloadFailureTimeStamp")]
    last_offload_failure_time_stamp: u64,
    #[serde(rename = "ongoingTxnCount")]
    ongoing_txn_count: u64,
    #[serde(rename = "abortedTxnCount")]
    aborted_txn_count: u64,
    #[serde(rename = "committedTxnCount")]
    committed_txn_count: u64,
    #[serde(rename = "publishers")]
    publishers: Vec<String>,
    #[serde(rename = "waitingPublishers")]
    waiting_publishers: u64,
    #[serde(rename = "subscriptions")]
    subscriptions: HashMap<String, String>,
    #[serde(rename = "replication")]
    replication: HashMap<String, String>,
    #[serde(rename = "deduplicationStatus")]
    deduplication_status: String,
    #[serde(rename = "nonContiguousDeletedMessagesRanges")]
    non_contiguous_deleted_messages_ranges: u64,
    #[serde(rename = "nonContiguousDeletedMessagesRangesSerializedSize")]
    non_contiguous_deleted_messages_ranges_serialized_size: u64,
    #[serde(rename = "delayedMessageIndexSizeInBytes")]
    delayed_message_index_size_in_bytes: u64,
    #[serde(rename = "compaction")]
    compaction: Compaction,
    #[serde(rename = "ownerBroker")]
    owner_broker: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Compaction {
    #[serde(rename = "lastCompactionRemovedEventCount")]
    last_compaction_removed_event_count: u64,
    #[serde(rename = "lastCompactionSucceedTimestamp")]
    last_compaction_succeed_timestamp: u64,
    #[serde(rename = "lastCompactionFailedTimestamp")]
    last_compaction_failed_timestamp: u64,
    #[serde(rename = "lastCompactionDurationTimeInMills")]
    last_compaction_duration_time_in_mills: u64,
}

impl<'a> PersistentTopics<'a> {
    pub fn new(inner_http_client: &'a InnerHttpClient) -> Self {
        PersistentTopics { inner_http_client }
    }

    pub async fn create_non_partitioned_topic(
        &self,
        tenant: &str,
        namespace: &str,
        topic: &str,
    ) -> Result<(), Box<dyn Error>> {
        let url_path = format!("{}/{}/{}/{}", URL_PERSISTENT, tenant, namespace, topic);
        self.inner_http_client.put(url_path.as_str(), "".to_string()).await
    }

    pub async fn delete_non_partitioned_topic(
        &self,
        tenant: &str,
        namespace: &str,
        topic: &str,
    ) -> Result<(), Box<dyn Error>> {
        let url_path = format!("{}/{}/{}/{}", URL_PERSISTENT, tenant, namespace, topic);
        self.inner_http_client.delete(url_path.as_str()).await
    }

    pub async fn list_non_partitioned_topic(
        &self,
        tenant: &str,
        namespace: &str,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let url_path = format!("{}/{}/{}", URL_PERSISTENT, tenant, namespace);
        let response = self.inner_http_client.get(url_path.as_str()).await?;
        let topics: Vec<String> = serde_json::from_str(&response)?;
        Ok(topics)
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

    pub async fn topic_stats(
        &self,
        tenant: &str,
        namespace: &str,
        topic: &str,
    ) -> Result<TopicStats, Box<dyn Error>> {
        let url_path = format!("{}/{}/{}/{}/stats", URL_PERSISTENT, tenant, namespace, topic);
        let response =self.inner_http_client.get(url_path.as_str()).await?;
        let topic_stats: TopicStats = serde_json::from_str(response.as_str())?;
        Ok(topic_stats)
    }
}

#[cfg(test)]
mod tests {
    use crate::{PulsarAdmin, util};

    const PULSAR_HOST: &str = "127.0.0.1";
    const PULSAR_PORT: u16 = 8080;

    #[tokio::test]
    async fn test_non_partitioned_topic() {
        let pulsar_admin = PulsarAdmin::new(PULSAR_HOST, PULSAR_PORT, None);
        let namespaces = pulsar_admin.namespaces();
        let persistent_topics = pulsar_admin.persistent_topics();
        let tenant = "public";
        let namespace = util::rand_str(8);
        println!("test_partitioned_topic namespace: {:?}", namespace);
        let result = namespaces.create_namespace(tenant, namespace.as_str()).await;
        assert!(result.is_ok());
        let topic = "test_partitioned_topic";
        let result = persistent_topics.create_non_partitioned_topic(tenant, namespace.as_str(), topic).await;
        assert!(result.is_ok());
        let result = persistent_topics.list_non_partitioned_topic(tenant, namespace.as_str()).await;
        assert!(result.is_ok());
        let topics = result.unwrap();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0], format!("persistent://{}/{}/{}", tenant, namespace, topic));
        let result = persistent_topics.delete_non_partitioned_topic(tenant, namespace.as_str(), topic).await;
        assert!(result.is_ok());
        let result = namespaces.delete_namespace(tenant, namespace.as_str()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_non_partitioned_topic_with_topic_stats() {
        let pulsar_admin = PulsarAdmin::new(PULSAR_HOST, PULSAR_PORT, None);
        let namespaces = pulsar_admin.namespaces();
        let persistent_topics = pulsar_admin.persistent_topics();
        let tenant = "public";
        let namespace = util::rand_str(8);
        println!("test_partitioned_topic namespace: {:?}", namespace);
        let result = namespaces.create_namespace(tenant, namespace.as_str()).await;
        assert!(result.is_ok());
        let topic = "test_partitioned_topic";
        let result = persistent_topics.create_non_partitioned_topic(tenant, namespace.as_str(), topic).await;
        assert!(result.is_ok());
        let result = persistent_topics.list_non_partitioned_topic(tenant, namespace.as_str()).await;
        assert!(result.is_ok());
        let topics = result.unwrap();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0], format!("persistent://{}/{}/{}", tenant, namespace, topic));
        let result = persistent_topics.topic_stats(tenant, namespace.as_str(), topic).await;
        println!("topic_stats: {:?}", result);
        assert!(result.is_ok());
        let result = persistent_topics.delete_non_partitioned_topic(tenant, namespace.as_str(), topic).await;
        assert!(result.is_ok());
        let result = namespaces.delete_namespace(tenant, namespace.as_str()).await;
        assert!(result.is_ok());
    }

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
