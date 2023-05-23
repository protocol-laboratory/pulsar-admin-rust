use crate::inner_http_client::InnerHttpClient;

pub struct PersistentTopics<'a> {
    inner_http_client: &'a InnerHttpClient,
}

impl<'a> PersistentTopics<'a> {
    pub fn new(inner_http_client: &'a InnerHttpClient) -> Self {
        PersistentTopics { inner_http_client }
    }
}
