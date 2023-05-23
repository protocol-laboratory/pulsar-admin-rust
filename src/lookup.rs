use crate::inner_http_client::InnerHttpClient;

pub struct Lookup<'a> {
    inner_http_client: &'a InnerHttpClient,
}

impl<'a> Lookup<'a> {
    pub fn new(inner_http_client: &'a InnerHttpClient) -> Self {
        Lookup { inner_http_client }
    }
}
