use reqwest::{Client, Url};
use crate::SslParams;

pub struct InnerHttpClient {
    pub client: Client,
    pub base_url: Url,
}

impl InnerHttpClient {
    pub fn new(host: &str, port: u16, ssl_params: Option<SslParams>) -> Self {
        let scheme = if ssl_params.is_some() { "https" } else { "http" };
        let base_url = Url::parse(&format!("{}://{}:{}", scheme, host, port)).unwrap();

        let client = match ssl_params {
            Some(_params) => {
                Client::builder()
                    .use_rustls_tls()
                    .build()
                    .unwrap()
            }
            None => Client::new(),
        };

        InnerHttpClient { client, base_url }
    }
}
