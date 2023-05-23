use std::error::Error;
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

    pub async fn get(&self, path: &str) -> Result<String, Box<dyn Error>> {
        let url = self.base_url.join(path)?;
        let resp = self.client.get(url).send().await?;
        match resp.error_for_status() {
            Ok(resp) => Ok(resp.text().await?),
            Err(e) => Err(Box::new(e)),
        }
    }

    pub async fn put(&self, path: &str, body: String) -> Result<(), Box<dyn Error>> {
        let url = self.base_url.join(path)?;
        let resp = self.client.put(url).header("Content-Type", "application/json").body(body).send().await?;
        match resp.error_for_status() {
            Ok(_) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }

    pub async fn delete(&self, path: &str) -> Result<(), Box<dyn Error>> {
        let url = self.base_url.join(path)?;
        let resp = self.client.delete(url).send().await?;
        match resp.error_for_status() {
            Ok(_) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }
}
