use crate::inner_http_client::InnerHttpClient;
use crate::tenants::Tenants;
use crate::namespaces::Namespaces;

pub struct PulsarAdmin {
    inner_http_client: InnerHttpClient,
}

impl PulsarAdmin {
    pub fn new(host: &str, port: u16, ssl_params: Option<SslParams>) -> Self {
        let inner_http_client = InnerHttpClient::new(host, port, ssl_params);
        PulsarAdmin { inner_http_client }
    }

    pub fn tenants(&self) -> Tenants {
        Tenants::new(&self.inner_http_client)
    }

    pub fn namespaces(&self) -> Namespaces {
        Namespaces::new(&self.inner_http_client)
    }
}

pub struct SslParams {
}
