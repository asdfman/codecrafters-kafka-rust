pub enum ApiKeys {
    Invalid = 0,
    ApiVersions = 18,
}

impl From<i16> for ApiKeys {
    fn from(value: i16) -> Self {
        match value {
            18 => Self::ApiVersions,
            _ => Self::Invalid,
        }
    }
}
