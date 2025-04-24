use strum::IntoEnumIterator;
use strum_macros::EnumIter;

#[derive(EnumIter, PartialEq)]
#[repr(i16)]
pub enum ApiKeys {
    Invalid = 0,
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

impl From<i16> for ApiKeys {
    fn from(value: i16) -> Self {
        match value {
            18 => Self::ApiVersions,
            75 => Self::DescribeTopicPartitions,
            _ => Self::Invalid,
        }
    }
}

impl ApiKeys {
    pub fn get_vec() -> Vec<ApiKeys> {
        ApiKeys::iter().filter(|x| *x != ApiKeys::Invalid).collect()
    }

    pub fn versions(&self) -> (i16, i16) {
        match self {
            Self::ApiVersions => (0, 4),
            Self::DescribeTopicPartitions => (0, 0),
            Self::Invalid => (0, 0),
        }
    }
}
