/**
 * This module acts as a shim between serde_json and simd-json to allow for higher performance JSON
 * parsing on SIMD-capable architectures
 */


use serde;
use serde_json;
#[cfg(feature = "simd")]
use simd_json;

pub fn from_str<'a, S: serde::Deserialize<'a>>(buffer: &'a str) -> Result<S, serde_json::error::Error> {
    #[cfg(feature = "simd")]
    {
        simd_json::serde::from_str::<S>(buffer)
    }

    #[cfg(not(feature="simd"))]
    {
        serde_json::from_str::<S>(buffer)
    }
}

pub fn to_string<S: serde::Serialize>(value: &S) -> Result<String, serde_json::error::Error> {
    serde_json::to_string::<S>(value)
}
