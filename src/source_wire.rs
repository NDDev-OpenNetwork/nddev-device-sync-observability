// Generated; do not edit. AGPL-3.0-only. NDDev OpenNetwork — https://nddev.ai
// Source: NDDev-OpenNetwork/nddev-device-sync-protocol/contracts/v1/source-response.schema.json
// Schema SHA-256: ef73380eac46d9f46fbd4776bae3c2d91a1236176c67582ed0df407a584bc6d8
// Generator script SHA-256: 7bc9a3eb58f85fbefaea8170a5625af6258acbf3ff797fe56e30cafbaee3b6ee
// Tool: quicktype-core 26.0.0; dependency lock SHA-256: 643e0f2ba88602f4acb9546c401d4838e8e106a9180c0e6f3a760985081bfcf6
// DTO generation is not schema, authorization or cryptographic validation.

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceResponse {
    pub license: License,

    pub source_url: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum License {
    #[serde(rename = "AGPL-3.0-only")]
    Agpl30Only,
}
