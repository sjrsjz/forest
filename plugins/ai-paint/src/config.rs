use forest_serde::{DeriveSchema, DeriveValue};

pub const BACKEND: &str = "/backend";
pub const OPENAI_IMAGES: &str = "/openai-images";
pub const SILICONFLOW: &str = "/siliconflow";
pub const SWARM_UI: &str = "/swarm-ui";

#[derive(Clone, DeriveSchema, DeriveValue)]
pub enum Backend {
    OpenAiImages,
    SiliconFlow,
    SwarmUi,
}

#[derive(Clone, DeriveSchema, DeriveValue)]
pub struct Sizes {
    pub portrait: String,
    pub landscape: String,
    pub square: String,
}
impl Default for Sizes {
    fn default() -> Self {
        Self {
            portrait: "720x1440".into(),
            landscape: "1440x720".into(),
            square: "1024x1024".into(),
        }
    }
}

#[derive(Clone, DeriveSchema, DeriveValue)]
pub struct OpenAiImages {
    pub endpoint: String,
    pub api_key: String,
    pub model: String,
    pub quality: String,
    pub sizes: Sizes,
}
impl Default for OpenAiImages {
    fn default() -> Self {
        Self {
            endpoint: "https://api.openai-hk.com/v1/images/generations".into(),
            api_key: String::new(),
            model: "nano-banana-2".into(),
            quality: "high".into(),
            sizes: Sizes {
                portrait: "9x16".into(),
                landscape: "16x9".into(),
                square: "1x1".into(),
            },
        }
    }
}

#[derive(Clone, DeriveSchema, DeriveValue)]
pub struct SiliconFlow {
    pub endpoint: String,
    pub api_key: String,
    pub model: String,
    pub steps: i32,
    pub sizes: Sizes,
}
impl Default for SiliconFlow {
    fn default() -> Self {
        Self {
            endpoint: "https://api.siliconflow.cn/v1/images/generations".into(),
            api_key: String::new(),
            model: "Kwai-Kolors/Kolors".into(),
            steps: 20,
            sizes: Sizes::default(),
        }
    }
}

#[derive(Clone, DeriveSchema, DeriveValue)]
pub struct SwarmUi {
    pub endpoint: String,
    pub auth_token: String,
    pub anime_model: String,
    pub photo_model: String,
    pub default_model: String,
    pub anime_negative_prompt: String,
    pub photo_negative_prompt: String,
    pub steps: i32,
    pub sizes: Sizes,
}
impl Default for SwarmUi {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:7801".into(),
            auth_token: String::new(),
            anime_model: "text-to-image-anime".into(),
            photo_model: "text-to-image-photo".into(),
            default_model: "text-to-image".into(),
            anime_negative_prompt: String::new(),
            photo_negative_prompt: String::new(),
            steps: 30,
            sizes: Sizes::default(),
        }
    }
}
