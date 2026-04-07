use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct OrganizationRole(pub String);

impl OrganizationRole {
    pub fn new(role: impl Into<String>) -> Self {
        let normalized = role.into().trim().to_string();
        if normalized.is_empty() {
            Self::default()
        } else {
            Self(normalized)
        }
    }

    pub fn owner() -> Self {
        Self("owner".into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for OrganizationRole {
    fn default() -> Self {
        Self("member".into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthContextRef {
    pub user_id: String,
    pub organization_id: String,
    #[serde(default)]
    pub organization_role: OrganizationRole,
    #[serde(default)]
    pub organization_slug: Option<String>,
}

impl AuthContextRef {
    pub fn personal(user_id: impl Into<String>) -> Self {
        let user_id = user_id.into();
        Self {
            organization_id: personal_organization_id(&user_id),
            user_id,
            organization_role: OrganizationRole::owner(),
            organization_slug: None,
        }
    }

    pub fn default_organization_name(&self) -> String {
        if self.organization_id.starts_with("personal:") {
            "Personal workspace".into()
        } else if let Some(slug) = self.organization_slug.as_deref() {
            slug.replace('-', " ")
        } else {
            self.organization_id.clone()
        }
    }
}

pub fn personal_organization_id(user_id: &str) -> String {
    format!("personal:{user_id}")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserRecord {
    pub id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrganizationRecord {
    pub id: String,
    pub name: String,
    pub created_by_user_id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrganizationMembership {
    pub organization_id: String,
    pub user_id: String,
    pub role: OrganizationRole,
    pub created_at: chrono::DateTime<chrono::Utc>,
}
