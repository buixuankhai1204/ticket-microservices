use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};

use super::dto::{
    ErrorResponse, LoginRequest, LoginResponse, PaginatedUsersResponse, PaginationMeta,
    RegisterRequest, UserResponse,
};
use super::handlers;

#[derive(OpenApi)]
#[openapi(
    paths(
        handlers::register,
        handlers::login,
        handlers::get_user,
        handlers::list_users
    ),
    components(schemas(
        RegisterRequest,
        LoginRequest,
        LoginResponse,
        UserResponse,
        PaginatedUsersResponse,
        PaginationMeta,
        ErrorResponse
    )),
    modifiers(&SecurityAddon)
)]
pub struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearer_auth",
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .bearer_format("JWT")
                        .build(),
                ),
            );
        }
    }
}
