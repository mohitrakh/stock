use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
pub enum AppError {
    Database(sqlx::Error),
    NotFound,
    Validation(String),
}

impl From<sqlx::Error> for AppError {
    fn from(err: sqlx::Error) -> Self {
        AppError::Database(err)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::Database(err) => {
                eprintln!("Database error: {:?}", err);

                (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error").into_response()
            }

            AppError::NotFound => (StatusCode::NOT_FOUND, "Resource not found").into_response(),

            AppError::Validation(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
        }
    }
}
