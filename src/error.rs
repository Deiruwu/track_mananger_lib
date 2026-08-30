use thiserror::Error;

#[derive(Debug, Error)]
pub enum MicroserviceError {
    #[error("Conexión fallida: {0}")]
    ConnectionFailed(std::io::Error),

    #[error("Error de IO: {0}")]
    IoError(std::io::Error),

    #[error("Error del servicio: {0}")]
    ServiceError(String),

    #[error("Respuesta inválida: {0}")]
    InvalidResponse(String),
}