use crate::{error::MicroserviceError, request::Request};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

const DEFAULT_LIMIT_RADIO: usize = 15;
const DEFAULT_LIMIT_SEARCH: usize = 5;
const DEFAULT_LIMIT_ARTIST: usize = 5;

#[derive(Deserialize)]
struct ApiResponse<T> {
    status: String,
    data: Option<T>,
    message: Option<String>,
}

#[derive(Clone)]
pub struct MicroserviceClient {
    addr: String,
}

impl MicroserviceClient {
    pub fn new(host: &str, port: u16) -> Self {
        Self { addr: format!("{}:{}", host, port) }
    }

    // ── Métodos nombrados (ergonomía) ──────────────────────────────────────

    pub async fn resolve<T: DeserializeOwned>(&self, query: &str) -> Result<T, MicroserviceError> {
        self.send(Request::new("resolve").query(query)).await
    }

    pub async fn search<T: DeserializeOwned>(&self, query: &str, limit: Option<usize>, filter: Option<&str>) -> Result<Vec<T>, MicroserviceError> {
        let mut req = Request::new("search")
            .query(query)
            .limit(limit.unwrap_or(DEFAULT_LIMIT_SEARCH));

        if let Some(f) = filter {
            req = req.filter(f);
        }

        self.send(req).await
    }

    /// Como `search`, pero heterogéneo: cada item lleva `"kind"` (track | album | artist).
    /// `filter`: songs | videos | albums | artists | all.
    pub async fn search_items<T: DeserializeOwned>(&self, query: &str, limit: Option<usize>, filter: &str) -> Result<Vec<T>, MicroserviceError> {
        self.send(
            Request::new("search_items")
                .query(query)
                .limit(limit.unwrap_or(DEFAULT_LIMIT_SEARCH))
                .filter(filter),
        ).await
    }

    pub async fn download<T: DeserializeOwned>(&self, query: &str) -> Result<T, MicroserviceError> {
        self.send(Request::new("download").query(query)).await
    }

    /// Vuelve a bajar el audio aunque ya esté descargado.
    pub async fn redownload<T: DeserializeOwned>(&self, query: &str) -> Result<T, MicroserviceError> {
        self.send(Request::new("redownload").query(query)).await
    }

    /// Reescribe título/artistas/álbum/portadas del track desde YT Music.
    pub async fn refresh_metadata<T: DeserializeOwned>(&self, track_id: &str) -> Result<T, MicroserviceError> {
        self.send(Request::new("refresh_metadata").query(track_id)).await
    }

    /// Vuelve a buscar la letra (.lrc) de un track ya descargado.
    pub async fn refresh_lyrics<T: DeserializeOwned>(&self, track_id: &str) -> Result<T, MicroserviceError> {
        self.send(Request::new("refresh_lyrics").query(track_id)).await
    }

    /// Recalcula BPM/key de un track ya descargado.
    pub async fn reanalyze<T: DeserializeOwned>(&self, track_id: &str) -> Result<T, MicroserviceError> {
        self.send(Request::new("reanalyze").query(track_id)).await
    }

    pub async fn delete_track(&self, track_id: &str) -> Result<(), MicroserviceError> {
        self.send::<serde_json::Value>(Request::new("remove").query(track_id)).await?;
        Ok(())
    }

    pub async fn radio<T: DeserializeOwned>(&self, query: &str, limit: Option<usize>) -> Result<Vec<T>, MicroserviceError> {
        self.send(Request::new("radio").query(query).limit(limit.unwrap_or(DEFAULT_LIMIT_RADIO))).await
    }

    pub async fn album<T: DeserializeOwned>(&self, album_id: &str) -> Result<T, MicroserviceError> {
        self.send(Request::new("album").query(album_id)).await
    }

    pub async fn artist<T: DeserializeOwned>(&self, channel_id: &str, limit: Option<usize>) -> Result<T, MicroserviceError> {
        self.send(Request::new("artist").query(channel_id).limit(limit.unwrap_or(DEFAULT_LIMIT_ARTIST))).await
    }

    pub async fn artist_profile<T: DeserializeOwned>(&self, channel_id: &str) -> Result<T, MicroserviceError> {
        self.send(Request::new("artist_profile").query(channel_id)).await
    }

    /// Se conecta y manda `{"action":"subscribe"}`; a diferencia de los demás
    /// métodos, esa conexión no responde una vez — queda dedicada a empujar
    /// eventos (ver la acción `subscribe` en la doc del protocolo). Se abre
    /// una conexión NUEVA y separada (no reusa `self.addr` para otra cosa),
    /// y un task en background la va leyendo y reenviando cada línea parseada
    /// al receiver devuelto. `T` es, por ejemplo, el enum `DownloadEvent` de
    /// la doc — pégalo en tu proyecto, igual que con `Track`/`ArtistResult`.
    /// El receiver se cierra solo si el servidor cierra la conexión o si el
    /// JSON de una línea no matchea `T` de forma irrecuperable.
    pub async fn subscribe_downloads<T>(&self) -> Result<tokio::sync::mpsc::UnboundedReceiver<T>, MicroserviceError>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let mut stream = TcpStream::connect(&self.addr).await.map_err(MicroserviceError::ConnectionFailed)?;

        let payload = serde_json::to_string(&Request::new("subscribe"))
            .map_err(|e| MicroserviceError::InvalidResponse(e.to_string()))?
            + "\n";
        stream.write_all(payload.as_bytes()).await.map_err(MicroserviceError::IoError)?;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            let mut reader = BufReader::new(stream);
            let mut line = String::new();

            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) | Err(_) => break, // conexión cerrada o error de IO
                    Ok(_) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() { continue; }

                        if let Ok(event) = serde_json::from_str::<T>(trimmed) {
                            if tx.send(event).is_err() { break; } // nadie escucha ya
                        }
                    }
                }
            }
        });

        Ok(rx)
    }

    pub async fn resolve_many<T: DeserializeOwned>(&self, ids: &[String]) -> Result<Vec<T>, MicroserviceError> {
        self.send(Request::new("resolve_many").ids(ids.to_vec())).await
    }

    pub async fn mark_as_played(&self, track_id: &str) -> Result<(), MicroserviceError> {
        self.send::<serde_json::Value>(Request::new("played").query(track_id)).await?;
        Ok(())
    }

    // ── Genérico crudo (para acciones no cubiertas arriba) ─────────────────

    pub async fn send<T: DeserializeOwned>(&self, req: Request) -> Result<T, MicroserviceError> {
        let payload = serde_json::to_string(&req)
            .map_err(|e| MicroserviceError::InvalidResponse(e.to_string()))?
            + "\n";

        let raw = self.send_raw(&payload).await?;

        let response: ApiResponse<T> = serde_json::from_str(&raw).map_err(|e| {
            MicroserviceError::InvalidResponse(format!("Fallo parseo: {}. Raw: {}", e, raw))
        })?;

        if response.status == "ok" {
            response.data.ok_or_else(|| {
                MicroserviceError::ServiceError("El microservicio devolvió ok pero 'data' es null".into())
            })
        } else {
            Err(MicroserviceError::ServiceError(
                response.message.unwrap_or_else(|| "Error desconocido del microservicio".into()),
            ))
        }
    }

    async fn send_raw(&self, payload: &str) -> Result<String, MicroserviceError> {
        let mut stream = TcpStream::connect(&self.addr).await.map_err(MicroserviceError::ConnectionFailed)?;
        stream.write_all(payload.as_bytes()).await.map_err(MicroserviceError::IoError)?;

        let mut reader = BufReader::new(stream);
        let mut response = String::new();
        reader.read_line(&mut response).await.map_err(MicroserviceError::IoError)?;

        Ok(response.trim().to_string())
    }
}