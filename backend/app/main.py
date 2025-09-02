# backend/app/main.py

import os
import io
import json
import logging
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


app = FastAPI(title="CES Backend")

# API key opcional (si no está definida, no se exige en las rutas)
API_KEY_ENV = os.getenv("API_KEY")


# ---------- Arranque: inicializa Google Drive y lo inyecta al pipeline ----------
@app.on_event("startup")
def startup_event():
    # Asegura un directorio /content (tu pipeline lo usa para escribir archivos)
    os.makedirs("/content", exist_ok=True)

    # Lee Service Account desde la variable de entorno (JSON como string)
    sa_raw = os.getenv("GCP_SA_KEY")
    if not sa_raw:
        logging.warning(
            "GCP_SA_KEY no está definida. /generate fallará hasta que la configures."
        )
        return

    try:
        info = json.loads(sa_raw)
    except json.JSONDecodeError:
        logging.error("GCP_SA_KEY no contiene un JSON válido.")
        raise

    scopes_env = os.getenv(
        "GOOGLE_SCOPES",
        "https://www.googleapis.com/auth/drive.readonly"
    )
    scopes = [s.strip() for s in scopes_env.replace(",", " ").split() if s.strip()]

    creds = Credentials.from_service_account_info(info, scopes=scopes)
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    app.state.drive = drive

    # Inyecta el cliente de Drive en el módulo pipeline
    try:
        from . import pipeline as ces
    except Exception:
        import pipeline as ces  # fallback para ejecución local
    ces.drive = drive
    logging.info("Google Drive inicializado e inyectado en pipeline.")


def require_api_key(x_api_key: Optional[str] = Header(default=None)):
    """
    Verifica el header x-api-key si API_KEY está configurada en entorno.
    """
    if API_KEY_ENV and x_api_key != API_KEY_ENV:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True


# ---------- Modelos ----------
class GeneratePayload(BaseModel):
    folder_url: str
    subestacion: str


# ---------- Rutas ----------
@app.get("/")
def health():
    return {"ok": True}


@app.post("/generate")
def generate(payload: GeneratePayload, _: bool = Depends(require_api_key)):
    # En caso de cold start sin startup_event
    if not hasattr(app.state, "drive"):
        startup_event()

    try:
        # Import aquí para que funcione tanto con paquete (Render) como local
        try:
            from . import pipeline as ces
        except Exception:
            import pipeline as ces

        # Reinyecta por si acaso
        if hasattr(app.state, "drive"):
            ces.drive = app.state.drive

        # Ejecuta el pipeline con los parámetros del frontend
        out_path = ces.generar_word_ces(payload.folder_url, payload.subestacion)

        if not os.path.isfile(out_path):
            raise FileNotFoundError(f"No se encontró el archivo generado: {out_path}")

        filename = os.path.basename(out_path)
        with open(out_path, "rb") as f:
            data = f.read()

        return StreamingResponse(
            io.BytesIO(data),
            media_type=(
                "application/"
                "vnd.openxmlformats-officedocument.wordprocessingml.document"
            ),
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )

    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Error generando el documento.")
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Ejecución local opcional ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

