
import os
import requests
import streamlit as st

st.set_page_config(page_title="Generador CES", page_icon="📄")

BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000")
API_KEY = os.environ.get("API_KEY", "")

st.title("📄 Generar Word CES")

with st.form("gen_form"):
    folder_url = st.text_input("URL de la carpeta de Google Drive", help="Debe ser una carpeta que contenga la plantilla y los archivos fuente")
    subestacion = st.text_input("Nombre de subestación", value="cóndores")
    submitted = st.form_submit_button("Generar Word")

if submitted:
    if not folder_url or not subestacion:
        st.error("Completa todos los campos.")
    else:
        try:
            with st.spinner("Generando documento..."):
                params = {"folder_url": folder_url, "subestacion": subestacion}
                headers = {"X-API-Key": API_KEY} if API_KEY else {}
                resp = requests.post(f"{BACKEND_URL}/generate", params=params, headers=headers, timeout=300)
                if resp.status_code != 200:
                    st.error(f"Error {resp.status_code}: {resp.text}")
                else:
                    filename = resp.headers.get("Content-Disposition", 'attachment; filename="CES-Documento.docx"').split("filename=")[-1].strip('"')
                    st.download_button("Descargar Word", resp.content, file_name=filename, mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        except Exception as e:
            st.exception(e)
