# ===============================================================
# Importador MATERIAIS -> CSV no Google Drive
# - Lê a lista de fontes em BD_Config!A3:A (IDs ou URLs)
# - Copia A:E (linha 2+) da aba MATERIAIS de cada fonte
# - Concatena tudo
# - Converte coluna A para número
# - Gera coluna extra com base na coluna A
# - Salva CSV com delimitador ";" na pasta do Google Drive
# - Nome fixo do arquivo: MATERIAIS.csv
# - Se já existir, sobrescreve
# ===============================================================

import os
import re
import io
import csv
import json

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

# ===================== CONFIG =====================

SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), "credenciais.json")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive",
]

# Planilha onde fica a configuração das fontes
CONFIG_SPREADSHEET_ID = "1Ipp454Clq0lKik8G5LjMMmV-8eA0R6if4FGG555K1j8"
CONFIG_SHEET_NAME = "BD_Config"
CONFIG_RANGE = "A3:A"

# Aba de origem
SOURCE_SHEET_NAME = "MATERIAIS"

# Pasta de destino no Google Drive
DRIVE_FOLDER_ID = "1la_5Ozfa0zkZQ8a4OKElkjrIA9dPUB8Y"

# Nome fixo do arquivo CSV
OUTPUT_FILE_NAME = "MATERIAIS.csv"

NUM_COLS = 5  # A:E

# Se quiser incluir cabeçalho no CSV, mude para True
INCLUDE_HEADER = False
HEADER_EXTRA_NAME = "CODIGO_A"

# ===============================================================


def get_services_and_email():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(
            f"Arquivo de credenciais não encontrado: {SERVICE_ACCOUNT_FILE}"
        )

    if os.path.getsize(SERVICE_ACCOUNT_FILE) == 0:
        raise ValueError(f"O arquivo de credenciais está vazio: {SERVICE_ACCOUNT_FILE}")

    try:
        with open(SERVICE_ACCOUNT_FILE, "r", encoding="utf-8") as f:
            json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"O arquivo {SERVICE_ACCOUNT_FILE} não contém JSON válido. Erro: {e}"
        )

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
    )

    sheets_svc = build("sheets", "v4", credentials=creds)
    drive_svc = build("drive", "v3", credentials=creds)

    return sheets_svc, drive_svc, creds.service_account_email


def pad_row_to_n_cols(row, n):
    if len(row) < n:
        return row + [""] * (n - len(row))
    elif len(row) > n:
        return row[:n]
    return row


def limpar_numero(valor):
    """Converte texto numérico em número."""
    if isinstance(valor, (int, float)):
        return valor

    if not isinstance(valor, str):
        return ""

    v = valor.strip().replace("'", "").replace(" ", "")
    v = re.sub(r"(?i)r\$", "", v)
    v = v.replace(",", ".")

    try:
        return float(v)
    except ValueError:
        return ""


def tratar_colunas_numericas(rows):
    """Aplica limpeza na coluna A (0)."""
    for r in rows:
        if len(r) > 0:
            r[0] = limpar_numero(r[0])
    return rows


def read_values(svc, spreadsheet_id, rng):
    resp = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=rng,
        majorDimension="ROWS",
    ).execute()
    return resp.get("values", [])


def extract_spreadsheet_id(text):
    """Aceita ID puro ou URL; retorna o ID ou None se inválido."""
    if not text:
        return None

    text = text.strip()

    m = re.search(r"/d/([a-zA-Z0-9-_]+)", text)
    if m:
        return m.group(1)

    if re.fullmatch(r"[a-zA-Z0-9-_]{20,}", text):
        return text

    return None


def get_source_ids_from_config(svc):
    """Lê BD_Config!A3:A e devolve lista de IDs válidos (sem vazios e sem duplicados)."""
    raw = read_values(svc, CONFIG_SPREADSHEET_ID, f"{CONFIG_SHEET_NAME}!{CONFIG_RANGE}")

    ids = []
    for row in raw:
        cell = row[0].strip() if row and len(row) > 0 else ""
        if not cell:
            continue

        sid = extract_spreadsheet_id(cell)
        if sid:
            ids.append(sid)

    seen = set()
    uniq = []
    for sid in ids:
        if sid not in seen:
            uniq.append(sid)
            seen.add(sid)

    return uniq


def read_source_block(svc, spreadsheet_id, sheet_name):
    """Lê A2:E da origem e aplica tratamento numérico."""
    rng = f"{sheet_name}!A2:E"
    values = read_values(svc, spreadsheet_id, rng)
    rows = [pad_row_to_n_cols(r, NUM_COLS) for r in values]
    return tratar_colunas_numericas(rows)


def read_source_header(svc, spreadsheet_id, sheet_name):
    """Lê A1:E da origem para usar como cabeçalho do CSV, se necessário."""
    rng = f"{sheet_name}!A1:E1"
    values = read_values(svc, spreadsheet_id, rng)
    if values:
        return pad_row_to_n_cols(values[0], NUM_COLS)
    return [""] * NUM_COLS


def normalizar_valor_codigo(valor):
    """Normaliza o valor para uso no código, removendo .0 de inteiros."""
    if valor in ("", None):
        return ""

    if isinstance(valor, int):
        return str(valor)

    if isinstance(valor, float):
        if valor.is_integer():
            return str(int(valor))
        return format(valor, "f").rstrip("0").rstrip(".")

    s = str(valor).strip()

    # Caso venha como texto tipo '1133017.0'
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".", 1)[0]

    return s


def gerar_codigo_extra(valor_a):
    """Gera o valor da coluna extra com base na coluna A."""
    valor_a = normalizar_valor_codigo(valor_a)
    if valor_a == "":
        return ""

    before_underscore = valor_a.split("_", 1)[0]
    digits_only = re.sub(r"\D", "", before_underscore)

    if len(digits_only) == 6:
        prefix = "B-0"
    elif len(digits_only) == 7:
        prefix = "B-"
    else:
        prefix = "B-"

    return prefix + valor_a


def montar_linhas_finais(rows):
    """Adiciona a coluna extra em cada linha com base na coluna A."""
    final_rows = []
    for row in rows:
        row = pad_row_to_n_cols(row, NUM_COLS)
        val_a = row[0] if len(row) > 0 else ""
        extra_val = gerar_codigo_extra(val_a)
        final_rows.append(row + [extra_val])
    return final_rows


def format_csv_value(value):
    """Formata valores para escrita no CSV."""
    if value is None or value == "":
        return ""

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, int):
        return str(value)

    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return format(value, "f").rstrip("0").rstrip(".")

    return str(value)


def build_csv_bytes(rows):
    """Converte as linhas para bytes CSV com delimitador ';'."""
    buffer = io.StringIO(newline="")
    writer = csv.writer(
        buffer,
        delimiter=";",
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n",
    )

    for row in rows:
        writer.writerow([format_csv_value(v) for v in row])

    csv_content = buffer.getvalue()
    return csv_content.encode("utf-8-sig")


def find_existing_file_in_folder(drive_svc, folder_id, file_name):
    """Procura arquivo com nome exato dentro da pasta."""
    escaped_file_name = file_name.replace("'", "\\'")

    query = (
        f"name = '{escaped_file_name}' "
        f"and '{folder_id}' in parents "
        f"and trashed = false"
    )

    resp = drive_svc.files().list(
        q=query,
        spaces="drive",
        fields="files(id,name,modifiedTime,webViewLink)",
        pageSize=10,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()

    files = resp.get("files", [])
    return files[0] if files else None


def create_or_update_csv_in_drive(drive_svc, folder_id, file_name, csv_bytes):
    """Cria ou sobrescreve um CSV na pasta do Google Drive."""
    media = MediaIoBaseUpload(
        io.BytesIO(csv_bytes),
        mimetype="text/csv",
        resumable=False,
    )

    existing = find_existing_file_in_folder(drive_svc, folder_id, file_name)

    if existing:
        updated = drive_svc.files().update(
            fileId=existing["id"],
            media_body=media,
            fields="id,name,webViewLink",
            supportsAllDrives=True,
        ).execute()
        updated["_action"] = "updated"
        return updated

    file_metadata = {
        "name": file_name,
        "parents": [folder_id],
    }

    created = drive_svc.files().create(
        body=file_metadata,
        media_body=media,
        fields="id,name,webViewLink",
        supportsAllDrives=True,
    ).execute()
    created["_action"] = "created"
    return created


def main():
    print("🔄 Iniciando importação baseado em BD_Config!A3:A ...\n")

    try:
        sheets_svc, drive_svc, sa_email = get_services_and_email()
    except (FileNotFoundError, ValueError) as e:
        print("❌", e)
        return

    print(f"👤 Service Account: {sa_email}")
    print("   ➜ Garanta acesso à planilha de configuração, às fontes e à pasta do Drive.\n")

    try:
        source_ids = get_source_ids_from_config(sheets_svc)
    except HttpError as e:
        print("❌ Erro ao ler BD_Config:", e)
        return

    if not source_ids:
        print("❌ Nenhuma fonte encontrada em BD_Config!A3:A (IDs/URLs).")
        return

    print(f"📚 Fontes encontradas em BD_Config: {len(source_ids)}")
    for i, sid in enumerate(source_ids, start=1):
        print(f"   - Fonte #{i}: {sid}")
    print()

    all_rows = []
    report_lines = []
    header_row = None

    for i, fid in enumerate(source_ids, start=1):
        try:
            if INCLUDE_HEADER and header_row is None:
                header_row = read_source_header(sheets_svc, fid, SOURCE_SHEET_NAME)

            rows = read_source_block(sheets_svc, fid, SOURCE_SHEET_NAME)
            report_lines.append(f"Fonte #{i}: {len(rows)} linha(s).")
            all_rows.extend(rows)

        except HttpError as e:
            report_lines.append(f"Fonte #{i}: ERRO -> {e}")
            print(f"⚠️ Origem #{i} inacessível (ID: {fid}). Compartilhe com {sa_email}.")
        except Exception as e:
            report_lines.append(f"Fonte #{i}: ERRO -> {e}")

    total_expected = len(all_rows)
    report_lines.append(f"\nTotal consolidado: {total_expected} linha(s).")

    if total_expected == 0:
        print("\n".join(report_lines))
        print("\nNada para exportar.")
        return

    print(f"🧱 Montando linhas finais com coluna extra para {total_expected} linha(s)...")
    final_rows = montar_linhas_finais(all_rows)

    if INCLUDE_HEADER:
        if not header_row:
            header_row = [""] * NUM_COLS
        csv_rows = [header_row + [HEADER_EXTRA_NAME]] + final_rows
    else:
        csv_rows = final_rows

    print(f"📝 Gerando CSV: {OUTPUT_FILE_NAME}")
    csv_bytes = build_csv_bytes(csv_rows)

    try:
        uploaded = create_or_update_csv_in_drive(
            drive_svc=drive_svc,
            folder_id=DRIVE_FOLDER_ID,
            file_name=OUTPUT_FILE_NAME,
            csv_bytes=csv_bytes,
        )
    except HttpError as e:
        print("❌ Erro ao enviar CSV para o Google Drive:", e)
        return

    print("\n=== RELATÓRIO DE EXPORTAÇÃO ===")
    print("\n".join(report_lines))
    print("\n✅ CSV processado com sucesso!")
    print("♻️ Ação: sobrescrito" if uploaded.get("_action") == "updated" else "🆕 Ação: criado")
    print(f"📄 Nome: {uploaded.get('name')}")
    print(f"🆔 ID: {uploaded.get('id')}")
    if uploaded.get("webViewLink"):
        print(f"🔗 Link: {uploaded.get('webViewLink')}")


if __name__ == "__main__":
    main()
