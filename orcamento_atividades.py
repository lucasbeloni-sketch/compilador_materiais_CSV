# ===============================================================
# Exportador de CSVs no Google Drive
#
# Gera na mesma execução:
#
# 1) MATERIAIS.csv
#    - Base: MATERIAIS_BASE!A2:E
#    - Fontes: MATERIAIS!A2:E
#    - Cabeçalho fixo
#    - Gera coluna extra "Com Mascara" com base na coluna A
#    - Remove duplicadas
#
# 2) Vários arquivos MATERIAIS_POR_PONTO_<valor_coluna_H>.csv
#    - Base: MATERIAIS_POR_PONTO_BASE!A2:I
#    - Fontes: MATERIAIS_POR_PONTO!A2:I
#    - Cabeçalho lido de A1:I da aba base
#    - NÃO gera coluna extra
#    - Agrupa pelo valor da coluna H
#    - Remove duplicadas
#
# Regras comuns:
# - lê as fontes em BD_Config!A3:A
# - salva CSV com delimitador ";"
# - sobrescreve o arquivo se já existir
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

# Planilha principal onde fica a configuração das fontes
CONFIG_SPREADSHEET_ID = "1Ipp454Clq0lKik8G5LjMMmV-8eA0R6if4FGG555K1j8"
CONFIG_SHEET_NAME = "BD_Config"
CONFIG_RANGE = "A3:A"

# Pasta de destino no Google Drive
DRIVE_FOLDER_ID = "1la_5Ozfa0zkZQ8a4OKElkjrIA9dPUB8Y"

# --- Etapa 1: MATERIAIS ---
MATERIAIS_OUTPUT_FILE_NAME = "MATERIAIS.csv"
MATERIAIS_BASE_SHEET_NAME = "MATERIAIS_BASE"
MATERIAIS_BASE_RANGE = "A2:E"
MATERIAIS_SOURCE_SHEET_NAME = "MATERIAIS"
MATERIAIS_SOURCE_RANGE = "A2:E"
MATERIAIS_NUM_COLS = 5
MATERIAIS_HEADER = [
    "Projeto",
    "Código",
    "Descrição",
    "Quantidade",
    "Orçamentista",
    "Com Mascara",
]

# --- Etapa 2: MATERIAIS_POR_PONTO ---
MPP_FILE_PREFIX = "MATERIAIS_POR_PONTO"
MPP_BASE_SHEET_NAME = "MATERIAIS_POR_PONTO_BASE"
MPP_BASE_RANGE = "A2:I"
MPP_BASE_HEADER_RANGE = "A1:I1"
MPP_SOURCE_SHEET_NAME = "MATERIAIS_POR_PONTO"
MPP_SOURCE_RANGE = "A2:I"
MPP_NUM_COLS = 9
MPP_GROUP_COL_INDEX = 7  # coluna H (0-based)

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


def read_values(svc, spreadsheet_id, rng):
    resp = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=rng,
        majorDimension="ROWS",
    ).execute()
    return resp.get("values", [])


def pad_row_to_n_cols(row, n):
    if len(row) < n:
        return row + [""] * (n - len(row))
    if len(row) > n:
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
    """Aplica limpeza apenas na coluna A."""
    for r in rows:
        if len(r) > 0:
            r[0] = limpar_numero(r[0])
    return rows


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
    """Lê BD_Config!A3:A e devolve lista de IDs válidos, sem vazios e sem duplicados."""
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


def read_block(svc, spreadsheet_id, rng, num_cols):
    values = read_values(svc, spreadsheet_id, rng)
    rows = [pad_row_to_n_cols(r, num_cols) for r in values]
    return tratar_colunas_numericas(rows)


def read_header(svc, spreadsheet_id, rng, num_cols):
    values = read_values(svc, spreadsheet_id, rng)
    if values and values[0]:
        return pad_row_to_n_cols(values[0], num_cols)
    return [f"Coluna {i}" for i in range(1, num_cols + 1)]


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

    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".", 1)[0]

    return s


def gerar_codigo_extra(valor_a):
    """Gera a coluna 'Com Mascara' com base na coluna A."""
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


def montar_linhas_finais_materiais(rows):
    """Adiciona a coluna extra em cada linha com base na coluna A."""
    final_rows = []
    for row in rows:
        row = pad_row_to_n_cols(row, MATERIAIS_NUM_COLS)
        val_a = row[0] if len(row) > 0 else ""
        extra_val = gerar_codigo_extra(val_a)
        final_rows.append(row + [extra_val])
    return final_rows


def format_csv_value(value):
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


def remover_linhas_duplicadas(rows):
    """Remove linhas duplicadas preservando a primeira ocorrência."""
    seen = set()
    unique_rows = []

    for row in rows:
        chave = tuple(format_csv_value(v) for v in row)
        if chave not in seen:
            seen.add(chave)
            unique_rows.append(row)

    return unique_rows


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


def sanitize_filename_component(value):
    """
    Sanitiza o valor usado no nome do arquivo.
    Remove caracteres inválidos e padroniza vazios.
    """
    txt = format_csv_value(value).strip()

    if txt == "":
        return "SEM_VALOR"

    txt = re.sub(r'[\\/:*?"<>|]+', "_", txt)
    txt = re.sub(r"\s+", "_", txt)
    txt = re.sub(r"_+", "_", txt).strip("_")

    return txt if txt else "SEM_VALOR"


def find_existing_file_in_folder(drive_svc, folder_id, file_name):
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


def process_export_materiais(sheets_svc, drive_svc, source_ids):
    report_lines = []
    all_rows = []

    # Base principal
    try:
        base_rows = read_block(
            sheets_svc,
            CONFIG_SPREADSHEET_ID,
            f"{MATERIAIS_BASE_SHEET_NAME}!{MATERIAIS_BASE_RANGE}",
            MATERIAIS_NUM_COLS,
        )
        report_lines.append(
            f"{MATERIAIS_BASE_SHEET_NAME} ({CONFIG_SPREADSHEET_ID}): {len(base_rows)} linha(s)."
        )
        all_rows.extend(base_rows)
    except HttpError as e:
        report_lines.append(f"{MATERIAIS_BASE_SHEET_NAME}: ERRO -> {e}")
        print(f"⚠️ Erro ao ler {MATERIAIS_BASE_SHEET_NAME} da planilha principal.")

    # Fontes
    for i, fid in enumerate(source_ids, start=1):
        try:
            rows = read_block(
                sheets_svc,
                fid,
                f"{MATERIAIS_SOURCE_SHEET_NAME}!{MATERIAIS_SOURCE_RANGE}",
                MATERIAIS_NUM_COLS,
            )
            report_lines.append(f"Fonte #{i}: {len(rows)} linha(s).")
            all_rows.extend(rows)

        except HttpError as e:
            report_lines.append(f"Fonte #{i}: ERRO -> {e}")
            print(f"⚠️ Origem #{i} inacessível para MATERIAIS (ID: {fid}).")
        except Exception as e:
            report_lines.append(f"Fonte #{i}: ERRO -> {e}")

    total_lido = len(all_rows)
    report_lines.append(f"Total lido antes da deduplicação: {total_lido} linha(s).")

    if total_lido == 0:
        print("\n=== RELATÓRIO DE EXPORTAÇÃO: MATERIAIS ===")
        print("\n".join(report_lines))
        print("\nNada para exportar.\n")
        return

    print(f"🧱 Montando linhas finais de MATERIAIS com coluna extra para {total_lido} linha(s)...")
    final_rows = montar_linhas_finais_materiais(all_rows)
    final_rows_sem_duplicadas = remover_linhas_duplicadas(final_rows)

    total_final = len(final_rows_sem_duplicadas)
    removidas = total_lido - total_final

    report_lines.append(f"Total após remover duplicadas: {total_final} linha(s).")
    report_lines.append(f"Duplicadas removidas: {removidas} linha(s).")

    csv_rows = [MATERIAIS_HEADER] + final_rows_sem_duplicadas
    csv_bytes = build_csv_bytes(csv_rows)

    try:
        uploaded = create_or_update_csv_in_drive(
            drive_svc=drive_svc,
            folder_id=DRIVE_FOLDER_ID,
            file_name=MATERIAIS_OUTPUT_FILE_NAME,
            csv_bytes=csv_bytes,
        )
    except HttpError as e:
        print(f"❌ Erro ao enviar {MATERIAIS_OUTPUT_FILE_NAME} para o Google Drive:", e)
        return

    print("\n=== RELATÓRIO DE EXPORTAÇÃO: MATERIAIS ===")
    print("\n".join(report_lines))
    print("\n✅ CSV processado com sucesso!")
    print("♻️ Ação: sobrescrito" if uploaded.get("_action") == "updated" else "🆕 Ação: criado")
    print(f"📄 Nome: {uploaded.get('name')}")
    print(f"🆔 ID: {uploaded.get('id')}")
    if uploaded.get("webViewLink"):
        print(f"🔗 Link: {uploaded.get('webViewLink')}")
    print()


def process_export_materiais_por_ponto(sheets_svc, drive_svc, source_ids):
    report_lines = []
    all_rows = []

    # Cabeçalho da base
    try:
        csv_header = read_header(
            sheets_svc,
            CONFIG_SPREADSHEET_ID,
            f"{MPP_BASE_SHEET_NAME}!{MPP_BASE_HEADER_RANGE}",
            MPP_NUM_COLS,
        )
    except Exception:
        csv_header = [f"Coluna {i}" for i in range(1, MPP_NUM_COLS + 1)]

    # Base principal
    try:
        base_rows = read_block(
            sheets_svc,
            CONFIG_SPREADSHEET_ID,
            f"{MPP_BASE_SHEET_NAME}!{MPP_BASE_RANGE}",
            MPP_NUM_COLS,
        )
        report_lines.append(
            f"{MPP_BASE_SHEET_NAME} ({CONFIG_SPREADSHEET_ID}): {len(base_rows)} linha(s)."
        )
        all_rows.extend(base_rows)
    except HttpError as e:
        report_lines.append(f"{MPP_BASE_SHEET_NAME}: ERRO -> {e}")
        print(f"⚠️ Erro ao ler {MPP_BASE_SHEET_NAME} da planilha principal.")

    # Fontes
    for i, fid in enumerate(source_ids, start=1):
        try:
            rows = read_block(
                sheets_svc,
                fid,
                f"{MPP_SOURCE_SHEET_NAME}!{MPP_SOURCE_RANGE}",
                MPP_NUM_COLS,
            )
            report_lines.append(f"Fonte #{i}: {len(rows)} linha(s).")
            all_rows.extend(rows)

        except HttpError as e:
            report_lines.append(f"Fonte #{i}: ERRO -> {e}")
            print(f"⚠️ Origem #{i} inacessível para MATERIAIS_POR_PONTO (ID: {fid}).")
        except Exception as e:
            report_lines.append(f"Fonte #{i}: ERRO -> {e}")

    total_lido = len(all_rows)
    report_lines.append(f"Total lido antes da deduplicação: {total_lido} linha(s).")

    if total_lido == 0:
        print("\n=== RELATÓRIO DE EXPORTAÇÃO: MATERIAIS_POR_PONTO ===")
        print("\n".join(report_lines))
        print("\nNada para exportar.\n")
        return

    # Sem coluna extra nesta etapa
    final_rows = [pad_row_to_n_cols(row, MPP_NUM_COLS) for row in all_rows]
    final_rows_sem_duplicadas = remover_linhas_duplicadas(final_rows)

    total_final = len(final_rows_sem_duplicadas)
    removidas = total_lido - total_final

    report_lines.append(f"Total após remover duplicadas: {total_final} linha(s).")
    report_lines.append(f"Duplicadas removidas: {removidas} linha(s).")

    # Agrupa pela coluna H
    grouped_rows = {}
    for row in final_rows_sem_duplicadas:
        valor_h = row[MPP_GROUP_COL_INDEX] if len(row) > MPP_GROUP_COL_INDEX else ""
        group_key = sanitize_filename_component(valor_h)
        grouped_rows.setdefault(group_key, []).append(row)

    print("\n=== RELATÓRIO DE EXPORTAÇÃO: MATERIAIS_POR_PONTO ===")
    print("\n".join(report_lines))
    print(f"Arquivos a gerar: {len(grouped_rows)}")

    for group_key, rows in sorted(grouped_rows.items()):
        output_file_name = f"{MPP_FILE_PREFIX}_{group_key}.csv"
        csv_rows = [csv_header] + rows
        csv_bytes = build_csv_bytes(csv_rows)

        try:
            uploaded = create_or_update_csv_in_drive(
                drive_svc=drive_svc,
                folder_id=DRIVE_FOLDER_ID,
                file_name=output_file_name,
                csv_bytes=csv_bytes,
            )
        except HttpError as e:
            print(f"❌ Erro ao enviar {output_file_name} para o Google Drive:", e)
            continue

        print(
            f"✅ {output_file_name} -> {len(rows)} linha(s) | "
            + ("sobrescrito" if uploaded.get("_action") == "updated" else "criado")
        )

    print()


def main():
    print("🔄 Iniciando exportação dos CSVs...\n")

    try:
        sheets_svc, drive_svc, sa_email = get_services_and_email()
    except (FileNotFoundError, ValueError) as e:
        print("❌", e)
        return

    print(f"👤 Service Account: {sa_email}")
    print("   ➜ Garanta acesso à planilha principal, às fontes e à pasta do Drive.\n")

    try:
        source_ids = get_source_ids_from_config(sheets_svc)
    except HttpError as e:
        print("❌ Erro ao ler BD_Config:", e)
        return

    if source_ids:
        print(f"📚 Fontes encontradas em BD_Config: {len(source_ids)}")
        for i, sid in enumerate(source_ids, start=1):
            print(f"   - Fonte #{i}: {sid}")
        print()
    else:
        print("⚠️ Nenhuma fonte encontrada em BD_Config!A3:A. Serão exportadas apenas as abas base.\n")

    process_export_materiais(sheets_svc, drive_svc, source_ids)
    process_export_materiais_por_ponto(sheets_svc, drive_svc, source_ids)


if __name__ == "__main__":
    main()
