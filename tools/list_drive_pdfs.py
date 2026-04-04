#!/usr/bin/env python3
"""
List PDFs visible to Google Drive API credentials.

Modes
-----
1) Personal Google account — OAuth (browser login):
     pip install google-api-python-client google-auth-oauthlib google-auth-httplib2
     Create OAuth "Desktop" client in Google Cloud Console, download JSON as client_secret.json
     python tools/list_drive_pdfs.py --oauth path/to/client_secret.json

2) Service account — same style as dlt dlt/secrets.toml (share Drive folders with the SA email):
     python tools/list_drive_pdfs.py --secrets dlt/secrets.toml

3) Service account — standard GCP key file:
     set GOOGLE_APPLICATION_CREDENTIALS=C:\\path\\sa-key.json
     python tools/list_drive_pdfs.py

Optional: --folder FOLDER_ID limits listing to that folder (and children). Otherwise lists all
PDFs the credential can see (My Drive + shared).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
TOKEN_FILE = Path(__file__).resolve().parent / "token.json"


def _drive_service_sa_from_toml(secrets_path: Path):
    import tomllib
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    data = tomllib.loads(secrets_path.read_text(encoding="utf-8"))
    cred = data["sources"]["filesystem"]["credentials"]
    info = {
        "type": "service_account",
        "project_id": cred["project_id"],
        "private_key_id": cred.get("private_key_id", "dlt"),
        "private_key": cred["private_key"].replace("\\n", "\n"),
        "client_email": cred["client_email"],
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    credentials = service_account.Credentials.from_service_account_info(
        info, scopes=SCOPES
    )
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def _drive_service_sa_from_json(json_path: Path):
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    credentials = service_account.Credentials.from_service_account_file(
        str(json_path), scopes=SCOPES
    )
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def _drive_service_oauth(client_secret_path: Path):
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                str(client_secret_path), SCOPES
            )
            creds = flow.run_local_server(port=0)
        TOKEN_FILE.write_text(creds.to_json(), encoding="utf-8")
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _list_pdfs(service, folder_id: str | None) -> None:
    q_parts = ["mimeType = 'application/pdf'", "trashed = false"]
    if folder_id:
        q_parts.append(f"'{folder_id}' in parents")
    query = " and ".join(q_parts)
    page_token = None
    n = 0
    while True:
        resp = (
            service.files()
            .list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name, mimeType, webViewLink, modifiedTime)",
                pageToken=page_token,
                pageSize=100,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        for f in resp.get("files", []):
            n += 1
            link = f.get("webViewLink") or f"https://drive.google.com/file/d/{f['id']}/view"
            print(f"{n:4}  {f['name']}")
            print(f"      id: {f['id']}")
            print(f"      {link}")
            if f.get("modifiedTime"):
                print(f"      modified: {f['modifiedTime']}")
            print()
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    if n == 0:
        print("No PDFs found for this query / credential.", file=sys.stderr)
        if folder_id:
            print(f"Folder id was: {folder_id}", file=sys.stderr)
        sys.exit(1)
    print(f"Total: {n} PDF file(s)")


def main() -> None:
    ap = argparse.ArgumentParser(description="List PDFs in Google Drive")
    ap.add_argument(
        "--oauth",
        metavar="CLIENT_SECRET_JSON",
        help="OAuth desktop client secret JSON (personal Google account)",
    )
    ap.add_argument(
        "--secrets",
        metavar="SECRETS_TOML",
        help="dlt-style secrets.toml with [sources.filesystem.credentials]",
    )
    ap.add_argument(
        "--folder",
        metavar="FOLDER_ID",
        help="Only PDFs inside this Drive folder (file id from URL)",
    )
    args = ap.parse_args()
    repo = Path(__file__).resolve().parents[1]

    if args.oauth:
        service = _drive_service_oauth(Path(args.oauth))
    elif args.secrets:
        service = _drive_service_sa_from_toml(Path(args.secrets).resolve())
    elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        service = _drive_service_sa_from_json(
            Path(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
        )
    else:
        default_secrets = repo / "dlt" / "secrets.toml"
        if default_secrets.is_file():
            service = _drive_service_sa_from_toml(default_secrets)
        else:
            ap.print_help()
            print(
                "\nNo credentials found. Use --oauth, --secrets, set "
                "GOOGLE_APPLICATION_CREDENTIALS, or create dlt/secrets.toml",
                file=sys.stderr,
            )
            sys.exit(2)

    _list_pdfs(service, args.folder)


if __name__ == "__main__":
    main()
