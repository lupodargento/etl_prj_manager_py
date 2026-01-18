#!/usr/bin/env python3
"""
etl_prj_manager.py

Utilizzo:

  ls-remote-prj:
    python etl_prj_manager.py ls-remote-prj gitlab
    python etl_prj_manager.py ls-remote-prj github

  create-remote-prj (richiede piattaforma e submodalita' register o protocol):
    python etl_prj_manager.py create-remote-prj gitlab register
    python etl_prj_manager.py create-remote-prj gitlab protocol
    python etl_prj_manager.py create-remote-prj github register
    python etl_prj_manager.py create-remote-prj github protocol

Richiede un file config.yml nella stessa cartella e le variabili
segrete (impostate via environment, non nel file YAML):
  - GITLAB_TOKEN  (permessi api + repository)
  - GITHUB_TOKEN  (permessi repo)

config.yml deve includere almeno:

agency_config:
  short_name_template: "AAA-BB"
  category_id: 4
  agency_ipa_code: "codice"

project_config:
  gitlab_group_id: 123
  gitlab_url: "https://..."
  gitlab_username: "user"
  gitlab_origin_remote_name: "nome-progetto-esistente"
  git_default_branch: "main"
  github_owner: "my-org-o-utente"
  github_visibility: "private"  # oppure "public"
"""

import os
import re
import sys
import time
from urllib.parse import quote

import base64
import gitlab
import requests
import yaml
from gitlab import exceptions as gl_ex


# ----------------- Config -----------------


def load_config(cfg_path: str = "config.yml") -> dict:
    if not os.path.exists(cfg_path):
        print(f"[ERRORE] File di configurazione '{cfg_path}' non trovato.")
        sys.exit(1)

    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    for sec in ("agency_config", "project_config"):
        if sec not in cfg:
            print(f"[ERRORE] Sezione '{sec}' mancante nel file di configurazione.")
            sys.exit(1)

    return cfg


def ensure_agency_fields(agency_cfg: dict):
    required = ["short_name_template", "category_id", "agency_ipa_code"]
    missing = [k for k in required if not agency_cfg.get(k)]
    if missing:
        print(
            "[ERRORE] agency_config deve contenere e valorizzare: "
            + ", ".join(required)
            + f". Mancanti: {', '.join(missing)}"
        )
        sys.exit(1)


def ensure_users(users_cfg: dict):
    if not isinstance(users_cfg, dict):
        print("[ERRORE] Sezione users mancante o non valida.")
        sys.exit(1)
    num_raw = users_cfg.get("number")
    try:
        num = int(num_raw)
    except Exception:
        num = 0
    if num <= 0:
        print("[ERRORE] users.number deve essere maggiore di 0.")
        sys.exit(1)
    users = []
    for idx in range(1, num + 1):
        ukey = f"user{idx}"
        udata = users_cfg.get(ukey)
        if not udata or not udata.get("name"):
            print(f"[ERRORE] Dato mancante per '{ukey}' o campo name vuoto.")
            sys.exit(1)
        users.append(udata["name"])
    return users


def require_env_var(key: str) -> str:
    val = os.getenv(key)
    if val:
        return val
    print(f"[ERRORE] Variabile d'ambiente '{key}' non impostata. Impostala come secret e riprova.")
    sys.exit(1)


# ----------------- GitLab utility -----------------


def init_gitlab_client(gitlab_url: str, token: str):
    try:
        gl = gitlab.Gitlab(gitlab_url, private_token=token)
        gl.auth()
        return gl
    except Exception as e:
        print("[ERRORE] Connessione a GitLab fallita:", e)
        sys.exit(1)


def list_group_projects(gl_client, group_id: int):
    try:
        group = gl_client.groups.get(group_id)
        return group.projects.list(all=True)
    except Exception as e:
        print("[ERRORE] Impossibile ottenere progetti dal gruppo:", e)
        sys.exit(1)


def build_auth_url(http_url: str, username: str, token: str) -> str:
    username_enc = quote(username, safe="")
    token_enc = quote(token, safe="")

    if http_url.startswith("http://") or http_url.startswith("https://"):
        scheme, rest = http_url.split("://", 1)
        return f"{scheme}://{username_enc}:{token_enc}@{rest}"

    return http_url


def find_origin_project(gl_client, group_id: int, origin_name: str):
    projects = list_group_projects(gl_client, group_id)
    proj_by_name = {p.name.strip().lower(): p for p in projects}
    chosen = proj_by_name.get(origin_name.strip().lower())
    if not chosen:
        print(f"[ERRORE] Il progetto sorgente '{origin_name}' non esiste nel gruppo GitLab.")
        sys.exit(1)
    return chosen


def derive_new_project_name(origin_name: str, new_suffix: str) -> str:
    m = re.search(r"([A-Za-z0-9]+-[A-Za-z0-9]+)$", origin_name)
    if not m:
        print("[ERRORE] Il progetto sorgente non termina con un suffisso tipo AAA-BB.")
        sys.exit(1)
    old_suffix = m.group(1)
    return origin_name[: -len(old_suffix)] + new_suffix.upper()


def create_gitlab_project_from_import(
    gl_client,
    group_id: int,
    project_name: str,
    import_url: str,
    default_branch: str,
):
    try:
        proj = gl_client.projects.create(
            {
                "name": project_name,
                "namespace_id": group_id,
                "visibility": "private",
                "default_branch": default_branch,
                "import_url": import_url,
            }
        )
        return proj
    except Exception as e:
        print("[ERRORE] Creazione o import GitLab fallita:", e)
        sys.exit(1)


def wait_for_gitlab_import(gl_client, project_id: int, timeout_sec: int = 180, poll_sec: int = 5):
    deadline = time.time() + timeout_sec
    while True:
        proj = gl_client.projects.get(project_id)
        status = getattr(proj, "import_status", None)
        if status in {None, "finished"}:
            return proj
        if status == "failed":
            err = getattr(proj, "import_error", "")
            print(f"[ERRORE] Import GitLab fallito: {err}")
            sys.exit(1)
        if time.time() > deadline:
            print("[ERRORE] Import GitLab non completato nei tempi previsti.")
            sys.exit(1)
        time.sleep(poll_sec)


def upsert_gitlab_file(proj, file_path: str, content: str, branch: str, commit_message: str):
    try:
        f = proj.files.get(file_path=file_path, ref=branch)
        f.content = content
        f.save(branch=branch, commit_message=commit_message)
    except gl_ex.GitlabGetError:
        proj.files.create(
            {
                "file_path": file_path,
                "branch": branch,
                "content": content,
                "commit_message": commit_message,
            }
        )


# ----------------- GitHub utility -----------------


def github_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def create_github_repo(token: str, owner: str, repo_name: str, private: bool = True) -> dict:
    headers = github_headers(token)
    if owner:
        url = f"https://api.github.com/orgs/{owner}/repos"
    else:
        url = "https://api.github.com/user/repos"

    payload = {"name": repo_name, "private": private, "auto_init": False}
    resp = requests.post(url, headers=headers, json=payload, timeout=30)

    if resp.status_code >= 300:
        detail = resp.json() if resp.content else {}
        print("[ERRORE] Creazione repository GitHub fallita:", detail)
        sys.exit(1)

    return resp.json()


def start_github_import(
    token: str,
    owner: str,
    repo_name: str,
    source_url: str,
    vcs_username: str,
    vcs_password: str,
):
    headers = github_headers(token)
    url = f"https://api.github.com/repos/{owner}/{repo_name}/import"
    payload = {
        "vcs": "git",
        "vcs_url": source_url,
        "vcs_username": vcs_username,
        "vcs_password": vcs_password,
    }

    resp = requests.put(url, headers=headers, json=payload, timeout=30)

    if resp.status_code not in {201, 202}:
        detail = resp.json() if resp.content else {}
        print("[ERRORE] Import in GitHub fallito:", detail)
        sys.exit(1)

    return resp.json()


def set_github_default_branch(token: str, owner: str, repo_name: str, branch: str):
    headers = github_headers(token)
    url = f"https://api.github.com/repos/{owner}/{repo_name}"
    resp = requests.patch(url, headers=headers, json={"default_branch": branch}, timeout=30)
    if resp.status_code >= 300:
        print("[WARNING] Impossibile impostare il default branch su GitHub:", resp.text)


def wait_for_github_import(token: str, owner: str, repo_name: str, timeout_sec: int = 180, poll_sec: int = 5):
    headers = github_headers(token)
    url = f"https://api.github.com/repos/{owner}/{repo_name}/import"
    deadline = time.time() + timeout_sec
    while True:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code >= 400:
            print("[ERRORE] Lettura stato import GitHub fallita:", resp.text)
            sys.exit(1)
        data = resp.json()
        status = data.get("status")
        if status in {"imported", "complete", None}:
            return data
        if status == "error":
            print("[ERRORE] Import GitHub fallito:", data)
            sys.exit(1)
        if time.time() > deadline:
            print("[ERRORE] Import GitHub non completato nei tempi previsti.")
            sys.exit(1)
        time.sleep(poll_sec)


def github_repo_exists(token: str, owner: str, repo_name: str) -> dict:
    headers = github_headers(token)
    url = f"https://api.github.com/repos/{owner}/{repo_name}"
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code == 404:
        return {}
    if resp.status_code >= 400:
        print("[ERRORE] Verifica repo origine GitHub fallita:", resp.text)
        sys.exit(1)
    return resp.json()


def upsert_github_file(token: str, owner: str, repo: str, path: str, content: str, message: str, branch: str):
    headers = github_headers(token)
    url_get = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    resp_get = requests.get(url_get, headers=headers, params={"ref": branch}, timeout=30)
    sha = None
    if resp_get.status_code == 200:
        sha = resp_get.json().get("sha")
    elif resp_get.status_code not in (404,):
        print("[ERRORE] Lettura file GitHub fallita:", resp_get.text)
        sys.exit(1)

    payload = {
        "message": message,
        "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
        "branch": branch,
    }
    if sha:
        payload["sha"] = sha

    resp_put = requests.put(url_get, headers=headers, json=payload, timeout=30)
    if resp_put.status_code >= 300:
        print("[ERRORE] Scrittura file GitHub fallita:", resp_put.text)
        sys.exit(1)


# ----------------- Generation utility -----------------


def ensure_settings_path(file_name: str) -> str:
    return f"settings/{file_name}"


def create_user_yaml_files(proj, branch: str, agency_cfg: dict, users_cfg: dict):
    users = ensure_users(users_cfg)
    for username in users:
        content_lines = [
            f"agency_ipa_code: '{agency_cfg['agency_ipa_code']}'",
            "db_host_name: ''",
            "db_name: ''",
            "db_port_number: ''",
            "db_pwd: ''",
            "db_username: ''",
            "delay_write_and_read_table_procedure: 150",
            f"professional_category_id: {agency_cfg['category_id']}",
            "root_path_global_common_transformation: ''",
            "root_path_unioncol: ''",
            "root_prj_folder: ''",
            f"short_name: '{agency_cfg['short_name_template']}'",
        ]
        file_path = ensure_settings_path(f"{username}_etlSetting.yml")
        upsert_gitlab_file(
            proj,
            file_path,
            "\n".join(content_lines) + "\n",
            branch,
            "Aggiungi configurazioni utente",
        )


def create_register_csv(proj, branch: str, register_cfg: dict):
    template = register_cfg.get("default_privacy_template", {}) if isinstance(register_cfg, dict) else {}
    headers = [
        "profileName",
        "picture",
        "fiscalCode",
        "vatNumber",
        "birthDate",
        "gender",
        "studioAddress",
        "studioEmail",
        "studioPec",
        "studioPecReginde",
        "studioPhone",
        "studioFax",
        "residenceAddress",
        "residenceEmail",
        "residencePec",
        "residencePecReginde",
        "residencePhone",
        "residenceFax",
        "professionalDomicileAddress",
        "professionalDomicileEmail",
        "professionalDomicilePec",
        "professionalDomicilePecReginde",
        "professionalDomicilePhone",
        "professionalDomicileFax",
        "taxDomicileAddress",
        "taxDomicileEmail",
        "taxDomicilePec",
        "taxDomicilePecReginde",
        "taxDomicilePhone",
        "taxDomicileFax",
        "mailingAddressAddress",
        "mailingAddressEmail",
        "mailingAddressPec",
        "mailingAddressPecReginde",
        "mailingAddressPhone",
        "mailingAddressFax",
        "studioMobilePhone",
        "residenceMobilePhone",
        "professionalDomicileMobilePhone",
        "taxDomicileMobilePhone",
        "mailingAddressMobilePhone",
    ]

    def val(key: str):
        v = template.get(key)
        return "" if v is None else str(v)

    row = [val(h) for h in headers]

    csv_lines = [
        ";".join([f'"{h}"' for h in headers]),
        ";".join([f'"{v}"' for v in row]),
    ]

    file_path = ensure_settings_path("privacy_default_template.csv")
    upsert_gitlab_file(
        proj,
        file_path,
        "\n".join(csv_lines) + "\n",
        branch,
        "Aggiungi template privacy",
    )


def build_register_csv_content(register_cfg: dict) -> str:
    template = register_cfg.get("default_privacy_template", {}) if isinstance(register_cfg, dict) else {}
    headers = [
        "profileName",
        "picture",
        "fiscalCode",
        "vatNumber",
        "birthDate",
        "gender",
        "studioAddress",
        "studioEmail",
        "studioPec",
        "studioPecReginde",
        "studioPhone",
        "studioFax",
        "residenceAddress",
        "residenceEmail",
        "residencePec",
        "residencePecReginde",
        "residencePhone",
        "residenceFax",
        "professionalDomicileAddress",
        "professionalDomicileEmail",
        "professionalDomicilePec",
        "professionalDomicilePecReginde",
        "professionalDomicilePhone",
        "professionalDomicileFax",
        "taxDomicileAddress",
        "taxDomicileEmail",
        "taxDomicilePec",
        "taxDomicilePecReginde",
        "taxDomicilePhone",
        "taxDomicileFax",
        "mailingAddressAddress",
        "mailingAddressEmail",
        "mailingAddressPec",
        "mailingAddressPecReginde",
        "mailingAddressPhone",
        "mailingAddressFax",
        "studioMobilePhone",
        "residenceMobilePhone",
        "professionalDomicileMobilePhone",
        "taxDomicileMobilePhone",
        "mailingAddressMobilePhone",
    ]

    def val(key: str):
        v = template.get(key)
        return "" if v is None else str(v)

    row = [val(h) for h in headers]

    csv_lines = [
        ";".join([f'"{h}"' for h in headers]),
        ";".join([f'"{v}"' for v in row]),
    ]
    return "\n".join(csv_lines) + "\n"


def extract_aoo_rows(protocol_cfg: dict):
    aoo_section = protocol_cfg.get("AOO", {}) if isinstance(protocol_cfg, dict) else {}
    num = int(aoo_section.get("number", 0) or 0)
    rows = []
    headers = [
        "accountable_email",
        "accountable_first_name",
        "accountable_last_name",
        "accountable_phone_number",
        "alboclassic_aoo_id",
        "date_creation",
        "name",
        "unicode",
    ]
    for i in range(1, num + 1):
        entry = aoo_section.get(f"AOO{i}", {})
        row = [
            entry.get(f"aoo{i}_accountable_email", ""),
            entry.get(f"aoo{i}_accountable_first_name", ""),
            entry.get(f"aoo{i}_accountable_last_name", ""),
            entry.get(f"aoo{i}_accountable_phone_number", ""),
            entry.get(f"aoo{i}_alboclassic_aoo_id", ""),
            entry.get(f"aoo{i}_date_creation", ""),
            entry.get(f"aoo{i}_name", ""),
            entry.get(f"aoo{i}_unicode", ""),
        ]
        rows.append(["" if v is None else v for v in row])
    return headers, rows


def extract_uo_rows(protocol_cfg: dict):
    uo_section = protocol_cfg.get("UO", {}) if isinstance(protocol_cfg, dict) else {}
    num = int(uo_section.get("number", 0) or 0)
    rows = []
    headers = [
        "accountable_first_name",
        "accountable_second_name",
        "alboclassic_uo_id",
        "albosmart_uo_id",
        "date_creation",
        "isDefault",
        "name",
        "unicode",
    ]
    for i in range(1, num + 1):
        entry = uo_section.get(f"UO{i}", {})
        row = [
            entry.get(f"uo{i}_accountable_first_name", ""),
            entry.get(f"uo{i}_accountable_second_name", ""),
            entry.get(f"uo{i}_alboclassic_uo_id", ""),
            entry.get(f"uo{i}_albosmart_uo_id", ""),
            entry.get(f"uo{i}_date_creation", ""),
            entry.get(f"uo{i}_isDefault", ""),
            entry.get(f"uo{i}_name", ""),
            entry.get(f"uo{i}_unicode", ""),
        ]
        rows.append(["" if v is None else v for v in row])
    return headers, rows


def create_protocol_csvs(proj, branch: str, protocol_cfg: dict):
    aoo_headers, aoo_rows = extract_aoo_rows(protocol_cfg)
    if aoo_rows:
        csv_lines = [
            ";".join([f'"{h}"' for h in aoo_headers]),
            *[";".join([f'"{v}"' for v in row]) for row in aoo_rows],
        ]
        upsert_gitlab_file(
            proj,
            ensure_settings_path("AOO.csv"),
            "\n".join(csv_lines) + "\n",
            branch,
            "Aggiungi CSV AOO",
        )

    uo_headers, uo_rows = extract_uo_rows(protocol_cfg)
    if uo_rows:
        csv_lines = [
            ";".join([f'"{h}"' for h in uo_headers]),
            *[";".join([f'"{v}"' for v in row]) for row in uo_rows],
        ]
        upsert_gitlab_file(
            proj,
            ensure_settings_path("UO.csv"),
            "\n".join(csv_lines) + "\n",
            branch,
            "Aggiungi CSV UO",
        )


def build_protocol_csv_contents(protocol_cfg: dict):
    aoo_headers, aoo_rows = extract_aoo_rows(protocol_cfg)
    uo_headers, uo_rows = extract_uo_rows(protocol_cfg)

    aoo_content = ""
    if aoo_rows:
        aoo_lines = [
            ";".join([f'"{h}"' for h in aoo_headers]),
            *[";".join([f'"{v}"' for v in row]) for row in aoo_rows],
        ]
        aoo_content = "\n".join(aoo_lines) + "\n"

    uo_content = ""
    if uo_rows:
        uo_lines = [
            ";".join([f'"{h}"' for h in uo_headers]),
            *[";".join([f'"{v}"' for v in row]) for row in uo_rows],
        ]
        uo_content = "\n".join(uo_lines) + "\n"

    return aoo_content, uo_content


# ----------------- Mode handlers -----------------


def mode_ls_remote_prj(cfg: dict, gitlab_token: str):
    proj_cfg = cfg["project_config"]
    gl_client = init_gitlab_client(proj_cfg["gitlab_url"], gitlab_token)
    projs = list_group_projects(gl_client, proj_cfg["gitlab_group_id"])

    print("Progetti nel gruppo:")
    for p in projs:
        print(" -", p.name)


def mode_ls_remote_prj_github(cfg: dict, github_token: str):
    proj_cfg = cfg["project_config"]
    github_owner = proj_cfg.get("github_owner", "").strip()
    if not github_owner:
        print("[ERRORE] 'project_config.github_owner' mancante nel config.yml.")
        sys.exit(1)

    headers = github_headers(github_token)
    url = f"https://api.github.com/orgs/{github_owner}/repos"
    resp = requests.get(url, headers=headers, params={"per_page": 100}, timeout=30)
    if resp.status_code >= 300:
        print("[ERRORE] Impossibile ottenere i repository GitHub:", resp.text)
        sys.exit(1)

    repos = resp.json()
    print(f"Repository per owner {github_owner}:")
    for r in repos:
        print(" -", r.get("name"))


def mode_create_remote_prj_gitlab(cfg: dict, gitlab_token: str, submode: str):
    if submode not in {"register", "protocol"}:
        print("[ERRORE] Specificare 'register' o 'protocol'.")
        sys.exit(1)

    agency_cfg = cfg["agency_config"]
    proj_cfg = cfg["project_config"]
    ensure_agency_fields(agency_cfg)

    gl_client = init_gitlab_client(proj_cfg["gitlab_url"], gitlab_token)

    origin_name = proj_cfg["gitlab_origin_remote_name"].strip()
    origin_project = find_origin_project(gl_client, proj_cfg["gitlab_group_id"], origin_name)

    new_project_name = derive_new_project_name(origin_project.name, agency_cfg["short_name_template"])
    print("Progetto sorgente:", origin_project.name)
    print("Nuovo progetto:", new_project_name)

    origin_repo_url = origin_project.http_url_to_repo
    gitlab_user = proj_cfg["gitlab_username"].strip()
    default_branch = proj_cfg.get("git_default_branch", "main")

    import_url = build_auth_url(origin_repo_url, gitlab_user, gitlab_token)
    new_gitlab_proj = create_gitlab_project_from_import(
        gl_client,
        proj_cfg["gitlab_group_id"],
        new_project_name,
        import_url,
        default_branch,
    )

    new_gitlab_proj = wait_for_gitlab_import(gl_client, new_gitlab_proj.id)
    print("Creato progetto GitLab:", new_gitlab_proj.http_url_to_repo)

    create_user_yaml_files(new_gitlab_proj, default_branch, agency_cfg, cfg.get("users", {}))

    if submode == "register":
        create_register_csv(new_gitlab_proj, default_branch, cfg.get("register_config", {}))
    else:
        create_protocol_csvs(new_gitlab_proj, default_branch, cfg.get("protocol_config", {}))

    print("Operazione completata su GitLab.")


def mode_create_remote_prj_github(cfg: dict, github_token: str, submode: str):
    if submode not in {"register", "protocol"}:
        print("[ERRORE] Specificare 'register' o 'protocol'.")
        sys.exit(1)

    agency_cfg = cfg["agency_config"]
    proj_cfg = cfg["project_config"]
    ensure_agency_fields(agency_cfg)

    github_owner = proj_cfg.get("github_owner", "").strip()
    if not github_owner:
        print("[ERRORE] 'project_config.github_owner' mancante nel config.yml.")
        sys.exit(1)

    origin_name = proj_cfg["gitlab_origin_remote_name"].strip()
    origin_repo = github_repo_exists(github_token, github_owner, origin_name)
    if not origin_repo:
        print(f"[ERRORE] Il progetto sorgente '{origin_name}' non esiste in GitHub (owner {github_owner}).")
        sys.exit(1)

    new_project_name = derive_new_project_name(origin_repo.get("name", origin_name), agency_cfg["short_name_template"])
    print("Progetto sorgente GitHub:", origin_repo.get("full_name"))
    print("Nuovo progetto GitHub:", f"{github_owner}/{new_project_name}")

    origin_repo_url = origin_repo.get("clone_url")
    default_branch = proj_cfg.get("git_default_branch", "main")

    github_private = proj_cfg.get("github_visibility", "private").lower() != "public"

    created_repo = create_github_repo(
        github_token,
        github_owner,
        new_project_name,
        private=github_private,
    )

    start_github_import(
        github_token,
        github_owner,
        new_project_name,
        origin_repo_url,
        github_owner,
        github_token,
    )

    wait_for_github_import(github_token, github_owner, new_project_name)
    set_github_default_branch(github_token, github_owner, new_project_name, default_branch)

    # Preparazione contenuti
    users_cfg = cfg.get("users", {})
    users = ensure_users(users_cfg)
    register_csv_content = build_register_csv_content(cfg.get("register_config", {}))
    aoo_content, uo_content = build_protocol_csv_contents(cfg.get("protocol_config", {}))

    for username in users:
        content_lines = [
            f"agency_ipa_code: '{agency_cfg['agency_ipa_code']}'",
            "db_host_name: ''",
            "db_name: ''",
            "db_port_number: ''",
            "db_pwd: ''",
            "db_username: ''",
            "delay_write_and_read_table_procedure: 150",
            f"professional_category_id: {agency_cfg['category_id']}",
            "root_path_global_common_transformation: ''",
            "root_path_unioncol: ''",
            "root_prj_folder: ''",
            f"short_name: '{agency_cfg['short_name_template']}'",
        ]
        upsert_github_file(
            github_token,
            github_owner,
            new_project_name,
            ensure_settings_path(f"{username}_etlSetting.yml"),
            "\n".join(content_lines) + "\n",
            "Aggiungi configurazioni utente",
            default_branch,
        )

    if submode == "register":
        upsert_github_file(
            github_token,
            github_owner,
            new_project_name,
            ensure_settings_path("privacy_default_template.csv"),
            register_csv_content,
            "Aggiungi template privacy",
            default_branch,
        )
    else:
        if aoo_content:
            upsert_github_file(
                github_token,
                github_owner,
                new_project_name,
                ensure_settings_path("AOO.csv"),
                aoo_content,
                "Aggiungi CSV AOO",
                default_branch,
            )
        if uo_content:
            upsert_github_file(
                github_token,
                github_owner,
                new_project_name,
                ensure_settings_path("UO.csv"),
                uo_content,
                "Aggiungi CSV UO",
                default_branch,
            )

    print("Creato progetto GitHub:", created_repo.get("html_url"))
    print("Operazione completata su GitHub.")


# ----------------- Main -----------------


def main():
    if len(sys.argv) < 2:
        print("Uso:")
        print("  etl_prj_manager.py ls-remote-prj <gitlab|github>")
        print("  etl_prj_manager.py create-remote-prj <gitlab|github> <register|protocol>")
        sys.exit(1)

    mode = sys.argv[1]

    cfg = load_config("config.yml")

    if mode == "ls-remote-prj":
        if len(sys.argv) < 3:
            print("[ERRORE] Specificare piattaforma (gitlab|github).")
            sys.exit(1)
        platform = sys.argv[2].lower()
        if platform == "gitlab":
            gitlab_token = require_env_var("GITLAB_TOKEN")
            mode_ls_remote_prj(cfg, gitlab_token)
        elif platform == "github":
            github_token = require_env_var("GITHUB_TOKEN")
            mode_ls_remote_prj_github(cfg, github_token)
        else:
            print("[ERRORE] Piattaforma non valida. Usa gitlab o github.")
            sys.exit(1)
    elif mode == "create-remote-prj":
        if len(sys.argv) < 4:
            print("[ERRORE] Specificare piattaforma (gitlab|github) e 'register' o 'protocol'.")
            sys.exit(1)
        platform = sys.argv[2].lower()
        submode = sys.argv[3]
        if platform == "gitlab":
            gitlab_token = require_env_var("GITLAB_TOKEN")
            mode_create_remote_prj_gitlab(cfg, gitlab_token, submode)
        elif platform == "github":
            github_token = require_env_var("GITHUB_TOKEN")
            mode_create_remote_prj_github(cfg, github_token, submode)
        else:
            print("[ERRORE] Piattaforma non valida. Usa gitlab o github.")
            sys.exit(1)
    else:
        print("Modalita' non valida.")
        sys.exit(1)


if __name__ == "__main__":
    main()
