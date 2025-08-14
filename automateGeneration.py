#!/usr/bin/env python3
"""
automateGeneration.py

Runs the same processing logic as your Streamlit "Generate Job Orders & Invoices" button
for each client in sequence, waiting 60 seconds between clients.
Intended to be executed from CI (GitHub Actions) or cron.

REQUIRES these environment variables (set them in GitHub Secrets or CI env):
- SUPABASE_URL
- SUPABASE_API_KEY
- SUPABASE_STORAGE_BUCKET (optional; default from code below)
- SUPABASE_TABLE_NAME (optional; default "orders")
- GSHEET_SERVICE_ACCOUNT_JSON  -> base64-encoded service account JSON OR raw JSON string
- GSHEET_SPREADSHEET_ID
- GSHEET_SHEET_NAME (optional; default "Saved")
- OPTIONAL: LOG_LEVEL (DEBUG/INFO; default INFO)

Your repo must contain the processing modules:
- goodsmartInvoices.generate_invoice_excel
- halanInvoices.build_master_and_invoices_bytes
- rabbitInvoices.rabbitInvoices
- pdfsToExcels.process_talabat_invoices
- breadfastInvoices.process_breadfast_invoice
and your config module with translation_dict, categories_dict, branches_dict, branches_translation_tlbt, columns
"""

import os
import time
import json
import base64
import logging
from io import BytesIO
from zipfile import ZipFile
from tempfile import NamedTemporaryFile
from typing import Optional, List, Dict, Any

import requests
import gspread
from google.oauth2.service_account import Credentials

# import your processing functions & config (assumes these modules are in repo)
from goodsmartInvoices import generate_invoice_excel
from halanInvoices import build_master_and_invoices_bytes
from rabbitInvoices import rabbitInvoices
from pdfsToExcels import process_talabat_invoices
from breadfastInvoices import process_breadfast_invoice
from config import (
    translation_dict,
    categories_dict,
    branches_dict,
    branches_translation_tlbt,
    columns
)

# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# --- Environment & Supabase Setup ---
SUPABASE_URL = os.getenv("https://rabwvltxgpdyvpmygdtc.supabase.co")
SUPABASE_API_KEY = os.getenv("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJhYnd2bHR4Z3BkeXZwbXlnZHRjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDkyMzg4MTQsImV4cCI6MjA2NDgxNDgxNH0.hnQYr3jL0rLTNOGXE0EgF9wmd_bynff6JXtqwjCOc6Y")
STORAGE_BUCKET = os.getenv("SUPABASE_STORAGE_BUCKET", "order_files")
TABLE_NAME = os.getenv("SUPABASE_TABLE_NAME", "orders")

if not SUPABASE_URL or not SUPABASE_API_KEY:
    logger.error("Missing SUPABASE_URL or SUPABASE_API_KEY environment variables.")
    raise SystemExit(1)

SUPABASE_API_URL = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}"
SUPABASE_HEADERS = {
    "accept": "*/*",
    "apikey": SUPABASE_API_KEY,
    "authorization": f"Bearer {SUPABASE_API_KEY}"
}

# --- Google Sheets config ---
GSHEET_SERVICE_ACCOUNT_JSON = os.getenv("GSHEET_SERVICE_ACCOUNT_JSON")
GSHEET_SPREADSHEET_ID = os.getenv("GSHEET_SPREADSHEET_ID")
GSHEET_SHEET_NAME = os.getenv("GSHEET_SHEET_NAME", "Saved")

if not GSHEET_SERVICE_ACCOUNT_JSON or not GSHEET_SPREADSHEET_ID:
    logger.error("Missing Google Sheets environment variables (GSHEET_SERVICE_ACCOUNT_JSON or GSHEET_SPREADSHEET_ID).")
    raise SystemExit(1)

def load_gspread_client():
    # support either base64-encoded JSON or plain JSON string
    sa_json = GSHEET_SERVICE_ACCOUNT_JSON
    try:
        # if looks base64-ish, decode; otherwise assume plain JSON
        try:
            decoded = base64.b64decode(sa_json).decode("utf-8")
            info = json.loads(decoded)
        except Exception:
            info = json.loads(sa_json)
    except Exception as e:
        logger.exception("Failed to parse GSHEET_SERVICE_ACCOUNT_JSON: %s", e)
        raise

    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    return gc

def read_invoice_number_from_sheet(gc) -> int:
    sh = gc.open_by_key(GSHEET_SPREADSHEET_ID)
    ws = sh.worksheet(GSHEET_SHEET_NAME)
    val = ws.acell("A1").value
    try:
        num = int(val)
    except Exception:
        logger.warning("Invalid invoice number in sheet (A1='%s'), defaulting to 1", val)
        num = 1
    return num

def write_invoice_number_to_sheet(gc, new_value: int):
    sh = gc.open_by_key(GSHEET_SPREADSHEET_ID)
    ws = sh.worksheet(GSHEET_SHEET_NAME)
    ws.update_acell("A1", str(new_value))

# --- Supabase helpers (same semantics as your Streamlit app) ---
def fetch_pending_orders() -> List[Dict[str, Any]]:
    resp = requests.get(f"{SUPABASE_API_URL}?select=*&order=created_at.desc", headers=SUPABASE_HEADERS, timeout=60)
    resp.raise_for_status()
    orders = [o for o in resp.json() if o.get("status") == "Pending"]
    return orders

def download_from_url(url: str) -> bytes:
    r = requests.get(url, timeout=90)
    r.raise_for_status()
    return r.content

def upload_order_and_metadata(
    file_bytes: bytes,
    filename: str,
    client: str,
    order_type: str,
    order_date: str,
    delivery_date: str,
    po_number: Optional[int] = None,
    city: Optional[str] = None,
    status: str = "Pending"
):
    # upload to storage
    object_name = f"{int(delivery_date.replace('-', ''))}-{filename}"
    storage_url = f"{SUPABASE_URL}/storage/v1/object/{STORAGE_BUCKET}/{object_name}"

    with NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name
    try:
        with open(tmp_path, "rb") as f:
            up = requests.post(
                storage_url,
                headers={"apikey": SUPABASE_API_KEY, "authorization": f"Bearer {SUPABASE_API_KEY}"},
                files={"file": (filename, f, "application/octet-stream")},
                timeout=120
            )
        up.raise_for_status()
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    file_url = f"{SUPABASE_URL}/storage/v1/object/public/{STORAGE_BUCKET}/{object_name}"
    payload = [{
        "client": client,
        "order_type": order_type,
        "order_date": order_date,
        "delivery_date": delivery_date,
        "status": status,
        "file_urls": [file_url],
        "city": city,
        "po_number": po_number
    }]
    ins = requests.post(
        SUPABASE_API_URL,
        headers={**SUPABASE_HEADERS, "content-type": "application/json", "prefer": "return=representation"},
        json=payload,
        timeout=60
    )
    ins.raise_for_status()
    return ins.json()

def mark_purchase_order_done(client: str, delivery_date: str, city: Optional[str] = None):
    headers = {"apikey": SUPABASE_API_KEY, "authorization": f"Bearer {SUPABASE_API_KEY}", "content-type": "application/json"}
    params = {
        "client": f"eq.{client}",
        "order_type": "eq.Purchase Order",
        "delivery_date": f"eq.{delivery_date}",
        "status": "eq.Pending"
    }
    if city:
        params["city"] = f"eq.{city}"

    resp = requests.get(f"{SUPABASE_URL}/rest/v1/orders", headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    orders = resp.json()
    for order in orders:
        oid = order.get("id")
        patch = requests.patch(
            f"{SUPABASE_URL}/rest/v1/orders?id=eq.{oid}",
            headers=headers,
            json={"status": "Done"},
            timeout=30
        )
        patch.raise_for_status()

# --- Clients order (must end with Talabat) ---
CLIENT_SEQUENCE = [
    ("goodsmart", "GoodsMart"),
    ("halan", "Halan"),
    ("khateer", "Khateer"),
    ("rabbit", "Rabbit"),
    ("breadfast", "Breadfast"),
    ("talabat", "Talabat"),  # last
]

def process_client(selected_key: str, selected_label: str, invoice_number_holder: Dict[str, int]):
    """
    Process all pending Purchase Orders for a given client.
    invoice_number_holder is a dict used to mutate the invoice counter across calls: {'n': 123}
    """
    logger.info("Processing client: %s (%s)", selected_label, selected_key)
    orders = fetch_pending_orders()
    if not orders:
        logger.info("No pending orders in DB at all.")
        return

    client_orders = [o for o in orders if o.get("order_type") == "Purchase Order" and o.get("client", "").strip().lower() == selected_key]
    if not client_orders:
        logger.info("No pending Purchase Orders for client %s", selected_label)
        return

    for order in client_orders:
        try:
            for file_url in order.get("file_urls", []):
                file_name = os.path.basename(file_url)
                logger.info("-> Processing file %s for order id=%s", file_name, order.get("id"))
                data = download_from_url(file_url)

                # --- goodsmart ---
                if selected_key == "goodsmart":
                    excel_bytes, d_date = generate_invoice_excel(
                        excel_bytes=data,
                        invoice_number=invoice_number_holder["n"],
                        delivery_date=order.get("delivery_date"),
                        po_value=order.get("po_number")
                    )
                    invoice_number_holder["n"] += 1
                    mark_purchase_order_done("GoodsMart", order.get("delivery_date"), order.get("city"))
                    for otype in ["Invoice", "Job Order"]:
                        upload_order_and_metadata(
                            file_bytes=excel_bytes,
                            filename=f"GoodsMart_{otype}_{d_date}.xlsx",
                            client="GoodsMart",
                            order_type=otype,
                            order_date=order.get("order_date"),
                            delivery_date=order.get("delivery_date"),
                            po_number=order.get("po_number"),
                            city=order.get("city")
                        )

                # --- halan ---
                elif selected_key == "halan":
                    excel_bytes, d_date = build_master_and_invoices_bytes(
                        excel_bytes=data,
                        invoice_number=invoice_number_holder["n"],
                        delivery_date=order.get("delivery_date"),
                        po_value=order.get("po_number")
                    )
                    invoice_number_holder["n"] += 5
                    mark_purchase_order_done("Halan", order.get("delivery_date"), order.get("city"))
                    for otype in ["Invoice", "Job Order"]:
                        upload_order_and_metadata(
                            file_bytes=excel_bytes,
                            filename=f"Halan_{d_date}_{otype.replace(' ', '_')}.xlsx",
                            client="Halan",
                            order_type=otype,
                            order_date=order.get("order_date"),
                            delivery_date=order.get("delivery_date"),
                            po_number=order.get("po_number"),
                            city=order.get("city")
                        )

                # --- khateer & rabbit (use rabbitInvoices) ---
                elif selected_key in ("khateer", "rabbit"):
                    zip_bytes, idx = rabbitInvoices(
                        data,
                        invoice_number_holder["n"],
                        order.get("delivery_date"),
                        branches_translation={
                            "ميفيدا": "Mevida",
                            "فرع المعادي": "MAADI",
                            "فرع الدقي": "MOHANDSEEN",
                            "فرع الرحاب": "Rehab",
                            "فرع التجمع": "TGAMOE",
                            "فرع مصر الجديدة": "MASR GEDIDA",
                            "فرع مدينة نصر": "Nasr City",
                            "اكتوبر٢": "OCTOBER",
                            "فرع دريم": "Dream",
                            "فرع زايد": "ZAYED",
                            "فرع سوديك": "Sodic",
                            "مدينتي": "Madinaty"
                        }
                    )
                    invoice_number_holder["n"] += idx + 1
                    z = ZipFile(BytesIO(zip_bytes))
                    inner = None; excels = []
                    for n in z.namelist():
                        c = z.read(n)
                        if n.lower().endswith('.zip'): inner = c
                        elif n.lower().endswith('.xlsx'): excels.append((n,c))
                    if inner:
                        upload_order_and_metadata(inner, f"{selected_label}_Invoice_{order['delivery_date']}.zip",
                                                  selected_label, "Invoice", order['order_date'], order['delivery_date'], order.get('po_number'), order.get('city'))
                    if excels:
                        newz = BytesIO()
                        with ZipFile(newz,'w') as z2:
                            for n,c in excels: z2.writestr(n,c)
                        mark_purchase_order_done(selected_label, order.get("delivery_date"), order.get("city"))
                        upload_order_and_metadata(newz.getvalue(), f"{selected_label}_JobOrder_{order['delivery_date']}.zip",
                                                  selected_label, "Job Order", order['order_date'], order['delivery_date'], order.get('po_number'), order.get('city'))

                # --- talabat ---
                elif selected_key == "talabat":
                    d_date = order.get("delivery_date")
                    zip_bytes, offset = process_talabat_invoices(
                        zip_file_bytes=data,
                        invoice_date=d_date,
                        base_invoice_number=invoice_number_holder["n"],
                        translation_dict=translation_dict,
                        categories_dict=categories_dict,
                        branches_dict=branches_dict,
                        branches_translation_tlbt=branches_translation_tlbt,
                        columns=columns
                    )
                    invoice_number_holder["n"] += offset
                    z = ZipFile(BytesIO(zip_bytes))
                    inner = None; excels = []
                    for n in z.namelist():
                        c = z.read(n)
                        if n.lower().endswith('.zip'): inner = c
                        elif n.lower().endswith('.xlsx'): excels.append((n,c))
                    if inner:
                        upload_order_and_metadata(inner, f"Talabat_Invoice_{d_date}.zip",
                                                  "Talabat", "Invoice", order['order_date'], d_date, order.get('po_number'), order.get('city'))
                    if excels:
                        newz = BytesIO()
                        with ZipFile(newz,'w') as z2:
                            for n,c in excels: z2.writestr(n,c)
                        mark_purchase_order_done("Talabat", d_date, order.get("city"))
                        upload_order_and_metadata(newz.getvalue(), f"Talabat_JobOrder_{d_date}.zip",
                                                  "Talabat", "Job Order", order['order_date'], d_date, order.get('po_number'), order.get('city'))

                # --- breadfast ---
                elif selected_key == "breadfast":
                    city = order.get("city")
                    d_date = order.get("delivery_date")
                    zip_bytes = process_breadfast_invoice(
                        city=city,
                        pdf_file_bytes=data,
                        invoice_number=invoice_number_holder["n"],
                        delivery_date_str=d_date
                    )
                    invoice_number_holder["n"] += (1 if city == "Mansoura" else 2)
                    z = ZipFile(BytesIO(zip_bytes))
                    jobf = []; invf = []
                    for n in z.namelist():
                        c = z.read(n)
                        if 'مجمع' in n: jobf.append((n,c))
                        else: invf.append((n,c))
                    if jobf:
                        jz = BytesIO()
                        with ZipFile(jz,'w') as z2:
                            for n,c in jobf: z2.writestr(n,c)
                        upload_order_and_metadata(jz.getvalue(), f"Breadfast_JobOrder_{city}_{d_date}.zip",
                                                  "Breadfast", "Job Order", order['order_date'], d_date, order.get('po_number'), city)
                    if invf:
                        iz = BytesIO()
                        with ZipFile(iz,'w') as z2:
                            for n,c in invf: z2.writestr(n,c)
                        upload_order_and_metadata(iz.getvalue(), f"Breadfast_Invoices_{city}_{d_date}.zip",
                                                  "Breadfast", "Invoice", order['order_date'], d_date, order.get('po_number'), city)
                    mark_purchase_order_done("Breadfast", d_date, city)

                else:
                    logger.warning("Unknown client key: %s", selected_key)

        except Exception as e:
            logger.exception("Error processing order %s: %s", order.get("id"), e)

def main():
    logger.info("Starting automated generation run.")
    gc = load_gspread_client()
    invoice_num = read_invoice_number_from_sheet(gc)
    invoice_holder = {"n": invoice_num}  # mutable holder

    for key, label in CLIENT_SEQUENCE:
        try:
            process_client(key, label, invoice_holder)
        except Exception as e:
            logger.exception("Unhandled error while processing client %s: %s", label, e)

        # wait 60 seconds before next client (as requested)
        logger.info("Sleeping 60 seconds before next client...")
        time.sleep(60)

    # update sheet with new invoice number
    logger.info("Updating invoice number in Google Sheet to %s", invoice_holder["n"])
    try:
        write_invoice_number_to_sheet(gc, invoice_holder["n"])
    except Exception:
        logger.exception("Failed updating sheet with invoice number.")

    logger.info("Automated generation run finished.")

if __name__ == "__main__":
    main()
