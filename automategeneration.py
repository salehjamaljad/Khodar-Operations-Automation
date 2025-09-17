#!/usr/bin/env python3
import os
import time
from io import BytesIO
from zipfile import ZipFile
from typing import Optional
from datetime import datetime
import json
import requests
import gspread

# local imports - make sure these modules are available in the same project
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

# === Supabase Configuration ===
SUPABASE_URL = "https://rabwvltxgpdyvpmygdtc.supabase.co"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJhYnd2bHR4Z3BkeXZwbXlnZHRjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDkyMzg4MTQsImV4cCI6MjA2NDgxNDgxNH0.hnQYr3jL0rLTNOGXE0EgF9wmd_bynff6JXtqwjCOc6Y"
AUTHORIZATION = f"Bearer {API_KEY}"
STORAGE_BUCKET = "order_files"
TABLE_NAME = "orders"
SUPABASE_API_URL = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}"
SUPABASE_HEADERS = {
    "accept": "*/*",
    "apikey": API_KEY,
    "authorization": AUTHORIZATION
}

# === Google Sheets Connection (gspread, using service account JSON from env var) ===
# Make sure you set GSHEET_SERVICE_ACCOUNT_JSON to the full JSON of your service account
service_account_info = json.loads(os.environ["GSHEET_SERVICE_ACCOUNT_JSON"])
gc = gspread.service_account_from_dict(service_account_info)

# Replace with your actual spreadsheet name
SPREADSHEET_NAME = "Khodar Pricing Control"
worksheet = gc.open(SPREADSHEET_NAME).worksheet("Saved")

# read invoice number from A2
a2 = worksheet.acell("A2").value
invoice_number = int(str(a2).strip())

# Mapping from local selected_key (lowercase) to the exact client name stored in DB/UI.
# IMPORTANT: Khateer must be "Khateer" (capital K) in DB & UI according to your note.
CLIENT_DB_MAPPING = {
    "khateer": "Khateer",
    "goodsmart": "GoodsMart",
    "halan": "Halan",
    "rabbit": "Rabbit",
    "breadfast": "Breadfast",
    "talabat": "Talabat"
}

# --- Helpers ---
def normalize_date_for_payload(d):
    """Try to convert various date representations to YYYY-MM-DD string. Return None if input is None."""
    if d is None:
        return None
    try:
        # Accept ISO-like strings and datetimes
        if isinstance(d, (datetime, )):
            return d.date().isoformat()
        return datetime.fromisoformat(str(d)).date().isoformat()
    except Exception:
        # fallback: attempt to parse common formats or return original string
        try:
            return datetime.strptime(str(d), "%Y-%m-%d").date().isoformat()
        except Exception:
            return str(d)

def mark_purchase_order_done(client: str, delivery_date: str, city: Optional[str] = None):
    """
    Mark matching orders as Done. `client` MUST be the exact DB value (case-sensitive).
    Only add city filter when city is provided.
    """
    headers = {"apikey": API_KEY, "authorization": AUTHORIZATION, "content-type": "application/json"}
    params = {
        "client": f"eq.{client}",
        "order_type": "eq.Purchase Order",
        "delivery_date": f"eq.{delivery_date}",
        "status": "eq.Pending"
    }
    if city:
        params["city"] = f"eq.{city}"

    try:
        resp = requests.get(f"{SUPABASE_URL}/rest/v1/orders", headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        orders = resp.json()
        if not orders:
            print(f"No pending Purchase Order rows found for client={client}, delivery_date={delivery_date}, city={city}")
            return
        for order in orders:
            oid = order.get("id")
            if not oid:
                continue
            patch = requests.patch(
                f"{SUPABASE_URL}/rest/v1/orders?id=eq.{oid}",
                headers=headers,
                json={"status": "Done"},
                timeout=30
            )
            if not patch.ok:
                print("Patch failed:", patch.status_code, patch.text)
                patch.raise_for_status()
            else:
                print(f"Marked order id={oid} as Done (client={client}).")
    except Exception as e:
        print("Error in mark_purchase_order_done:", e)
        raise

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
    """
    Uploads file to Supabase storage and inserts a row into the orders table.
    - Only includes fields that are not None (avoids sending city/po_number for Khateer).
    - Logs Supabase response body on error for easier debugging.
    - Returns the INSERT response JSON on success.
    """
    order_date_n = normalize_date_for_payload(order_date)
    delivery_date_n = normalize_date_for_payload(delivery_date)

    # choose an object name using delivery_date if available (fall back to timestamp)
    if delivery_date_n:
        try:
            obj_prefix = datetime.strptime(delivery_date_n, "%Y-%m-%d").strftime("%Y%m%d")
        except Exception:
            obj_prefix = str(int(time.time() * 1000))
    else:
        obj_prefix = str(int(time.time() * 1000))

    object_name = f"{obj_prefix}-{filename}"
    storage_url = f"{SUPABASE_URL}/storage/v1/object/{STORAGE_BUCKET}/{object_name}"

    # upload to storage
    from tempfile import NamedTemporaryFile
    try:
        with NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name
        with open(tmp_path, "rb") as f:
            up = requests.post(
                storage_url,
                headers={"apikey": API_KEY, "authorization": AUTHORIZATION},
                files={"file": (filename, f, "application/octet-stream")},
                timeout=60
            )
        os.remove(tmp_path)
        if not up.ok:
            print("Storage upload failed:", up.status_code, up.text)
            up.raise_for_status()
    except Exception as e:
        print("Storage upload exception:", str(e))
        raise

    # public URL (adjust if your storage setup is private)
    file_url = f"{SUPABASE_URL}/storage/v1/object/public/{STORAGE_BUCKET}/{object_name}"

    payload_obj = {
        "client": client,
        "order_type": order_type,
        "order_date": order_date_n,
        "delivery_date": delivery_date_n,
        "status": status,
        "file_urls": [file_url],
        "city": city,
        "po_number": (int(po_number) if po_number not in (None, "") else None)
    }

    # remove None values so we don't send fields that don't exist for some clients (e.g., Khateer)
    payload = {k: v for k, v in payload_obj.items() if v is not None}

    headers = {
        "apikey": API_KEY,
        "Authorization": AUTHORIZATION,
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }

    try:
        ins = requests.post(SUPABASE_API_URL, headers=headers, json=payload, timeout=30)
        if not ins.ok:
            print("Supabase insert failed:", ins.status_code)
            print("Response body:", ins.text)
            ins.raise_for_status()
        print(f"Inserted metadata row for client={client}, order_type={order_type}, delivery_date={delivery_date_n}")
        return ins.json()
    except requests.exceptions.RequestException as e:
        print("Error posting metadata to Supabase:", str(e))
        raise

def fetch_pending_orders():
    try:
        resp = requests.get(SUPABASE_API_URL + "?select=*&order=created_at.desc", headers=SUPABASE_HEADERS, timeout=30)
        resp.raise_for_status()
        return [o for o in resp.json() if o.get("status") == "Pending"]
    except Exception as e:
        print("Error fetching pending orders:", e)
        raise

def download_from_url(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content

def process_client(selected_key: str, invoice_number: int) -> int:
    """
    Processes pending Purchase Orders for the given client key (case-insensitive).
    selected_key should be one of: "khateer", "goodsmart", "halan", "rabbit", "breadfast", "talabat"
    """
    orders = fetch_pending_orders()
    if not orders:
        print("No pending orders found.")
        return invoice_number

    sk_lower = selected_key.lower()
    db_client_name = CLIENT_DB_MAPPING.get(sk_lower, selected_key)

    for order in orders:
        if order.get("order_type") != "Purchase Order":
            continue

        order_client_raw = str(order.get("client", "")).strip()
        # compare case-insensitively
        if order_client_raw.lower() != sk_lower:
            continue

        for file_url in order.get("file_urls", []):
            file_name = os.path.basename(file_url)
            print(f"🟢 Processing: {file_name} (client: {order_client_raw})")
            try:
                data = download_from_url(file_url)
            except Exception as e:
                print(f"Error downloading {file_url}: {e}")
                continue

            try:
                # ----- GoodsMart / goodsmart -----
                if sk_lower == "goodsmart":
                    excel_bytes, d_date = generate_invoice_excel(
                        excel_bytes=data,
                        invoice_number=invoice_number,
                        delivery_date=order.get("delivery_date"),
                        po_value=order.get("po_number")
                    )
                    invoice_number += 1
                    # Use DB client name when marking done
                    mark_purchase_order_done(db_client_name, order.get("delivery_date"), order.get("city"))
                    for otype in ["Invoice", "Job Order"]:
                        upload_order_and_metadata(
                            file_bytes=excel_bytes,
                            filename=f"{db_client_name}_{otype}_{d_date}.xlsx",
                            client=db_client_name,
                            order_type=otype,
                            order_date=order.get("order_date"),
                            delivery_date=order.get("delivery_date"),
                            po_number=order.get("po_number"),
                            city=order.get("city")
                        )

                # ----- Halan -----
                elif sk_lower == "halan":
                    excel_bytes, d_date = build_master_and_invoices_bytes(
                        excel_bytes=data,
                        invoice_number=invoice_number,
                        delivery_date=order.get("delivery_date"),
                        po_value=order.get("po_number")
                    )
                    invoice_number += 5
                    mark_purchase_order_done(db_client_name, order.get("delivery_date"), order.get("city"))
                    for otype in ["Invoice", "Job Order"]:
                        upload_order_and_metadata(
                            file_bytes=excel_bytes,
                            filename=f"{db_client_name}_{d_date}_{otype.replace(' ', '_')}.xlsx",
                            client=db_client_name,
                            order_type=otype,
                            order_date=order.get("order_date"),
                            delivery_date=order.get("delivery_date"),
                            po_number=order.get("po_number"),
                            city=order.get("city")
                        )

                # ----- Khateer (special: no city, no po_number fields in DB) -----
                elif sk_lower == "khateer":
                    zip_bytes, idx = rabbitInvoices(
                        data,
                        invoice_number,
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
                    invoice_number += idx + 1
                    z = ZipFile(BytesIO(zip_bytes))
                    inner = None; excels = []
                    for n in z.namelist():
                        c = z.read(n)
                        if n.lower().endswith('.zip'):
                            inner = c
                        elif n.lower().endswith('.xlsx'):
                            excels.append((n, c))

                    # When uploading for Khateer, DO NOT send city or po_number (they don't exist for Khateer).
                    if inner:
                        upload_order_and_metadata(inner, f"{sk_lower}_Invoice_{order['delivery_date']}.zip",
                                                  client=db_client_name,
                                                  order_type="Invoice",
                                                  order_date=order.get('order_date'),
                                                  delivery_date=order.get('delivery_date'),
                                                  po_number=None,
                                                  city=None)

                    if excels:
                        newz = BytesIO()
                        with ZipFile(newz, 'w') as z2:
                            for n, c in excels:
                                z2.writestr(n, c)
                        # mark done using exact DB client name
                        mark_purchase_order_done(db_client_name, order.get("delivery_date"))
                        upload_order_and_metadata(newz.getvalue(), f"{sk_lower}_JobOrder_{order['delivery_date']}.zip",
                                                  client=db_client_name,
                                                  order_type="Job Order",
                                                  order_date=order.get('order_date'),
                                                  delivery_date=order.get('delivery_date'),
                                                  po_number=None,
                                                  city=None)

                # ----- Rabbit (other client) -----
                elif sk_lower == "rabbit":
                    zip_bytes, idx = rabbitInvoices(
                        data,
                        invoice_number,
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
                    invoice_number += idx + 1
                    z = ZipFile(BytesIO(zip_bytes))
                    inner = None; excels = []
                    for n in z.namelist():
                        c = z.read(n)
                        if n.lower().endswith('.zip'):
                            inner = c
                        elif n.lower().endswith('.xlsx'):
                            excels.append((n, c))
                    if inner:
                        upload_order_and_metadata(inner, f"{sk_lower}_Invoice_{order['delivery_date']}.zip",
                                                  client=db_client_name,
                                                  order_type="Invoice",
                                                  order_date=order.get('order_date'),
                                                  delivery_date=order.get('delivery_date'),
                                                  po_number=order.get('po_number'),
                                                  city=order.get('city'))
                    if excels:
                        newz = BytesIO()
                        with ZipFile(newz, 'w') as z2:
                            for n, c in excels:
                                z2.writestr(n, c)
                        mark_purchase_order_done(db_client_name, order.get("delivery_date"), order.get("city"))
                        upload_order_and_metadata(newz.getvalue(), f"{sk_lower}_JobOrder_{order['delivery_date']}.zip",
                                                  client=db_client_name,
                                                  order_type="Job Order",
                                                  order_date=order.get('order_date'),
                                                  delivery_date=order.get('delivery_date'),
                                                  po_number=order.get('po_number'),
                                                  city=order.get('city'))

                # ----- Talabat -----
                elif sk_lower == "talabat":
                    d_date = order.get("delivery_date")
                    zip_bytes, offset = process_talabat_invoices(
                        zip_file_bytes=data,
                        invoice_date=d_date,
                        base_invoice_number=invoice_number,
                        translation_dict=translation_dict,
                        categories_dict=categories_dict,
                        branches_dict=branches_dict,
                        branches_translation_tlbt=branches_translation_tlbt,
                        columns=columns
                    )
                    invoice_number += offset
                    z = ZipFile(BytesIO(zip_bytes))
                    inner = None; excels = []
                    for n in z.namelist():
                        c = z.read(n)
                        if n.lower().endswith('.zip'):
                            inner = c
                        elif n.lower().endswith('.xlsx'):
                            excels.append((n, c))
                    if inner:
                        upload_order_and_metadata(inner, f"Talabat_Invoice_{d_date}.zip",
                                                  client=db_client_name,
                                                  order_type="Invoice",
                                                  order_date=order['order_date'],
                                                  delivery_date=d_date,
                                                  po_number=order.get('po_number'),
                                                  city=order.get('city'))
                    if excels:
                        newz = BytesIO()
                        with ZipFile(newz, 'w') as z2:
                            for n, c in excels:
                                z2.writestr(n, c)
                        mark_purchase_order_done(db_client_name, d_date, order.get("city"))
                        upload_order_and_metadata(newz.getvalue(), f"Talabat_JobOrder_{d_date}.zip",
                                                  client=db_client_name,
                                                  order_type="Job Order",
                                                  order_date=order['order_date'],
                                                  delivery_date=d_date,
                                                  po_number=order.get('po_number'),
                                                  city=order.get('city'))

                # ----- Breadfast -----
                elif sk_lower == "breadfast":
                    city = order.get("city")
                    d_date = order.get("delivery_date")
                    zip_bytes = process_breadfast_invoice(
                        city=city,
                        pdf_file_bytes=data,
                        invoice_number=invoice_number,
                        delivery_date_str=d_date
                    )
                    invoice_number += (1 if city == "Mansoura" else 2)
                    z = ZipFile(BytesIO(zip_bytes))
                    jobf = []; invf = []
                    for n in z.namelist():
                        c = z.read(n)
                        if 'مجمع' in n:
                            jobf.append((n, c))
                        else:
                            invf.append((n, c))
                    if jobf:
                        jz = BytesIO()
                        with ZipFile(jz, 'w') as z2:
                            for n, c in jobf:
                                z2.writestr(n, c)
                        upload_order_and_metadata(jz.getvalue(), f"Breadfast_JobOrder_{city}_{d_date}.zip",
                                                  client=db_client_name,
                                                  order_type="Job Order",
                                                  order_date=order['order_date'],
                                                  delivery_date=d_date,
                                                  po_number=order.get('po_number'),
                                                  city=city)
                    if invf:
                        iz = BytesIO()
                        with ZipFile(iz, 'w') as z2:
                            for n, c in invf:
                                z2.writestr(n, c)
                        upload_order_and_metadata(iz.getvalue(), f"Breadfast_Invoices_{city}_{d_date}.zip",
                                                  client=db_client_name,
                                                  order_type="Invoice",
                                                  order_date=order['order_date'],
                                                  delivery_date=d_date,
                                                  po_number=order.get('po_number'),
                                                  city=city)
                    mark_purchase_order_done(db_client_name, d_date, city)

            except Exception as e:
                print(f"Error processing {file_name}: {e}")

    return invoice_number


if __name__ == "__main__":
    clients = ["khateer", "goodsmart", "halan", "rabbit", "breadfast", "talabat"]
    for client in clients:
        print(f"=== Processing {client} ===")
        invoice_number = process_client(client, invoice_number)
        time.sleep(5)  # wait 5 seconds before next client
    # persist invoice number back to Google Sheet
    try:
        worksheet.update("A2", [[invoice_number]])
        print("✅ Finished processing all clients. Updated A2 with", invoice_number)
    except Exception as e:
        print("Finished processing but failed to update sheet A2:", e)
