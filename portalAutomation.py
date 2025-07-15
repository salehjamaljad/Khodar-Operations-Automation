import streamlit as st
import requests
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime
from streamlit_gsheets import GSheetsConnection
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
import os
from tempfile import NamedTemporaryFile
from typing import Optional

# --- Streamlit Page Setup ---
st.set_page_config(page_title="Job Orders & Invoices Generator", layout="centered")
st.title("📦 Transform Pending Purchase Orders Into Job Orders & Invoices")

# --- Supabase Configuration ---
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

# --- Helpers ---
def mark_purchase_order_done(client: str, delivery_date: str, city: Optional[str] = None):
    """
    Marks the matching pending Purchase Order(s) for given client and date as Done in Supabase.
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

    resp = requests.get(f"{SUPABASE_URL}/rest/v1/orders", headers=headers, params=params)
    resp.raise_for_status()
    orders = resp.json()
    for order in orders:
        oid = order.get("id")
        patch = requests.patch(
            f"{SUPABASE_URL}/rest/v1/orders?id=eq.{oid}",
            headers=headers,
            json={"status": "Done"}
        )
        patch.raise_for_status()


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
    Uploads file to Supabase storage and records metadata in orders table.
    """
    # upload file
    object_name = f"{int(order_date.replace('-', ''))}-{filename}"
    storage_url = f"{SUPABASE_URL}/storage/v1/object/{STORAGE_BUCKET}/{object_name}"
    with NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name
    with open(tmp_path, 'rb') as f:
        up = requests.post(
            storage_url,
            headers={"apikey": API_KEY, "authorization": AUTHORIZATION},
            files={"file": (filename, f, "application/octet-stream")}
        )
    os.remove(tmp_path)
    up.raise_for_status()
    file_url = f"{SUPABASE_URL}/storage/v1/object/public/{STORAGE_BUCKET}/{object_name}"

    # insert metadata record
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
        headers={"apikey": API_KEY, "authorization": AUTHORIZATION, "content-type": "application/json", "prefer": "return=representation"},
        json=payload
    )
    ins.raise_for_status()
    return ins.json()

# --- Client Selection ---
client_options = {
    "goodsmart": "GoodsMart",
    "halan": "Halan",
    "talabat": "Talabat",
    "khateer": "Khateer",
    "rabbit": "Rabbit",
    "breadfast": "BreadFast"
}
selected_client = st.selectbox("Select Client", list(client_options.values()))
selected_key = selected_client.strip().lower()

# --- Invoice Number from Google Sheets ---
conn = st.connection("gsheets", type=GSheetsConnection)
df_inv = conn.read(worksheet="Saved", cell="A1", ttl=5, headers=False)
invoice_number = int(df_inv.iat[0, 0])

# --- Fetch & Download Helpers ---
def fetch_pending_orders():
    resp = requests.get(SUPABASE_API_URL + "?select=*&order=created_at.desc", headers=SUPABASE_HEADERS)
    resp.raise_for_status()
    return [o for o in resp.json() if o.get("status") == "Pending"]

def download_from_url(url: str) -> bytes:
    r = requests.get(url)
    r.raise_for_status()
    return r.content

# --- Main Processing ---
if st.button("Generate Job Orders & Invoices"):
    with st.spinner("Fetching and processing files..."):
        orders = fetch_pending_orders()
        if not orders:
            st.info("No pending orders found.")
        for order in orders:
            if order.get("order_type") != "Purchase Order":
                continue
            if order.get("client", '').strip().lower() != selected_key:
                continue

            for file_url in order.get("file_urls", []):
                file_name = os.path.basename(file_url)
                st.write(f"🟢 Processing: {file_name}")
                data = download_from_url(file_url)
                try:
                    # --- goodsmart ---
                    if selected_key == "goodsmart":
                        excel_bytes, d_date = generate_invoice_excel(
                            excel_bytes=data,
                            invoice_number=invoice_number,
                            delivery_date=order.get("delivery_date"),
                            po_value=order.get("po_number")
                        )
                        invoice_number += 1
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
                            invoice_number=invoice_number,
                            delivery_date=order.get("delivery_date"),
                            po_value=order.get("po_number")
                        )
                        invoice_number += 5
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

                    # --- khateer & rabbit reuse rabbitInvoices ---
                    elif selected_key in ("khateer", "rabbit"):
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
                            if n.lower().endswith('.zip'): inner = c
                            elif n.lower().endswith('.xlsx'): excels.append((n,c))
                        if inner:
                            upload_order_and_metadata(inner, f"{selected_client}_Invoice_{order['delivery_date']}.zip",
                                                      selected_client, "Invoice", order['order_date'], order['delivery_date'], order.get('po_number'), order.get('city'))
                        if excels:
                            newz = BytesIO()
                            with ZipFile(newz,'w') as z2:
                                for n,c in excels: z2.writestr(n,c)
                            mark_purchase_order_done(selected_client.title(), order.get("delivery_date"), order.get("city"))
                            upload_order_and_metadata(newz.getvalue(), f"{selected_client}_JobOrder_{order['delivery_date']}.zip",
                                                      selected_client, "Job Order", order['order_date'], order['delivery_date'], order.get('po_number'), order.get('city'))

                    # --- talabat ---
                    elif selected_key == "talabat":
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
                            invoice_number=invoice_number,
                            delivery_date_str=d_date
                        )
                        invoice_number += (1 if city == "Mansoura" else 2)
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

                except Exception as e:
                    st.error(f"Error processing {file_name}: {e}")

        # --- Update Invoice Number in Sheet ---
        df_inv.iat[0, 0] = invoice_number
        conn.update(worksheet="Saved", data=df_inv)
        st.success("✅ Finished processing all orders.")
