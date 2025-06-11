import streamlit as st
import requests
import io
import zipfile
from datetime import datetime
from streamlit_gsheets import GSheetsConnection
from goodsmartInvoices import generate_invoice_excel
from rabbitInvoices import rabbitInvoices
from pdfsToExcels import process_talabat_invoices
from config import translation_dict, categories_dict, branches_dict, branches_translation_tlbt, columns
from breadfastInvoices import process_breadfast_invoice
from io import BytesIO
import os
from tempfile import NamedTemporaryFile
from typing import Optional
from zipfile import ZipFile



st.set_page_config(page_title="Download All Orders as ZIP", layout="centered")
st.title("📦 Convert Pending Purchase Orders into Job Orders & Invoices")


SUPABASE_URL = "https://rabwvltxgpdyvpmygdtc.supabase.co"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJhYnd2bHR4Z3BkeXZwbXlnZHRjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDkyMzg4MTQsImV4cCI6MjA2NDgxNDgxNH0.hnQYr3jL0rLTNOGXE0EgF9wmd_bynff6JXtqwjCOc6Y"
AUTHORIZATION = f"Bearer {API_KEY}"
STORAGE_BUCKET = "order_files"
TABLE_NAME = "orders"


# Supabase constants
SUPABASE_API_URL = "https://rabwvltxgpdyvpmygdtc.supabase.co/rest/v1/orders"
SUPABASE_HEADERS = {
    "accept": "*/*",
    "accept-language": "en-GB,en;q=0.8",
    "accept-profile": "public",
    "apikey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJhYnd2bHR4Z3BkeXZwbXlnZHRjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDkyMzg4MTQsImV4cCI6MjA2NDgxNDgxNH0.hnQYr3jL0rLTNOGXE0EgF9wmd_bynff6JXtqwjCOc6Y",
    "origin": "https://po.khodar.com",
    "referer": "https://po.khodar.com/",
    "user-agent": "Mozilla/5.0",
    "x-client-info": "supabase-js-web/2.50.0"
}
import requests

def mark_purchase_order_done(client, delivery_date, city=None):
    SUPABASE_URL = "https://rabwvltxgpdyvpmygdtc.supabase.co"
    API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJhYnd2bHR4Z3BkeXZwbXlnZHRjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDkyMzg4MTQsImV4cCI6MjA2NDgxNDgxNH0.hnQYr3jL0rLTNOGXE0EgF9wmd_bynff6JXtqwjCOc6Y"
    AUTH_HEADER = {
        "apikey": API_KEY,
        "authorization": f"Bearer {API_KEY}",
        "content-type": "application/json",
        "accept": "*/*",
        "content-profile": "public"
    }

    # Step 1: Fetch the matching order(s)
    params = {
        "client": f"eq.{client}",
        "order_type": "eq.Purchase Order",
        "delivery_date": f"eq.{delivery_date}",
        "status": "eq.Pending"
    }
    if city:
        params["city"] = f"eq.{city}"

    response = requests.get(
        f"{SUPABASE_URL}/rest/v1/orders",
        headers=AUTH_HEADER,
        params=params
    )
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch orders: {response.text}")

    orders = response.json()
    if not orders:
        print("No matching pending purchase order found.")
        return

    for order in orders:
        order_id = order["id"]

        # Step 2: Update the status to "Done"
        patch_response = requests.patch(
            f"{SUPABASE_URL}/rest/v1/orders?id=eq.{order_id}",
            headers=AUTH_HEADER,
            json={"status": "Done"}
        )

        if patch_response.status_code == 204:
            print(f"Order {order_id} marked as Done.")
        else:
            print(f"Failed to update order {order_id}: {patch_response.text}")


def upload_order_and_metadata(
    file_bytes: bytes,
    filename: str,
    client: str,
    order_type: str,
    order_date: str,
    delivery_date: str,
    status: str = "Pending",
    city: Optional[str] = None,
    po_number: Optional[int] = None,
):
    object_name = f"{int(order_date.replace('-', ''))}-{filename}"
    storage_url = f"{SUPABASE_URL}/storage/v1/object/{STORAGE_BUCKET}/{object_name}"

    with NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    with open(tmp_path, "rb") as f:
        upload_response = requests.post(
            storage_url,
            headers={
                "apikey": API_KEY,
                "authorization": AUTHORIZATION,
                "x-upsert": "false",
            },
            files={
                "file": (filename, f, "application/octet-stream")
            }
        )

    os.remove(tmp_path)

    if upload_response.status_code != 200:
        raise Exception(f"Upload failed: {upload_response.text}")

    file_url = f"{SUPABASE_URL}/storage/v1/object/public/{STORAGE_BUCKET}/{object_name}"
    insert_payload = [{
        "client": client,
        "order_type": order_type,
        "order_date": order_date,
        "delivery_date": delivery_date,
        "status": status,
        "file_urls": [file_url],
        "city": city,
        "po_number": po_number
    }]

    insert_response = requests.post(
        f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}",
        headers={
            "apikey": API_KEY,
            "authorization": AUTHORIZATION,
            "content-type": "application/json",
            "prefer": "return=representation",
        },
        json=insert_payload
    )

    if insert_response.status_code not in [200, 201]:
        raise Exception(f"Insertion failed: {insert_response.text}")

    return insert_response.json()


# --- Client Selection ---
client_options = {
    "goodsmart": "GoodsMart",
    "talabat": "Talabat",
    "khateer": "Khateer",
    "rabbit": "Rabbit",
    "breadfast": "BreadFast"
}

selected_client = st.selectbox("Select Client", list(client_options.values()))
selected_key = selected_client.strip().lower()

# --- Invoice Number Handling ---
conn = st.connection("gsheets", type=GSheetsConnection)
df_invoice_number = conn.read(worksheet="Saved", cell="A1", ttl=5, headers=False)
invoice_number = int(df_invoice_number.iat[0, 0])


def fetch_pending_orders():
    params = {"select": "*", "order": "created_at.desc"}
    resp = requests.get(SUPABASE_API_URL, headers=SUPABASE_HEADERS, params=params)
    if resp.status_code == 200:
        orders = resp.json()
        return [o for o in orders if o.get("status") == "Pending"]
    return []


def download_from_url(file_url):
    resp = requests.get(file_url)
    return resp.content if resp.status_code == 200 else None


if st.button("Download Pending Orders"):
    with st.spinner("Fetching and processing files..."):
        orders = fetch_pending_orders()

        if not orders:
            st.info("No pending orders were found.")
        else:
            for order in orders:
                if (
                    order.get("order_type") == "Purchase Order"
                    and order.get("client", "").strip().lower() == selected_key
                ):
                    for file_url in order.get("file_urls", []):
                        file_name = file_url.split("/")[-1]
                        st.write(f"🟢 Downloading: {file_name}")
                        file_data = download_from_url(file_url)
                        if not file_data:
                            st.error(f"Failed to download {file_name}")
                            continue

                        try:
                            if selected_key == "goodsmart":
                                processed_excel, delivery_date_str = generate_invoice_excel(
                                    excel_bytes=file_data,
                                    invoice_number=invoice_number,
                                    delivery_date=order.get("delivery_date"),
                                    po_value=order.get("po_number", "N/A")
                                )
                                filename = f"GoodsMart_{delivery_date_str}.xlsx"
                                invoice_number += 1

                                mark_purchase_order_done(client="GoodsMart", delivery_date=order.get("delivery_date"), city=order.get("city"))

                                upload_order_and_metadata(
                                    file_bytes=processed_excel,
                                    filename=f"GoodsMart_{delivery_date_str}.xlsx",
                                    client="GoodsMart",
                                    order_type="Invoice",
                                    order_date=order["order_date"],
                                    delivery_date=order["delivery_date"],
                                    po_number=order.get("po_number")
                                )
                                upload_order_and_metadata(
                                    file_bytes=processed_excel,
                                    filename=f"GoodsMart_{delivery_date_str}.xlsx",
                                    client="GoodsMart",
                                    order_type="job order",
                                    order_date=order["order_date"],
                                    delivery_date=order["delivery_date"],
                                    po_number=order.get("po_number")
                                )

                            elif selected_key == "khateer":
                                output_zip_bytes, file_index = rabbitInvoices(
                                    file_data,
                                    invoice_number,
                                    order["delivery_date"],
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

                                invoice_number += file_index + 1

                                # Split inner zip and Excel files
                                original_zip = ZipFile(BytesIO(output_zip_bytes))
                                inner_zip_bytes = None
                                excel_files = []

                                for name in original_zip.namelist():
                                    content = original_zip.read(name)
                                    if name.lower().endswith(".zip"):
                                        inner_zip_bytes = content
                                    elif name.lower().endswith(".xlsx"):
                                        excel_files.append((name, content))

                                if inner_zip_bytes:
                                    upload_order_and_metadata(
                                        file_bytes=inner_zip_bytes,
                                        filename="Khateer_Invoice.zip",
                                        client="Khateer",
                                        order_type="Invoice",
                                        order_date=order["order_date"],
                                        delivery_date=order["delivery_date"],
                                        po_number=order.get("po_number"),
                                        city=order.get("city")
                                    )

                                if excel_files:
                                    new_zip_io = BytesIO()
                                    with ZipFile(new_zip_io, mode="w") as zf:
                                        for fname, content in excel_files:
                                            zf.writestr(fname, content)
                                    new_zip_io.seek(0)
                                    mark_purchase_order_done(client="Khateer", delivery_date=order.get("delivery_date"), city=order.get("city"))
                                    upload_order_and_metadata(
                                        file_bytes=new_zip_io.getvalue(),
                                        filename="Khateer_JobOrder.zip",
                                        client="Khateer",
                                        order_type="Job Order",
                                        order_date=order["order_date"],
                                        delivery_date=order["delivery_date"],
                                        po_number=order.get("po_number"),
                                        city=order.get("city")
                                    )

                            elif selected_key == "rabbit":
                                output_zip_bytes, file_index = rabbitInvoices(
                                    file_data,
                                    invoice_number,
                                    order["delivery_date"],
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

                                invoice_number += file_index + 1

                                # Split inner zip and Excel files
                                original_zip = ZipFile(BytesIO(output_zip_bytes))
                                inner_zip_bytes = None
                                excel_files = []

                                for name in original_zip.namelist():
                                    content = original_zip.read(name)
                                    if name.lower().endswith(".zip"):
                                        inner_zip_bytes = content
                                    elif name.lower().endswith(".xlsx"):
                                        excel_files.append((name, content))

                                if inner_zip_bytes:
                                    upload_order_and_metadata(
                                        file_bytes=inner_zip_bytes,
                                        filename=f"Rabbit_Invoice_{order['delivery_date']}.zip",
                                        client="Rabbit",
                                        order_type="Invoice",
                                        order_date=order["order_date"],
                                        delivery_date=order["delivery_date"],
                                        po_number=order.get("po_number"),
                                        city=order.get("city")
                                    )

                                if excel_files:
                                    new_zip_io = BytesIO()
                                    with ZipFile(new_zip_io, mode="w") as zf:
                                        for fname, content in excel_files:
                                            zf.writestr(fname, content)
                                    new_zip_io.seek(0)
                                    mark_purchase_order_done(client="Rabbit", delivery_date=order.get("delivery_date"), city=order.get("city"))
                                    upload_order_and_metadata(
                                        file_bytes=new_zip_io.getvalue(),
                                        filename=f"Rabbit_JobOrder_{order['delivery_date']}.zip",
                                        client="Rabbit",
                                        order_type="Job Order",
                                        order_date=order["order_date"],
                                        delivery_date=order["delivery_date"],
                                        po_number=order.get("po_number"),
                                        city=order.get("city")
                                    )


                            elif selected_key == "talabat":
                                delivery_date = order.get("delivery_date")
                                output_zip_bytes, offset = process_talabat_invoices(
                                    zip_file_bytes=file_data,
                                    invoice_date=delivery_date,
                                    base_invoice_number=invoice_number,
                                    translation_dict=translation_dict,
                                    categories_dict=categories_dict,
                                    branches_dict=branches_dict,
                                    branches_translation_tlbt=branches_translation_tlbt,
                                    columns=columns
                                )
                                invoice_number = invoice_number + offset


                                # --- Step 1: Extract and separate inner ZIP and Excel files ---
                                original_zip = ZipFile(BytesIO(output_zip_bytes))
                                inner_zip_bytes = None
                                excel_files = []

                                for name in original_zip.namelist():
                                    content = original_zip.read(name)
                                    if name.lower().endswith(".zip"):
                                        inner_zip_bytes = content  # Assume only one inner zip
                                    elif name.lower().endswith(".xlsx"):
                                        excel_files.append((name, content))

                                # --- Step 2: Upload inner ZIP as Invoice ---
                                if inner_zip_bytes:
                                    upload_order_and_metadata(
                                        file_bytes=inner_zip_bytes,
                                        filename=f"Talabat_Invoice_{delivery_date}.zip",
                                        client="Talabat",
                                        order_type="Invoice",
                                        order_date=order["order_date"],
                                        delivery_date=delivery_date,
                                        po_number=order.get("po_number"),
                                        city=order.get("city")
                                    )

                                # --- Step 3: Re-zip the Excel files and upload as Job Order ---
                                if excel_files:
                                    new_zip_io = BytesIO()
                                    with ZipFile(new_zip_io, mode="w") as zf:
                                        for fname, content in excel_files:
                                            zf.writestr(fname, content)
                                    new_zip_io.seek(0)
                                    mark_purchase_order_done(client="Talabat", delivery_date=order.get("delivery_date"), city=order.get("city"))
                                    upload_order_and_metadata(
                                        file_bytes=new_zip_io.getvalue(),
                                        filename=f"Talabat_JobOrder_{delivery_date}.zip",
                                        client="Talabat",
                                        order_type="Job Order",
                                        order_date=order["order_date"],
                                        delivery_date=delivery_date,
                                        po_number=order.get("po_number"),
                                        city=order.get("city")
                                    )
                            elif selected_key == "breadfast":
                                city = order.get("city")
                                delivery_date_str = order.get("delivery_date")

                                output_zip_bytes = process_breadfast_invoice(
                                    city=city,
                                    pdf_file_bytes=file_data,
                                    invoice_number=invoice_number,
                                    delivery_date_str=delivery_date_str
                                )

                                if city == "Mansoura":
                                    invoice_number += 1
                                else:
                                    invoice_number += 2


                                mark_purchase_order_done(client="Breadfast", delivery_date=order.get("delivery_date"), city=order.get("city"))
                                upload_order_and_metadata(
                                    file_bytes=output_zip_bytes,
                                    filename=f"Breadfast_{city}_{delivery_date_str}.zip",
                                    client="Breadfast",
                                    order_type="Job Order",
                                    order_date=order["order_date"],
                                    delivery_date=order["delivery_date"],
                                    city=city
                                )

                        except Exception as e:
                            st.error(f"Error processing {file_name}: {e}")

            df_invoice_number.iat[0, 0] = invoice_number
            conn.update(worksheet="Saved", data=df_invoice_number)
            st.success("✅ Finished processing all orders.")
