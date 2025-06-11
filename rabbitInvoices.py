import streamlit as st
import pandas as pd
import zipfile
import io
from datetime import datetime
import xlsxwriter
from streamlit_gsheets import GSheetsConnection

import io
import zipfile
import pandas as pd

def rabbitInvoices(zip_bytes: bytes, base_invoice_num: int, branches_translation: dict) -> bytes:
    """
    Processes a ZIP of Excel invoices and returns a new ZIP containing:
    - Individual formatted invoices
    - Aggregated summary sheets for Khateer and Rabbit
    - PO totals summary

    Args:
        zip_bytes (bytes): Input ZIP file as bytes.
        base_invoice_num (int): Starting invoice number.
        branches_translation (dict): Branch name translation dictionary.

    Returns:
        bytes: Output ZIP file as bytes.
    """
    zip_ref = zipfile.ZipFile(io.BytesIO(zip_bytes))
    output_zip_io = io.BytesIO()
    last_invoice_number = base_invoice_num

    with zipfile.ZipFile(output_zip_io, "w", zipfile.ZIP_DEFLATED) as output_zip:
        khateer_data = []
        khodar_data = []
        po_totals_rows = []
        invoice_zip_buffer = io.BytesIO()
        invoice_zip = zipfile.ZipFile(invoice_zip_buffer, "w")

        for file_index, file_name in enumerate(zip_ref.namelist()):
            if not file_name.endswith(".xlsx") or file_name.startswith("__MACOSX"):
                continue

            with zip_ref.open(file_name) as file:
                try:
                    df = pd.read_excel(file, skiprows=8)
                    file.seek(0)
                    df2 = pd.read_excel(file)
                    df = df[:-9].reset_index(drop=True)

                    branch = str(df2.iloc[1, 1]).strip()
                    order_number = int(df2.iloc[2, 6])
                    invoice_total = df2.iloc[-9, -1]
                    delivery_date = pd.to_datetime(df2.iloc[1, 6], errors="coerce").strftime("%Y-%m-%d")

                    name_lc = str(df.iat[0, 3]) if df.shape[0] > 0 and df.shape[1] > 3 else ""
                    prefix = "خطير" if "khateer" in name_lc.lower() else "رابيت"
                    parts = filter(None, [prefix, branch, delivery_date])
                    output_filename = "_".join(parts) + ".xlsx"
                    base_name = output_filename.rsplit("_", 1)[0]

                    filename_with_prefix = base_name
                    clean_base_name = base_name
                    for p in ["خطير_", "رابيت_"]:
                        if base_name.startswith(p):
                            clean_base_name = base_name[len(p):]
                            break

                    invoice_number = base_invoice_num + file_index

                    if not output_filename.startswith("مجمع"):
                        po_totals_rows.append({
                            "branch 'en'": branches_translation.get(clean_base_name, clean_base_name),
                            "filename": filename_with_prefix,
                            "PO Number": order_number,
                            "Invoice Total": invoice_total,
                            "Invoice Number": invoice_number
                        })

                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
                        df.to_excel(writer, index=False, sheet_name="Data")
                        workbook = writer.book
                        invoice_ws = workbook.add_worksheet("فاتورة")
                        meta_format = workbook.add_format({'bold': True, 'border': 2})
                        bold_border_right = workbook.add_format({'bold': True, 'border': 2})
                        bold_center = workbook.add_format({'bold': True, 'align': 'center'})
                        bold_merge = workbook.add_format({'bold': True, 'border': 2, 'align': 'center', 'valign': 'vcenter'})
                        headers_format = workbook.add_format({'bold': True, 'border': 1, 'align': 'center'})
                        centered_meta_format = workbook.add_format({'bold': True, 'border': 2, 'align': 'center', 'valign': 'vcenter'})
                        border_format = workbook.add_format({'border': 1})
                        barcode_format = workbook.add_format({'num_format': '0', 'border': 1})
                        qty_total_format = workbook.add_format({'border': 1})

                        try:
                            invoice_ws.insert_image("A1", "Picture1.png", {'x_scale': 1.5, 'y_scale': 1})
                        except:
                            pass

                        invoice_ws.merge_range("B1:C1", "شركه خضار للتجارة والتسويق", centered_meta_format)
                        invoice_ws.merge_range("B2:C2", "Khodar for Trading & Marketing", centered_meta_format)

                        invoice_ws.write("F1", "فاتورة مبيعات", meta_format)
                        invoice_ws.write("F2", "رقم الفاتورة #", meta_format)
                        invoice_ws.write("F3", "تاريخ الاستلام", meta_format)
                        invoice_ws.write("F4", "امر شراء رقم", meta_format)
                        invoice_ws.write("F6", "اسم العميل", meta_format)
                        invoice_ws.write("F7", "الفرع", meta_format)

                        invoice_ws.write("E2", invoice_number, meta_format)
                        invoice_ws.write("E3", delivery_date, meta_format)
                        invoice_ws.write("E4", str(order_number), meta_format)
                        invoice_ws.write("E6", f"{prefix} - فرع {branch}", meta_format)
                        invoice_ws.write("E7", branch, meta_format)

                        invoice_ws.write("A11", "Barcode", headers_format)
                        invoice_ws.write("B11", "Arabic Product Name", headers_format)
                        invoice_ws.write("C11", "Unit Cost", headers_format)
                        invoice_ws.write("D11", "quantity", headers_format)
                        invoice_ws.write("E11", "total", headers_format)

                        for idx, row in df.iterrows():
                            row_num = 11 + idx
                            barcode_value = row.get("Barcode", "")
                            if pd.isna(barcode_value) or barcode_value == '':
                                invoice_ws.write_blank(row_num, 0, "", border_format)
                            else:
                                try:
                                    barcode_int = int(barcode_value)
                                    invoice_ws.write_number(row_num, 0, barcode_int, barcode_format)
                                except:
                                    invoice_ws.write_string(row_num, 0, str(barcode_value), border_format)

                            invoice_ws.write(row_num, 1, row.get("Arabic Product Name", ""), border_format)
                            invoice_ws.write(row_num, 2, row.get("Unit Cost", ""), border_format)
                            invoice_ws.write(row_num, 3, "", qty_total_format)
                            invoice_ws.write(row_num, 4, "", qty_total_format)

                        last_row = 11 + len(df)
                        invoice_ws.merge_range(last_row, 0, last_row, 3, "Subtotal", bold_merge)
                        invoice_ws.write_blank(last_row, 4, "", bold_border_right)
                        invoice_ws.merge_range(last_row + 1, 0, last_row + 1, 3, "Total", bold_merge)
                        invoice_ws.write(last_row + 1, 4, invoice_total, bold_border_right)

                        for i, text in enumerate(["شركة خضار للتجارة و التسويق", "ش.ذ.م.م", "سجل تجارى / 13138  بطاقه ضريبية/721/294/448"]):
                            row = last_row + 3 + i
                            invoice_ws.merge_range(row, 0, row, 3, text, bold_center)

                        invoice_ws.set_column("A:A", 25)
                        invoice_ws.set_column("B:B", 30)
                        invoice_ws.set_column("C:E", 15)

                    excel_buffer.seek(0)
                    invoice_zip.writestr(output_filename, excel_buffer.getvalue())

                    pivot_cols = ["SKU", "Barcode", "Arabic Product Name", "Unit Cost", "Total PC"]
                    if all(col in df.columns for col in pivot_cols):
                        pivot_df = df[pivot_cols].copy()
                        pivot_df.rename(columns={"Total PC": branch}, inplace=True)
                        if "khateer" in name_lc.lower():
                            khateer_data.append(pivot_df)
                        else:
                            khodar_data.append(pivot_df)

                except Exception as e:
                    error_txt = f"Failed to process {file_name}: {str(e)}"
                    output_zip.writestr(f"errors/Error_{file_name}.txt", error_txt)

        def create_aggregated_df(list_of_dfs):
            if not list_of_dfs:
                return None
            merged_df = list_of_dfs[0]
            for df in list_of_dfs[1:]:
                merged_df = pd.merge(merged_df, df, on=["SKU", "Barcode", "Arabic Product Name", "Unit Cost"], how="outer")
            branch_cols = sorted([col for col in merged_df.columns if col not in ["SKU", "Barcode", "Arabic Product Name", "Unit Cost"]])
            merged_df[branch_cols] = merged_df[branch_cols].fillna(0)
            merged_df["Total Quantity"] = merged_df[branch_cols].sum(axis=1)
            reordered_cols = ["SKU", "Barcode", "Arabic Product Name"] + branch_cols + ["Total Quantity", "Unit Cost"]
            merged_df = merged_df[reordered_cols]
            merged_df["Grand Total"] = merged_df["Total Quantity"] * merged_df["Unit Cost"]
            return merged_df

        khateer_pivot = create_aggregated_df(khateer_data)
        khodar_pivot = create_aggregated_df(khodar_data)

        invoice_zip.close()
        output_zip.writestr("invoices.zip", invoice_zip_buffer.getvalue())

        if khateer_pivot is not None:
            khateer_buffer = io.BytesIO()
            khateer_pivot.to_excel(khateer_buffer, index=False)
            output_zip.writestr("مجمع خطير.xlsx", khateer_buffer.getvalue())

        if khodar_pivot is not None:
            khodar_buffer = io.BytesIO()
            khodar_pivot.to_excel(khodar_buffer, index=False)
            output_zip.writestr("مجمع رابيت.xlsx", khodar_buffer.getvalue())

        if po_totals_rows:
            po_totals_df = pd.DataFrame(po_totals_rows)
            po_totals_df["Invoice Total"] = pd.to_numeric(po_totals_df["Invoice Total"], errors="coerce")
            po_totals_buffer = io.BytesIO()
            po_totals_df.to_excel(po_totals_buffer, index=False)
            output_zip.writestr("po_totals.xlsx", po_totals_buffer.getvalue())

    output_zip_io.seek(0)
    return output_zip_io.getvalue(), file_index