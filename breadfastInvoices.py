import pdfplumber
import re
import pandas as pd
from io import BytesIO
import zipfile
import os
import tempfile
from datetime import datetime
from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Border, Side, Alignment, Font
from openpyxl.drawing.image import Image
from openpyxl.utils import get_column_letter

from config import barcode_to_product, categories_dict

def process_breadfast_invoice(
    city: str,
    pdf_file_bytes: bytes,
    invoice_number: int,
    delivery_date_str: str
) -> bytes:
    """
    Processes a single PDF for Breadfast orders in either Alexandria or Mansoura.
    Args:
        city: "الاسكندرية" or "المنصورة"
        pdf_file_bytes: raw bytes of the uploaded PDF
        invoice_number: starting invoice number (integer)
        delivery_date_str: string date in "YYYY-MM-DD" format
    Returns:
        A bytes object containing a ZIP archive with generated Excel files.
    Raises:
        ValueError if city is not recognized or required PDF patterns are missing.
    """

    def extract_prices(text_block: str) -> list:
        # Match numbers like " 12.345678 " and round to 2 decimals
        matches = re.findall(r"\s(\d+\.\d{6})\s", text_block)
        return [round(float(p), 2) for p in matches]

    def insert_nulls(barcodes: list, ids: list) -> list:
        # For Alexandria: insert "" at positions where ID == "6484003"
        target_indexes = [i for i, id_val in enumerate(ids) if id_val == "6484003"]
        for count, original_index in enumerate(target_indexes):
            adjusted_index = original_index + count
            barcodes.insert(adjusted_index, "")
        return barcodes

    def extract_data_alex(text_block: str, branch_name: str) -> pd.DataFrame:
        # For Alexandria: extract IDs, barcodes, quantities, prices
        ids = re.findall(r"\[(\d+)\]", text_block)
        barcodes = re.findall(r"\s(22\d{11})\s", text_block)
        quantities = re.findall(r"\s(\d+(?:\.\d+)?)\.0000000\s", text_block)

        n = len(ids)
        barcodes = insert_nulls(barcodes, ids)
        barcodes = barcodes[:n] + [""] * max(0, n - len(barcodes))
        quantities = quantities[:n] + ["0"] * max(0, n - len(quantities))
        quantities = [int(float(q)) for q in quantities]
        prices = extract_prices(text_block)
        prices = prices[:n] + [""] * max(0, n - len(prices))

        df = pd.DataFrame({
            "ID": ids,
            "Barcode": barcodes,
            "Quantity": quantities,
            "pp": prices
        })

        def to_int_or_empty(x):
            try:
                return int(x)
            except:
                return ""

        df["Barcode"] = df["Barcode"].apply(to_int_or_empty)
        df["Product Name"] = df["Barcode"].astype(str).map(barcode_to_product).fillna("غير معروف")
        df["فرع"] = branch_name

        return df

    def extract_data_mansoura(text_block: str) -> pd.DataFrame:
        # For Mansoura: extract IDs, barcodes, quantities, prices
        ids = re.findall(r"\[(\d+)\]", text_block)
        barcodes = re.findall(r"\s(22\d{11})\s", text_block)
        quantities = re.findall(r"\s(\d+(?:\.\d+)?)\.0000000\s", text_block)

        n = len(ids)
        # mansoura does not need insert_nulls
        barcodes = barcodes[:n] + [""] * max(0, n - len(barcodes))
        quantities = quantities[:n] + ["0"] * max(0, n - len(quantities))
        quantities = [int(float(q)) for q in quantities]
        prices = extract_prices(text_block)
        prices = prices[:n] + [""] * max(0, n - len(prices))

        df = pd.DataFrame({
            "ID": ids,
            "Barcode": barcodes,
            "Quantity": quantities,
            "pp": prices
        })

        def to_int_or_empty(x):
            try:
                return int(x)
            except:
                return ""

        df["Barcode"] = df["Barcode"].apply(to_int_or_empty)
        df["Product Name"] = df["Barcode"].astype(str).map(barcode_to_product).fillna("غير معروف")
        df["فرع"] = "المنصورة"

        return df

    def create_pivot_excel_alex(df: pd.DataFrame) -> BytesIO:
        # Pivot for Alexandria: branches "لوران" and "سموحة"
        pivot_df = df.pivot_table(
            index=["Barcode", "Product Name", "pp"],
            columns="فرع",
            values="Quantity",
            aggfunc="sum",
            fill_value=0
        ).reset_index()

        # Ensure both columns exist
        for col in ["لوران", "سموحة"]:
            if col not in pivot_df.columns:
                pivot_df[col] = 0

        pivot_df["total_quantity"] = pivot_df["لوران"] + pivot_df["سموحة"]
        pivot_df["total"] = pivot_df["total_quantity"] * pivot_df["pp"]

        # Reorder columns
        pivot_df = pivot_df[["Barcode", "Product Name", "لوران", "سموحة", "total_quantity", "pp", "total"]]

        # Map categories
        product_to_category = {product: cat for cat, products in categories_dict.items() for product in products}
        pivot_df["category"] = pivot_df["Product Name"].map(product_to_category).fillna("غير معرف")

        # Sort by category & product name
        category_order = ["فاكهه", "خضار", "جاهز", "اعشاب", "غير معرف"]
        pivot_df["category_order"] = pivot_df["category"].apply(lambda x: category_order.index(x))
        pivot_df.sort_values(by=["category_order", "Product Name"], inplace=True)
        pivot_df.drop(columns=["category_order"], inplace=True)

        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            pivot_df.to_excel(writer, index=False, sheet_name="مجمع اسكندرية")
            workbook = writer.book
            worksheet = writer.sheets["مجمع اسكندرية"]
            number_format = workbook.add_format({'num_format': '0'})
            quantity_format = workbook.add_format({'num_format': '0'})
            price_format = workbook.add_format({'num_format': '0.00'})
            total_format = workbook.add_format({'num_format': '0.00'})

            worksheet.set_column("A:A", 20, number_format)
            worksheet.set_column("B:B", 40)
            worksheet.set_column("C:D", 10, quantity_format)
            worksheet.set_column("E:E", 12, quantity_format)
            worksheet.set_column("F:F", 10, price_format)
            worksheet.set_column("G:G", 15, total_format)
        output.seek(0)
        return output

    def create_pivot_excel_mansoura(df: pd.DataFrame) -> BytesIO:
        # Pivot for Mansoura: single branch "المنصورة"
        pivot_df = df.pivot_table(
            index=["Barcode", "Product Name", "pp"],
            columns="فرع",
            values="Quantity",
            aggfunc="sum",
            fill_value=0
        ).reset_index()

        pivot_df["total_quantity"] = pivot_df["المنصورة"]
        pivot_df["total"] = pivot_df["total_quantity"] * pivot_df["pp"]

        # Map categories
        product_to_category = {product: cat for cat, products in categories_dict.items() for product in products}
        pivot_df["category"] = pivot_df["Product Name"].map(product_to_category).fillna("غير معرف")

        # Sort
        category_order = ["فاكهه", "خضار", "جاهز", "اعشاب", "غير معرف"]
        pivot_df["category_order"] = pivot_df["category"].apply(lambda x: category_order.index(x))
        pivot_df.sort_values(by=["category_order", "Product Name"], inplace=True)
        pivot_df.drop(columns=["category_order"], inplace=True)

        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            pivot_df.to_excel(writer, index=False, sheet_name="مجمع المنصورة")
            ws = writer.sheets["مجمع المنصورة"]
            fmt = writer.book.add_format({'num_format': '0.00'})
            ws.set_column("A:A", 20)
            ws.set_column("B:B", 40)
            ws.set_column("C:D", 12)
            ws.set_column("E:E", 14)
            ws.set_column("F:F", 10, fmt)
            ws.set_column("G:G", 15, fmt)
        output.seek(0)
        return output

    def create_invoice_excel_alex(
        df: pd.DataFrame,
        invoice_num: int,
        branch: str,
        po_value: str,
        delivery_date: datetime
    ) -> BytesIO:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name="Orders")
            workbook = writer.book
            worksheet = writer.sheets["Orders"]

            qty_col = df.columns.get_loc("Quantity")  # zero-based
            pp_col = df.columns.get_loc("pp")
            last_row = len(df) + 1  # 1-based header

            # Grand Total formulas
            bold_border = workbook.add_format({'bold': True, 'border': 1})
            worksheet.write(last_row, 0, "Grand Total", bold_border)
            worksheet.write_formula(
                last_row, qty_col,
                f"=SUM({chr(65 + qty_col)}2:{chr(65 + qty_col)}{last_row})",
                bold_border
            )
            worksheet.write_formula(
                last_row, pp_col,
                f"=SUM({chr(65 + pp_col)}2:{chr(65 + pp_col)}{last_row})",
                bold_border
            )

            # Invoice sheet
            invoice_ws = workbook.add_worksheet("فاتورة")
            meta_format = workbook.add_format({'bold': True, 'border': 2})
            center_format = workbook.add_format({'bold': True, 'align': 'center'})
            merge_format = workbook.add_format({'bold': True, 'border': 2, 'align': 'center', 'valign': 'vcenter'})
            header_format = workbook.add_format({'bold': True, 'border': 1, 'align': 'center'})

            # Insert image if available
            try:
                invoice_ws.insert_image("A1", "Picture1.png", {'x_scale': 0.5, 'y_scale': 0.5})
            except:
                pass

            # Static cells
            invoice_ws.write("A5", "شركه خضار للتجارة والتسويق", meta_format)
            invoice_ws.write("C1", "شركه خضار للتجارة والتسويق", meta_format)
            invoice_ws.write("C2", "Khodar for Trading & Marketing", meta_format)
            invoice_ws.write("F1", "فاتورة مبيعات", meta_format)
            invoice_ws.write("F2", "رقم الفاتورة #", meta_format)
            invoice_ws.write("F3", "تاريخ الاستلام", meta_format)
            invoice_ws.write("F4", "امر شراء رقم", meta_format)
            invoice_ws.write("F6", "اسم العميل", meta_format)
            invoice_ws.write("F7", "الفرع", meta_format)

            invoice_ws.write("E2", invoice_num, meta_format)
            invoice_ws.write("E3", delivery_date.strftime("%Y-%m-%d"), meta_format)
            invoice_ws.write("E4", str(po_value), workbook.add_format({'border': 2, 'align': 'center', 'bold': True}))
            invoice_ws.write("E6", f"بريدفاست - فرع {branch}", meta_format)
            invoice_ws.write("E7", branch, meta_format)

            # Table headers at row 11 (0-based index 10)
            invoice_ws.write("A11", "Barcode", header_format)
            invoice_ws.write("B11", "Product Name", header_format)
            invoice_ws.write("C11", "PP", header_format)
            invoice_ws.write("D11", "Qty", header_format)
            invoice_ws.write("E11", "Total", header_format)

            # Rows starting at row 12
            for idx, row in df.iterrows():
                r = 11 + idx
                barcode = row["Barcode"]
                if barcode == "" or pd.isna(barcode):
                    invoice_ws.write_blank(r, 0, "", workbook.add_format({'border': 1}))
                else:
                    try:
                        invoice_ws.write_number(r, 0, int(barcode), workbook.add_format({'num_format': '0', 'border': 1}))
                    except:
                        invoice_ws.write(r, 0, str(barcode), workbook.add_format({'border': 1}))
                invoice_ws.write(r, 1, row["Product Name"])
                invoice_ws.write(r, 2, row["pp"])
                invoice_ws.write(r, 3, "")  # Empty Qty
                invoice_ws.write(r, 4, "")  # Empty Total

            last = 11 + len(df)
            invoice_ws.merge_range(last, 0, last, 3, "Subtotal", merge_format)
            invoice_ws.write_blank(last, 4, "", meta_format)
            invoice_ws.merge_range(last + 1, 0, last + 1, 3, "Total", merge_format)
            invoice_ws.write_blank(last + 1, 4, "", meta_format)

            footer_texts = [
                "شركة خضار للتجارة و التسويق",
                "ش.ذ.م.م",
                "سجل تجارى / 13138  بطاقه ضريبية/721/294/448"
            ]
            for i, txt in enumerate(footer_texts):
                invoice_ws.merge_range(last + 3 + i, 0, last + 3 + i, 3, txt, center_format)

            invoice_ws.set_column("A:E", 25)

        output.seek(0)
        return output

    def create_invoice_excel_mansoura(
        df: pd.DataFrame,
        invoice_num: int,
        branch: str,
        po_value: str,
        delivery_date: datetime
    ) -> BytesIO:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name="Orders")
            workbook = writer.book
            worksheet = writer.sheets["Orders"]

            qty_col = df.columns.get_loc("Quantity")
            pp_col = df.columns.get_loc("pp")
            last_row = len(df) + 1

            bold_border = workbook.add_format({'bold': True, 'border': 1})
            worksheet.write(last_row, 0, "Grand Total", bold_border)
            worksheet.write_formula(
                last_row, qty_col,
                f"=SUM({chr(65 + qty_col)}2:{chr(65 + qty_col)}{last_row})",
                bold_border
            )
            worksheet.write_formula(
                last_row, pp_col,
                f"=SUM({chr(65 + pp_col)}2:{chr(65 + pp_col)}{last_row})",
                bold_border
            )

            invoice_ws = workbook.add_worksheet("فاتورة")
            meta_fmt = workbook.add_format({'bold': True, 'border': 2})
            center_fmt = workbook.add_format({'bold': True, 'align': 'center'})
            merge_fmt = workbook.add_format({'bold': True, 'border': 2, 'align': 'center', 'valign': 'vcenter'})
            header_fmt = workbook.add_format({'bold': True, 'border': 1, 'align': 'center'})

            try:
                invoice_ws.insert_image("A1", "Picture1.png", {'x_scale': 0.5, 'y_scale': 0.5})
            except:
                pass

            invoice_ws.write("A5", "شركه خضار للتجارة والتسويق", meta_fmt)
            invoice_ws.write("C1", "شركه خضار للتجارة والتسويق", meta_fmt)
            invoice_ws.write("C2", "Khodar for Trading & Marketing", meta_fmt)
            invoice_ws.write("F1", "فاتورة مبيعات", meta_fmt)
            invoice_ws.write("F2", "رقم الفاتورة #", meta_fmt)
            invoice_ws.write("F3", "تاريخ الاستلام", meta_fmt)
            invoice_ws.write("F4", "امر شراء رقم", meta_fmt)
            invoice_ws.write("F6", "اسم العميل", meta_fmt)
            invoice_ws.write("F7", "الفرع", meta_fmt)

            invoice_ws.write("E2", invoice_num, meta_fmt)
            invoice_ws.write("E3", delivery_date.strftime("%Y-%m-%d"), meta_fmt)
            invoice_ws.write("E4", str(po_value), workbook.add_format({'border': 2, 'align': 'center', 'bold': True}))
            invoice_ws.write("E6", f"بريدفاست - فرع {branch}", meta_fmt)
            invoice_ws.write("E7", branch, meta_fmt)

            invoice_ws.write("A11", "Barcode", header_fmt)
            invoice_ws.write("B11", "Product Name", header_fmt)
            invoice_ws.write("C11", "PP", header_fmt)
            invoice_ws.write("D11", "Qty", header_fmt)
            invoice_ws.write("E11", "Total", header_fmt)

            for idx, row in df.iterrows():
                r = 11 + idx
                barcode = row["Barcode"]
                if barcode == "" or pd.isna(barcode):
                    invoice_ws.write_blank(r, 0, "", workbook.add_format({'border': 1}))
                else:
                    try:
                        invoice_ws.write_number(r, 0, int(barcode), workbook.add_format({'num_format': '0', 'border': 1}))
                    except:
                        invoice_ws.write(r, 0, str(barcode), workbook.add_format({'border': 1}))
                invoice_ws.write(r, 1, row["Product Name"])
                invoice_ws.write(r, 2, row["pp"])
                invoice_ws.write(r, 3, "")
                invoice_ws.write(r, 4, "")

            last = 11 + len(df)
            invoice_ws.merge_range(last, 0, last, 3, "Subtotal", merge_fmt)
            invoice_ws.write_blank(last, 4, "", meta_fmt)
            invoice_ws.merge_range(last + 1, 0, last + 1, 3, "Total", merge_fmt)
            invoice_ws.write_blank(last + 1, 4, "", meta_fmt)

            footer_texts = [
                "شركة خضار للتجارة و التسويق",
                "ش.ذ.م.م",
                "سجل تجارى / 13138  بطاقه ضريبية/721/294/448"
            ]
            for i, txt in enumerate(footer_texts):
                invoice_ws.merge_range(last + 3 + i, 0, last + 3 + i, 3, txt, center_fmt)

            invoice_ws.set_column("A:E", 25)

        output.seek(0)
        return output

    # ----------------------------
    # Begin main logic based on city
    # ----------------------------
    delivery_date = datetime.strptime(delivery_date_str, "%Y-%m-%d")

    if city == "Alexandria":
        # Read PDF into text
        all_text = ""
        with pdfplumber.open(BytesIO(pdf_file_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    all_text += "\n" + text

        # Find Alexandria FP # occurrences
        alex_matches = list(re.finditer(r"Alexandria FP #\d+", all_text))
        if len(alex_matches) < 2:
            raise ValueError("Less than two 'Alexandria FP #' entries found in the PDF.")

        second_fp_text = alex_matches[1].group()
        split_pos = alex_matches[1].start()

        # Determine branch order
        if "FP #2" in second_fp_text:
            branch_before = "سموحة"
            branch_after = "لوران"
        else:
            branch_before = "لوران"
            branch_after = "سموحة"

        text_part1 = all_text[:split_pos]
        text_part2 = all_text[split_pos:]

        # Extract PO values (first two occurrences)
        po_matches = re.findall(r"#P\d+", all_text)
        po_loran = po_matches[0] if len(po_matches) > 0 else ""
        po_smouha = po_matches[1] if len(po_matches) > 1 else ""

        # Extract DataFrames for each part
        df1 = extract_data_alex(text_part1, branch_before)
        df2 = extract_data_alex(text_part2, branch_after)

        # Generate pivot Excel for Alexandria
        pivot_excel = create_pivot_excel_alex(pd.concat([df1, df2], ignore_index=True))

        # Create invoice Excel for each branch
        excel1 = create_invoice_excel_alex(
            df1,
            invoice_number if branch_before == "لوران" else invoice_number + 1,
            branch_before,
            po_loran if branch_before == "لوران" else po_smouha,
            delivery_date
        )
        excel2 = create_invoice_excel_alex(
            df2,
            invoice_number if branch_after == "لوران" else invoice_number + 1,
            branch_after,
            po_loran if branch_after == "لوران" else po_smouha,
            delivery_date
        )

        # Build ZIP in memory
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr(f"orders_branch_{branch_before}.xlsx", excel1.getvalue())
            zip_file.writestr(f"orders_branch_{branch_after}.xlsx", excel2.getvalue())
            zip_file.writestr("مجمع اسكندرية.xlsx", pivot_excel.getvalue())
        zip_buffer.seek(0)
        return zip_buffer.getvalue()

    elif city == "Mansoura":
        # Read PDF into text
        all_text = ""
        with pdfplumber.open(BytesIO(pdf_file_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    all_text += "\n" + text

        # Extract PO value (first occurrence)
        po_match = re.search(r"#P\d+", all_text)
        po_value = po_match.group() if po_match else ""

        # Extract DataFrame
        df = extract_data_mansoura(all_text)

        # Generate pivot Excel for Mansoura
        pivot_excel = create_pivot_excel_mansoura(df)

        # Create invoice Excel for Mansoura
        excel_invoice = create_invoice_excel_mansoura(
            df,
            invoice_number,
            "المنصورة",
            po_value,
            delivery_date
        )

        # Build ZIP in memory
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr("مجمع المنصورة.xlsx", pivot_excel.getvalue())
            zip_file.writestr("فاتورة المنصورة.xlsx", excel_invoice.getvalue())
        zip_buffer.seek(0)
        return zip_buffer.getvalue()

    else:
        raise ValueError(f"Unsupported city: {city}")
