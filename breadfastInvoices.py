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

from config import barcode_to_product, categories_dict, ids_to_products

def process_breadfast_invoice(
    city: str,
    pdf_file_bytes: bytes,
    invoice_number: int,
    delivery_date_str: str
) -> bytes:
    """
    Processes a single PDF for Breadfast orders in either Alexandria, Mansoura or Cairo.
    Args:
        city: "Alexandria", "Mansoura" or "Cairo"
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
        # For Alexandria: insert "" at positions where ID == "5513135413135435131543"
        target_indexes = [i for i, id_val in enumerate(ids) if id_val == "5513135413135435131543"]
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
        df["Product Name"] = df["ID"].astype(str).map(ids_to_products).fillna("غير معروف")
        df["فرع"] = branch_name

        return df

    def extract_data_mansoura(text_block: str) -> pd.DataFrame:
        # For Mansoura: extract IDs, barcodes, quantities, prices
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
        df["Product Name"] = df["ID"].astype(str).map(ids_to_products).fillna("غير معروف")
        df["فرع"] = "المنصورة"

        return df

    def extract_data_cairo(text_block: str, branch_name: str) -> pd.DataFrame:
        # For Cairo: same extraction logic but with variable branch_name
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
        df["Product Name"] = df["ID"].astype(str).map(ids_to_products).fillna("غير معروف")
        df["فرع"] = branch_name

        return df

    def create_pivot_excel_alex(df: pd.DataFrame) -> BytesIO:
        # Pivot for Alexandria: branches "لوران" and "سموحة"
        pivot_df = df.pivot_table(
            index=["ID", "Barcode", "Product Name", "pp"],
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
        pivot_df = pivot_df[["ID", "Barcode", "Product Name", "لوران", "سموحة", "total_quantity", "pp", "total"]]

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
            index=["ID", "Barcode", "Product Name", "pp"],
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

    def create_pivot_excel_cairo(df: pd.DataFrame, branch_order_ar: list) -> BytesIO:
        # Pivot for Cairo: many branches
        pivot_df = df.pivot_table(
            index=["ID", "Barcode", "Product Name", "pp"],
            columns="فرع",
            values="Quantity",
            aggfunc="sum",
            fill_value=0
        ).reset_index()

        # Ensure all Cairo branch columns exist
        for col in branch_order_ar:
            if col not in pivot_df.columns:
                pivot_df[col] = 0

        # Sum totals across the branch columns
        pivot_df["total_quantity"] = pivot_df[branch_order_ar].sum(axis=1)
        pivot_df["total"] = pivot_df["total_quantity"] * pivot_df["pp"]

        # Reorder columns: ID, Barcode, Product Name, branch columns..., total_quantity, pp, total
        cols = ["ID", "Barcode", "Product Name"] + branch_order_ar + ["total_quantity", "pp", "total"]
        pivot_df = pivot_df[cols]

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
            pivot_df.to_excel(writer, index=False, sheet_name="مجمع القاهرة")
            workbook = writer.book
            worksheet = writer.sheets["مجمع القاهرة"]
            number_format = workbook.add_format({'num_format': '0'})
            quantity_format = workbook.add_format({'num_format': '0'})
            price_format = workbook.add_format({'num_format': '0.00'})
            total_format = workbook.add_format({'num_format': '0.00'})

            worksheet.set_column("A:A", 20, number_format)
            worksheet.set_column("B:B", 40)
            # Set widths for branch columns dynamically
            start_col = 2 + 1  # C is index 2 (0-based), but xlsxwriter uses letters when setting ranges so we'll just set wide columns
            worksheet.set_column(2, 2 + len(branch_order_ar) - 1, 12, quantity_format)
            worksheet.set_column(2 + len(branch_order_ar), 2 + len(branch_order_ar), 12, price_format)  # total_quantity column
            worksheet.set_column(2 + len(branch_order_ar) + 1, 2 + len(branch_order_ar) + 2, 12, price_format)  # pp and total
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
        # reuse the Alex invoice layout for Mansoura (same format)
        return create_invoice_excel_alex(df, invoice_num, branch, po_value, delivery_date)

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

    elif city == "Cairo":
        # Read PDF into text
        all_text = ""
        with pdfplumber.open(BytesIO(pdf_file_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    all_text += "\n" + text

        # Define expected English labels and their Arabic translations (in the order expected)
        cairo_labels_en = [
            "Garden City FP #1",
            "Maadi FP #1",
            "Maadi FP #2",
            "Maadi FP #3",
            "Maadi FP #4",
            "Madinaty FP #1",
            "Madinaty FP #2",
            "Helwan FP #1",
            "Shobra FP #1"
        ]
        # Arabic names mapping according to user request
        cairo_labels_ar = {
            "Garden City FP #1": "جاردن سيتي",
            "Maadi FP #1": "المعادي 1",
            "Maadi FP #2": "المعادي 2",
            "Maadi FP #3": "المعادي 3",
            "Maadi FP #4": "المعادي 4",
            "Madinaty FP #1": "مدينتي 1",
            "Madinaty FP #2": "مدينتي 2",
            "Helwan FP #1": "حلوان",
            "Shobra FP #1": "شبرا"
        }

        # Find matches for the expected labels in the PDF text (preserving order by start position)
        # Build alternation regex, escape strings to be safe
        alternation = "|".join([re.escape(lbl) for lbl in cairo_labels_en])
        matches = list(re.finditer(rf"({alternation})", all_text))

        # Validate found matches - we expect at least the 9 labelled sections
        if len(matches) < len(cairo_labels_en):
            raise ValueError(f"Expected {len(cairo_labels_en)} Cairo FP labels, found {len(matches)}. Labels must be present exactly as: {cairo_labels_en}")

        # Sort matches by start (just in case)
        matches.sort(key=lambda m: m.start())

        # Build text parts by slicing between matches
        parts = []
        for i, m in enumerate(matches):
            start = m.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(all_text)
            part_text = all_text[start:end]
            part_label_en = m.group(1)
            part_label_ar = cairo_labels_ar.get(part_label_en, part_label_en)
            parts.append((part_label_en, part_label_ar, part_text))

        # Extract PO values (any occurrences) and map sequentially to parts (if available)
        po_matches = re.findall(r"#P\d+", all_text)
        # Fill PO list with empty strings if fewer than parts
        po_for_parts = [po_matches[i] if i < len(po_matches) else "" for i in range(len(parts))]

        # Extract DataFrames for each Cairo part
        dfs = []
        for (en_label, ar_label, text_part) in parts:
            df_part = extract_data_cairo(text_part, ar_label)
            dfs.append((ar_label, df_part))

        # Concatenate all dfs for pivot
        concatenated_df = pd.concat([df for (_, df) in dfs], ignore_index=True) if dfs else pd.DataFrame(columns=["ID","Barcode","Quantity","pp","Product Name","فرع"])

        # Generate pivot Excel for Cairo
        branch_order_ar = [cairo_labels_ar[lbl] for lbl in cairo_labels_en]
        pivot_excel = create_pivot_excel_cairo(concatenated_df, branch_order_ar)

        # Create invoice Excel for each branch (invoice numbers sequential)
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            # Add pivot
            zip_file.writestr("مجمع القاهرة.xlsx", pivot_excel.getvalue())

            # create and write each branch invoice
            for idx, (ar_label, df_part) in enumerate(dfs):
                inv_num = invoice_number + idx
                po_val = po_for_parts[idx] if idx < len(po_for_parts) else ""
                excel_invoice = create_invoice_excel_alex(
                    df_part,
                    inv_num,
                    ar_label,
                    po_val,
                    delivery_date
                )
                # use safe filename - include index to avoid duplicates
                safe_name = f"orders_branch_{idx+1}_{ar_label}.xlsx"
                zip_file.writestr(safe_name, excel_invoice.getvalue())

        zip_buffer.seek(0)
        return zip_buffer.getvalue()

    else:
        raise ValueError(f"Unsupported city: {city}")
