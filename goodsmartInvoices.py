import pandas as pd
from io import BytesIO
from datetime import datetime
from config import barcode_to_product, categories_dict

def generate_invoice_excel(excel_bytes, invoice_number, delivery_date, po_value):
    def assign_category_with_barcode(df, barcode_to_product, categories_dict):
        product_to_category = {
            p.strip(): cat
            for cat, products in categories_dict.items()
            for p in products
        }

        def get_category(row):
            barcode = str(row.get("Barcode", "")).strip()
            prod_name = str(row.get("Product Name", "")).strip()

            product_from_barcode = barcode_to_product.get(barcode, "").strip()
            if product_from_barcode and product_from_barcode in product_to_category:
                return product_to_category[product_from_barcode]

            if prod_name in product_to_category:
                return product_to_category[prod_name]

            return "غير مصنف"

        df["Category"] = df.apply(get_category, axis=1)
        category_order = ["فاكهه", "خضار", "جاهز", "اعشاب", "غير مصنف"]
        df["Category"] = pd.Categorical(df["Category"], categories=category_order, ordered=True)
        df.sort_values(["Category", "Product Name"], inplace=True)
        return df

    def create_excel_file(df, invoice_num, delivery_date, po_value):
        output = BytesIO()
        branch_name = "Zaied"
        client_name = "Goodsmart - Zaied Branch"
        df = assign_category_with_barcode(df, barcode_to_product, categories_dict)

        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name="Orders", startrow=0)
            worksheet1 = writer.sheets["Orders"]
            workbook = writer.book

            number_format = workbook.add_format({'num_format': '0'})
            worksheet1.set_column("A:A", 20, number_format)
            worksheet1.set_column("B:B", 30)
            worksheet1.set_column("C:C", 15)
            worksheet1.set_column("D:D", 10)
            worksheet1.set_column("E:E", 20)
            worksheet1.set_column("F:F", 15)

            last_row_index = len(df) + 1
            bold_border = workbook.add_format({'bold': True, 'top': 2})
            worksheet1.write(last_row_index, 1, "Grand Total", bold_border)
            worksheet1.write_formula(last_row_index, 2, f"=SUM(C2:C{last_row_index})", bold_border)
            worksheet1.write_formula(last_row_index, 3, f"=SUM(D2:D{last_row_index})", bold_border)
            worksheet1.write_formula(last_row_index, 4, f"=SUM(E2:E{last_row_index})", bold_border)

            invoice_ws = workbook.add_worksheet("فاتورة")
            meta_format = workbook.add_format({'bold': True, 'border': 2})
            bold_center = workbook.add_format({'bold': True, 'align': 'center'})
            bold_merge = workbook.add_format({'bold': True, 'border': 2, 'align': 'center', 'valign': 'vcenter'})
            headers_format = workbook.add_format({'bold': True, 'border': 1, 'align': 'center'})
            border_format = workbook.add_format({'border': 1})
            bold_border_right = workbook.add_format({'bold': True, 'border': 2})

            try:
                invoice_ws.insert_image("A1", "Picture1.png", {'x_scale': 0.5, 'y_scale': 0.5})
            except:
                pass

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
            invoice_ws.write("E3", delivery_date, meta_format)
            invoice_ws.write("E4", po_value, workbook.add_format({'border': 2, 'align': 'center', 'bold': True}))
            invoice_ws.write("E6", client_name, meta_format)
            invoice_ws.write("E7", branch_name, meta_format)

            invoice_ws.write("A11", "Barcode", headers_format)
            invoice_ws.write("B11", "Product Name", headers_format)
            invoice_ws.write("C11", "PP", headers_format)
            invoice_ws.write("D11", "Qty", headers_format)
            invoice_ws.write("E11", "Total", headers_format)

            for idx, row in df.iterrows():
                row_num = 11 + idx
                barcode = row["Barcode"]
                name = row["Product Name"]
                cost = row["pp"]
                if pd.isna(barcode) or barcode == '':
                    invoice_ws.write_blank(row_num, 0, "", border_format)
                else:
                    try:
                        invoice_ws.write_number(row_num, 0, int(barcode), workbook.add_format({'num_format': '0', 'border': 1}))
                    except:
                        invoice_ws.write_string(row_num, 0, str(barcode), border_format)

                invoice_ws.write(row_num, 1, name, border_format)
                invoice_ws.write(row_num, 2, cost, border_format)
                invoice_ws.write(row_num, 3, "", border_format)
                invoice_ws.write(row_num, 4, "", border_format)

            last_row = 11 + len(df)
            invoice_ws.merge_range(last_row, 0, last_row, 3, "Subtotal", bold_merge)
            invoice_ws.write_blank(last_row, 4, "", bold_border_right)
            invoice_ws.merge_range(last_row + 1, 0, last_row + 1, 3, "Total", bold_merge)
            invoice_ws.write_blank(last_row + 1, 4, "", bold_border_right)

            footer_texts = [
                "شركة خضار للتجارة و التسويق",
                "ش.ذ.م.م",
                "سجل تجارى / 13138  بطاقه ضريبية/721/294/448"
            ]
            for i, text in enumerate(footer_texts):
                row = last_row + 3 + i
                invoice_ws.merge_range(row, 0, row, 3, text, bold_center)

            invoice_ws.set_column("A:A", 25)
            invoice_ws.set_column("B:B", 25)
            invoice_ws.set_column("C:E", 25)

        output.seek(0)
        return output.getvalue(), delivery_date

    df = pd.read_excel(BytesIO(excel_bytes))
    df.columns = df.columns.str.strip()

    required_columns = {
        "Barcode": "Barcode",
        "Arabic Name": "Product Name",
        "Cost": "pp",
        "Qty": "Qty",
        "Total Cost": "Total Cost"
    }

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df[list(required_columns.keys())].copy()
    df.rename(columns=required_columns, inplace=True)

    return create_excel_file(df, invoice_number, delivery_date, po_value)
