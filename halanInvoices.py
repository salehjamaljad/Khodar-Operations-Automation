import pandas as pd
from io import BytesIO
from functools import reduce

def build_master_and_invoices_bytes(
    excel_bytes: bytes,
    invoice_number: int,
    delivery_date: str,
    po_value: int,
    image_path: str = "Picture1.png"
) -> tuple[bytes, str]:
    """
    Takes Excel bytes input and generates a master summary sheet plus 8 invoice sheets,
    returning the result as an in-memory Excel file (bytes) and the delivery date.
    
    Column positions are now fixed:
      - col 0: Barcode
      - col 2: Product name
      - col 4: Qty (named by sheet)
      - col -2: Price

    Handles variant sheet names for the Haram Gardens branch:
      both 'حدائق الاهرام' and 'حدايق الاهرام' map to 'حدائق الاهرام'.
    """
    # mapping of alternate sheet names to canonical
    name_map = {
        'حدايق الاهرام': 'حدائق الاهرام',
    }

    xls = pd.ExcelFile(BytesIO(excel_bytes))
    # normalize sheet names
    sheets = [name_map.get(s.strip(), s.strip()) for s in xls.sheet_names]
    dfs = []

    for orig in xls.sheet_names:
        raw = orig.strip()
        name = name_map.get(raw, raw)
        df = pd.read_excel(xls, sheet_name=orig)
        df.columns = [c.strip() for c in df.columns]

        # select by position: 0,1,4,-2
        tmp = df.iloc[:, [0, 3, 4, -2]].copy()
        tmp.columns = ['Barcode', 'Product name', name, f'price_{name}']
        dfs.append(tmp)

    # merge on Barcode & Product name
    merged = reduce(
        lambda a, b: pd.merge(a, b, on=['Barcode', 'Product name'], how='outer'),
        dfs
    )
    merged = merged[merged['Barcode'].fillna(0) != 0]

    qty_cols = sheets
    price_cols = [f'price_{s}' for s in sheets]
    merged[qty_cols + price_cols] = (
        merged[qty_cols + price_cols]
        .apply(pd.to_numeric, errors='coerce')
        .fillna(0)
    )

    # compute price, totals
    merged['price'] = merged[price_cols].max(axis=1)
    merged['total qty'] = merged[qty_cols].sum(axis=1)
    merged['grand total'] = merged['total qty'] * merged['price']
    merged.drop(columns=price_cols, inplace=True)
    merged['Barcode'] = merged['Barcode'].astype(float).map('{:.0f}'.format)

    final_cols = [
        'Barcode', 'Product name',
        'مدينه نصر', 'جسر السويس', 'حدائق الاهرام', 'المقطم',
        'total qty', 'price', 'grand total'
    ]
    merged = merged[final_cols]

    totals = {col: merged[col].sum() for col in final_cols}
    totals.update({'Barcode': 'المجموع', 'Product name': ''})
    merged = pd.concat([merged, pd.DataFrame([totals])], ignore_index=True)

    # WRITE TO MEMORY
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='yyyy-mm-dd')
    wb = writer.book
    merged.to_excel(writer, sheet_name='Summary', index=False)

    # formats
    meta_fmt = wb.add_format({'bold': True, 'border': 2})
    bold_center = wb.add_format({'bold': True, 'align': 'center'})
    bold_merge = wb.add_format({'bold': True, 'border': 2, 'align': 'center', 'valign': 'vcenter'})
    headers_fmt = wb.add_format({'bold': True, 'border': 1, 'align': 'center'})
    border_fmt = wb.add_format({'border': 1})
    bold_br_right = wb.add_format({'bold': True, 'border': 2})

    branch_order = ['مدينه نصر', 'حدائق الاهرام', 'جسر السويس', 'المقطم']
    inv_num = invoice_number
    po_val = po_value

    for branch in branch_order:
        for fill in (False, True):
            sheet_name = f"{branch}{'' if not fill else '_filled'}"
            ws = wb.add_worksheet(sheet_name)

            try:
                ws.insert_image("A1", image_path, {'x_scale': 0.5, 'y_scale': 0.5})
            except:
                pass

            ws.write("A5", "شركه خضار للتجارة والتسويق", meta_fmt)
            ws.write("C1", "شركه خضار للتجارة والتسويق", meta_fmt)
            ws.write("C2", "Khodar for Trading & Marketing", meta_fmt)
            ws.write("F1", "فاتورة مبيعات", meta_fmt)
            ws.write("F2", "رقم الفاتورة #", meta_fmt)
            ws.write("F3", "تاريخ الاستلام", meta_fmt)
            ws.write("F4", "امر شراء رقم", meta_fmt)
            ws.write("F6", "اسم العميل", meta_fmt)
            ws.write("F7", "الفرع", meta_fmt)

            ws.write("E2", inv_num, meta_fmt)
            ws.write("E3", delivery_date, meta_fmt)
            ws.write("E4", po_val, wb.add_format({'border': 2, 'align': 'center', 'bold': True}))
            client_name = f"حالا - فرع {branch}"
            ws.write("E6", client_name, meta_fmt)
            ws.write("E7", branch, meta_fmt)

            dfb = dfs[sheets.index(branch)].copy()
            dfb = dfb[dfb['Barcode'].fillna(0) != 0]
            dfb['Barcode'] = dfb['Barcode'].astype(float).map('{:.0f}'.format)
            dfb = dfb[['Barcode', 'Product name', branch, f'price_{branch}']].rename(columns={
                branch: 'Qty',
                f'price_{branch}': 'price'
            })

            if not fill:
                dfb['Qty'] = ''
                dfb['Total'] = ''
            else:
                dfb['Total'] = dfb['Qty'] * dfb['price']

            for col_idx, header in enumerate(['Barcode', 'Product name', 'price', 'Qty', 'Total']):
                ws.write(10, col_idx, header, headers_fmt)

            for row_idx, row in enumerate(dfb.itertuples(index=False, name=None), start=11):
                ws.write(row_idx, 0, row[0], border_fmt)
                ws.write(row_idx, 1, row[1], border_fmt)
                ws.write(row_idx, 2, row[3], border_fmt)
                ws.write(row_idx, 3, row[2], border_fmt)
                ws.write(row_idx, 4, row[4], border_fmt)

            last = 11 + len(dfb)
            ws.merge_range(last, 0, last, 3, "Subtotal", bold_merge)
            ws.write_blank(last, 4, "", bold_br_right)
            ws.merge_range(last+1, 0, last+1, 3, "Total", bold_merge)
            if fill:
                ws.write_formula(last+1, 4, f"=SUM(E12:E{last})", bold_br_right)
            else:
                ws.write_blank(last+1, 4, "", bold_br_right)

            footer_texts = [
                "شركة خضار للتجارة و التسويق",
                "ش.ذ.م.م",
                "سجل تجارى / 13138  بطاقه ضريبية/721/294/448"
            ]
            for i, txt in enumerate(footer_texts, start=last+3):
                ws.merge_range(i, 0, i, 3, txt, bold_center)

            ws.set_column("A:A", 25)
            ws.set_column("B:B", 25)
            ws.set_column("C:E", 25)

        inv_num += 1
        po_val += 1

    writer.close()
    output.seek(0)
    return output.getvalue(), delivery_date
