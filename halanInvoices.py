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
    - Summary sheet over whatever branches existed
    - For each branch present, two sheets:
        * فاتورة {branch}
        * فاتورة {branch}_filled
    - Normalizes:
        'حدايق الاهرام' → 'حدائق الاهرام'
        'مدينة نصر'   → 'مدينه نصر'
    - Skips missing branches without consuming invoice/PO slots.
    """

    # map any alternate spellings → canonical
    name_map = {
        'حدايق الاهرام': 'حدائق الاهرام',
        'مدينة نصر':   'مدينه نصر',
    }

    xls = pd.ExcelFile(BytesIO(excel_bytes))
    raw_sheets = xls.sheet_names
    sheets = [name_map.get(s.strip(), s.strip()) for s in raw_sheets]

    # read each sheet into small df
    dfs = []
    for orig_name, norm in zip(raw_sheets, sheets):
        df = pd.read_excel(xls, sheet_name=orig_name)
        df.columns = [c.strip() for c in df.columns]
        small = df.iloc[:, [0, -4, -3, -2]].copy()
        small.columns = ['Barcode', 'Product name', norm, f'price_{norm}']
        dfs.append(small)

    # build the master summary
    merged = reduce(
        lambda a, b: pd.merge(a, b, on=['Barcode','Product name'], how='outer'),
        dfs
    ).loc[lambda d: d['Barcode'].fillna(0) != 0]

    qty_cols   = sheets
    price_cols = [f'price_{s}' for s in sheets]
    merged[qty_cols+price_cols] = merged[qty_cols+price_cols] \
        .apply(pd.to_numeric, errors='coerce').fillna(0)

    merged['price']       = merged[price_cols].max(axis=1)
    merged['total qty']   = merged[qty_cols].sum(axis=1)
    merged['grand total'] = merged['price'] * merged['total qty']
    merged = merged.drop(columns=price_cols)
    merged['Barcode'] = merged['Barcode'].astype(float).map('{:.0f}'.format)

    # only include the fixed branches that actually appeared
    branch_order = ['مدينه نصر','حدائق الاهرام','جسر السويس','المقطم', 'اكتوبر']
    present = [b for b in branch_order if b in sheets]

    final_cols = ['Barcode','Product name'] + present + ['total qty','price','grand total']
    merged = merged[final_cols]

    # add totals row
    totals = {c: merged[c].sum() for c in final_cols if c not in ['Barcode','Product name']}
    totals.update({'Barcode':'المجموع','Product name':''})
    merged = pd.concat([merged, pd.DataFrame([totals])], ignore_index=True)

    # write Excel
    out = BytesIO()
    writer = pd.ExcelWriter(out, engine='xlsxwriter', datetime_format='yyyy-mm-dd')
    wb = writer.book
    merged.to_excel(writer, sheet_name='Summary', index=False)

    # formats
    fmt_meta     = wb.add_format({'bold': True,'border':2})
    fmt_hdr      = wb.add_format({'bold': True,'border':1,'align':'center'})
    fmt_border   = wb.add_format({'border':1})
    fmt_merge    = wb.add_format({'bold':True,'border':2,'align':'center','valign':'vcenter'})
    fmt_center   = wb.add_format({'bold':True,'align':'center'})
    fmt_br_right = wb.add_format({'bold':True,'border':2})

    inv = invoice_number
    po  = po_value

    for br in branch_order:
        if br not in present:
            continue

        for filled in (False, True):
            suffix = '_filled' if filled else ''
            sheet_name = f"فاتورة {br}{suffix}"
            ws = wb.add_worksheet(sheet_name)

            # optional logo
            try:
                ws.insert_image("A1", image_path, {'x_scale':0.5,'y_scale':0.5})
            except:
                pass

            # header block
            ws.write("A5", "شركه خضار للتجارة والتسويق", fmt_meta)
            ws.write("C1", "شركه خضار للتجارة والتسويق", fmt_meta)
            ws.write("C2", "Khodar for Trading & Marketing", fmt_meta)
            ws.write("F1", "فاتورة مبيعات", fmt_meta)
            ws.write("F2", "رقم الفاتورة #", fmt_meta)
            ws.write("F3", "تاريخ الاستلام", fmt_meta)
            ws.write("F4", "امر شراء رقم", fmt_meta)
            ws.write("F6", "اسم العميل", fmt_meta)
            ws.write("F7", "الفرع", fmt_meta)

            ws.write("E2", inv, fmt_meta)
            ws.write("E3", delivery_date, fmt_meta)
            ws.write("E4", po, wb.add_format({'border':2,'align':'center','bold':True}))
            ws.write("E6", f"حالا - فرع {br}", fmt_meta)
            ws.write("E7", br, fmt_meta)

            # branch data
            idx = sheets.index(br)
            dfb = dfs[idx].loc[lambda d: d['Barcode'].fillna(0)!=0].copy()
            dfb['Barcode'] = dfb['Barcode'].astype(float).map('{:.0f}'.format)
            dfb = dfb[['Barcode','Product name',br,f'price_{br}']] \
                  .rename(columns={br:'Qty',f'price_{br}':'price'})

            if not filled:
                dfb['Qty'] = ''
                dfb['Total'] = ''
            else:
                dfb['Total'] = dfb['Qty'] * dfb['price']

            # write headers
            for i,h in enumerate(['Barcode','Product name','price','Qty','Total']):
                ws.write(10, i, h, fmt_hdr)

            # write rows
            for r, row in enumerate(dfb.itertuples(index=False,name=None), start=11):
                ws.write(r,0,row[0],fmt_border)
                ws.write(r,1,row[1],fmt_border)
                ws.write(r,2,row[3],fmt_border)
                ws.write(r,3,row[2],fmt_border)
                ws.write(r,4,row[4],fmt_border)

            last = 11 + len(dfb)
            ws.merge_range(last,0,last,3,"Subtotal",fmt_merge)
            ws.write_blank(last,4,"",fmt_br_right)
            ws.merge_range(last+1,0,last+1,3,"Total",fmt_merge)
            if filled:
                ws.write_formula(last+1,4,f"=SUM(E12:E{last})",fmt_br_right)
            else:
                ws.write_blank(last+1,4,"",fmt_br_right)

            footer = [
                "شركة خضار للتجارة و التسويق",
                "ش.ذ.م.م",
                "سجل تجارى / 13138  بطاقه ضريبية/721/294/448"
            ]
            for i,txt in enumerate(footer, start=last+3):
                ws.merge_range(i,0,i,3,txt,fmt_center)

            ws.set_column("A:A",25)
            ws.set_column("B:B",25)
            ws.set_column("C:E",25)

        inv += 1
        po  += 1

    writer.close()
    out.seek(0)
    return out.getvalue(), delivery_date



