import zipfile, sqlite3, tempfile, os
zip_path = r'c:\Users\USER\OneDrive\桌面\CLOUD\church_finance\church_finance_deploy.zip'
if not os.path.exists(zip_path):
    print('zip missing')
    raise SystemExit
with zipfile.ZipFile(zip_path, 'r') as z:
    db_members = [n for n in z.namelist() if n.endswith('church.db')]
    print('db files in zip:', db_members)
    target = None
    for n in db_members:
        if n.endswith('/instance/church.db') or n.endswith('instance/church.db'):
            target = n
            break
    if not target and db_members:
        target = db_members[0]
    if not target:
        print('no db in zip')
        raise SystemExit
    with tempfile.TemporaryDirectory() as td:
        out = z.extract(target, td)
        con = sqlite3.connect(out)
        cur = con.cursor()
        tables = ['transactions','offering_counts','offering_denominations','members','personal_offerings','bank_transactions','bank_accounts','units','accounts','budgets','cashier_handovers','audit_records','monthly_reconciliations','accounting_checks']
        tset = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        total=0; counts={}
        for t in tables:
            if t in tset:
                n = cur.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
                counts[t]=n; total += n
        con.close()
        print('picked:', target)
        print('total=', total)
        print('counts=', counts)
