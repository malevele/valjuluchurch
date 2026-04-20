import sqlite3, os
paths = [r'c:\Users\USER\OneDrive\桌面\CLOUD\church_finance\church.db', r'c:\Users\USER\OneDrive\桌面\CLOUD\church_finance\instance\church.db']
tables = ['transactions','offering_counts','offering_denominations','members','personal_offerings','bank_transactions','bank_accounts','units','accounts','budgets','cashier_handovers','audit_records','monthly_reconciliations','accounting_checks']
for p in paths:
    print('\n'+p)
    if not os.path.exists(p):
        print('  missing')
        continue
    con = sqlite3.connect(p)
    cur = con.cursor()
    tset = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    total = 0
    counts = {}
    for t in tables:
        if t in tset:
            n = cur.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
            counts[t] = n
            total += n
    con.close()
    print(f'  size={os.path.getsize(p)} total={total}')
    print('  counts=', counts)
