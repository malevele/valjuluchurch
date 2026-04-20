import os, sqlite3, datetime
from pathlib import Path
roots = [
    Path(r'c:\Users\USER\OneDrive\桌面\CLOUD\church_finance'),
    Path(r'c:\Users\USER\OneDrive\桌面\CLOUD'),
]
tables = [
    'transactions','offering_counts','offering_denominations',
    'members','personal_offerings','bank_transactions',
    'bank_accounts','units','accounts','budgets',
    'cashier_handovers','audit_records','monthly_reconciliations','accounting_checks'
]
seen=set(); cands=[]
for root in roots:
    if not root.exists():
        continue
    for p in root.rglob('*'):
        if not p.is_file():
            continue
        n = p.name.lower()
        if n.endswith(('.db','.sqlite','.sqlite3','.bak')) or 'backup' in n or 'church.db' in n:
            sp = str(p.resolve())
            if sp not in seen:
                seen.add(sp); cands.append(Path(sp))
rows=[]
for p in sorted(cands, key=lambda x: str(x).lower()):
    try:
        con=sqlite3.connect(str(p)); cur=con.cursor()
        tset={r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        counts={}; total=0
        for t in tables:
            if t in tset:
                val=cur.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
                counts[t]=val; total += val
        con.close()
        rows.append((str(p), p.stat().st_size, datetime.datetime.fromtimestamp(p.stat().st_mtime), total, counts))
    except Exception:
        pass
rows.sort(key=lambda x: (x[3], x[1]), reverse=True)
print('Found files:', len(rows))
for p,size,mtime,total,counts in rows[:20]:
    print('\n'+p)
    print('  total_rows=', total, ' size=', size, ' mtime=', mtime)
    print('  key=', {k:counts.get(k,0) for k in ['transactions','offering_counts','members','personal_offerings','bank_transactions','units','accounts']})
