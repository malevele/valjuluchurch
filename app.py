import os
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from datetime import date, datetime
from decimal import Decimal
from sqlalchemy import func, extract
from sqlalchemy.orm import joinedload
from models import (db, Account, BankAccount, BankTransaction, Unit, Transaction,
                    OfferingCount, OfferingDenomination, Budget,
                    CashierHandover, AuditRecord, Member, PersonalOffering,
                           MonthlyReconciliation, AccountingCheck,
            DENOMINATIONS, MEETING_TYPES, OFFERING_TYPES, YEAR_RANGE)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'church-finance-secret-2025'
# 使用絕對路徑，確保無論工作目錄為何，資料庫位置永遠固定
_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'church.db')
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{_DB_PATH}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)


@app.context_processor
def inject_globals():
    units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    return {'now': datetime.now(), 'cashbook_units': units}


# ── 初始化資料庫 ────────────────────────────────────────────────────────────
def init_db():
    db.create_all()
    # 補舊資料庫缺少的欄位
    from sqlalchemy import text
    with db.engine.connect() as conn:
        for sql in [
            'ALTER TABLE bank_accounts ADD COLUMN unit_id INTEGER REFERENCES units(id)',
            'ALTER TABLE offering_counts ADD COLUMN account_id INTEGER REFERENCES accounts(id)',
            'ALTER TABLE offering_counts ADD COLUMN unit_id INTEGER REFERENCES units(id)',
            'ALTER TABLE offering_counts ADD COLUMN description VARCHAR(200)',
            'ALTER TABLE units ADD COLUMN code VARCHAR(10)',
            'ALTER TABLE units ADD COLUMN sort_order INTEGER DEFAULT 999',
            'ALTER TABLE accounts ADD COLUMN description VARCHAR(200)',
            'ALTER TABLE personal_offerings ADD COLUMN unit_id INTEGER REFERENCES units(id)',
            'ALTER TABLE personal_offerings ADD COLUMN account_id INTEGER REFERENCES accounts(id)',
            "ALTER TABLE accounting_checks ADD COLUMN audit_type VARCHAR(10) DEFAULT '季度查帳'",
            'ALTER TABLE accounting_checks ADD COLUMN prev_period_balance NUMERIC(12,2) DEFAULT 0',
        ]:
            try:
                conn.execute(text(sql))
                conn.commit()
            except Exception:
                pass  # 欄位已存在則忽略
    # 設定各單位預設排序與傳票簡碼
    _unit_meta = [
        ('大會',           'A'),
        ('建堂基金',       'B'),
        ('關懷小組',       'C'),
        ('松年團契',       'D'),
        ('弟兄團契',       'E'),
        ('婦女會',         'F'),
        ('社青團契',       'G'),
        ('青少年團契',     'H'),
        ('傳道小組',       'I'),
        ('聖歌隊',         'J'),
        ('主日學',         'K'),
        ('家庭禮拜第一小組', 'L1'),
        ('家庭禮拜第二小組', 'L2'),
        ('家庭禮拜第三小組', 'L3'),
        ('家庭禮拜第四小組', 'L4'),
        ('家庭禮拜第五小組', 'L5'),
        ('課輔班',         'M'),
        ('瓦酪露關懷協會', 'N'),
    ]
    # ── 建立索引（CREATE INDEX IF NOT EXISTS，完全不動資料）──────────────────
    _indexes = [
        # transactions：最常被篩選的欄位
        'CREATE INDEX IF NOT EXISTS idx_tx_unit_date   ON transactions(unit_id, date)',
        'CREATE INDEX IF NOT EXISTS idx_tx_date        ON transactions(date)',
        'CREATE INDEX IF NOT EXISTS idx_tx_account     ON transactions(account_id)',
        'CREATE INDEX IF NOT EXISTS idx_tx_voucher     ON transactions(voucher_no)',
        'CREATE INDEX IF NOT EXISTS idx_tx_year_month  ON transactions(strftime("%Y", date), strftime("%m", date))',
        # offering_counts
        'CREATE INDEX IF NOT EXISTS idx_oc_unit        ON offering_counts(unit_id)',
        'CREATE INDEX IF NOT EXISTS idx_oc_date        ON offering_counts(date)',
        'CREATE INDEX IF NOT EXISTS idx_oc_attach      ON offering_counts(attachment_no)',
        # personal_offerings
        'CREATE INDEX IF NOT EXISTS idx_po_member      ON personal_offerings(member_id)',
        'CREATE INDEX IF NOT EXISTS idx_po_year        ON personal_offerings(year, month)',
        # monthly_reconciliations
        'CREATE INDEX IF NOT EXISTS idx_recon_unit     ON monthly_reconciliations(unit_id, year, month)',
        # bank_transactions
        'CREATE INDEX IF NOT EXISTS idx_btx_account    ON bank_transactions(bank_account_id, date)',
    ]
    with db.engine.connect() as conn:
        for sql in _indexes:
            try:
                conn.execute(text(sql))
                conn.commit()
            except Exception:
                pass
    # 單位改名：關懷協會 → 關懷小組（僅首次，若目標名稱不存在）
    old_u = Unit.query.filter_by(name='關懷協會').first()
    if old_u and not Unit.query.filter_by(name='關懷小組').first():
        old_u.name = '關懷小組'
    # 僅在 code 尚未設定 或 sort_order 仍為預設值(999) 時才寫入初始值
    # 使用者已手動設定的值不覆蓋
    for idx, (uname, ucode) in enumerate(_unit_meta, start=10):
        u = Unit.query.filter_by(name=uname).first()
        if u:
            if not u.code:
                u.code = ucode
            if u.sort_order == 999:
                u.sort_order = idx
    db.session.commit()
    if Account.query.count() == 0:
        seed_accounts()
    if Unit.query.count() == 0:
        seed_units()


def seed_accounts():
    """建立預設會計科目（依長老教會總會標準）"""
    income_accounts = [
        ('I01', '月定奉獻', 1), ('I02', '禮拜獻金', 2), ('I03', '聖餐獻金', 3),
        ('I04', '節期獻金', 4), ('I05', '第一主日奉獻', 5), ('I06', '第二主日奉獻', 6),
        ('I07', '第三主日奉獻', 7), ('I08', '第四主日奉獻', 8), ('I09', '第五主日奉獻', 9),
        ('I10', '感恩奉獻', 10), ('I11', '感謝奉獻', 11), ('I12', '特別奉獻', 12),
        ('I13', '教會典禮費', 13), ('I14', '特別典禮費', 14), ('I15', '復活節獻金', 15),
        ('I16', '培靈佈道費', 16), ('I17', '義賣收入', 17), ('I18', '攤位奉獻', 18),
        ('I19', '大會奉獻', 19), ('I20', '中會奉獻', 20), ('I21', '百分百中會', 21),
        ('I22', '建築及專案獻金', 22), ('I23', '建堂基金', 23), ('I24', '其他收入', 24),
    ]
    expense_accounts = [
        ('E01', '傳教師謝禮', 1), ('E02', '傳教師及職工薪資', 2), ('E03', '工資/勞務報酬', 3),
        ('E04', '牧師實務費', 4), ('E05', '講師費', 5), ('E06', '教會事工費', 6),
        ('E07', '團契事工費', 7), ('E08', '靈修活動支出', 8), ('E09', '查經講師費', 9),
        ('E10', '教會典禮費', 10), ('E11', '特別典禮費', 11), ('E12', '培靈活動費', 12),
        ('E13', '旅運費', 13), ('E14', '油資', 14), ('E15', '招待費', 15),
        ('E16', '茶水費', 16), ('E17', '便當', 17), ('E18', '中午招待湯費', 18),
        ('E19', '消耗品費', 19), ('E20', '奉獻袋', 20), ('E21', '開會點心費', 21),
        ('E22', '雜支', 22), ('E23', '慈善慰問費', 23), ('E24', '生日禮金', 24),
        ('E25', '白包/探訪費', 25), ('E26', '圖書教材及印刷', 26), ('E27', '備品設備', 27),
        ('E28', '定期會議費', 28), ('E29', '報名費', 29), ('E30', '母親節禮金', 30),
        ('E31', '負擔金', 31), ('E32', '其他支出', 32),
    ]
    for code, name, order in income_accounts:
        db.session.add(Account(code=code, name=name, type='income', sort_order=order))
    for code, name, order in expense_accounts:
        db.session.add(Account(code=code, name=name, type='expense', sort_order=order))
    db.session.commit()


def seed_units():
    """建立預設單位（教會本帳排第一）"""
    units = [
        ('教會本帳', '主帳'),
        ('松年團契', '團契'), ('弟兄團契', '團契'), ('婦女團契', '團契'),
        ('社青團契', '團契'), ('青少年團契', '團契'), ('主日學', '事工'),
        ('家庭小組第一組', '小組'), ('家庭小組第二組', '小組'),
        ('家庭小組第三組', '小組'), ('家庭小組第四組', '小組'),
        ('家庭小組第五組', '小組'), ('課輔班', '事工'),
        ('關懷協會', '協力'), ('瓦酪露發展協會', '協力'),
    ]
    for name, typ in units:
        db.session.add(Unit(name=name, type=typ))
    db.session.commit()


# ── 輔助函式 ─────────────────────────────────────────────────────────────────
def performance_monitor(f):
    """性能監控裝飾器（簡易版）"""
    import functools
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        import time
        start = time.time()
        result = f(*args, **kwargs)
        elapsed = time.time() - start
        if elapsed > 1.0:  # 超過1秒記錄
            print(f"慢查詢: {f.__name__} 耗時 {elapsed:.2f}秒")
        return result
    return wrapper


def unit_prev_balance(unit_id, tx_date, exclude_id=None):
    """取得某單位在指定日期（含）前最後一筆的餘額。"""
    q = Transaction.query.filter(
        Transaction.unit_id == unit_id,
        Transaction.date <= tx_date,
    )
    if exclude_id:
        q = q.filter(Transaction.id != exclude_id)
    prev = q.order_by(Transaction.date.desc(), Transaction.id.desc()).first()
    return prev.balance if prev else Decimal('0')


def tx_period_totals(unit_id, year, start_month, end_month):
    """取得指定期間（同一年）收入與支出總額。"""
    q = db.session.query(
        func.coalesce(func.sum(Transaction.amount_in), 0).label('income'),
        func.coalesce(func.sum(Transaction.amount_out), 0).label('expense'),
    ).filter(
        extract('year', Transaction.date) == year,
        extract('month', Transaction.date) >= start_month,
        extract('month', Transaction.date) <= end_month,
    )
    if unit_id:
        q = q.filter(Transaction.unit_id == unit_id)
    row = q.first()
    return (row.income or Decimal('0')), (row.expense or Decimal('0'))


def tx_cumulative_balance(unit_id, end_year, end_month):
    """取得截至指定年月（含）之累計結存（收入-支出）。"""
    q = db.session.query(
        func.coalesce(func.sum(Transaction.amount_in - Transaction.amount_out), 0)
    )
    if unit_id:
        q = q.filter(Transaction.unit_id == unit_id)
    q = q.filter(
        (extract('year', Transaction.date) < end_year) |
        ((extract('year', Transaction.date) == end_year) &
         (extract('month', Transaction.date) <= end_month))
    )
    return q.scalar() or Decimal('0')


def accounting_period(audit_type):
    """回傳查帳期間設定：(start_month, end_month, quarter, label)。"""
    mapping = {
        '第一季查帳': (1, 3, 1, '1-3月'),
        '第二季查帳': (4, 6, 2, '4-6月'),
        '第三季查帳': (7, 9, 3, '7-9月'),
        '第四季查帳': (10, 12, 4, '10-12月'),
        '年度總查': (1, 12, None, '1-12月'),
    }
    return mapping.get(audit_type, mapping['第一季查帳'])


def next_voucher_no(vtype, year, unit_id=None):
    """自動產生傳票號，含單位簡碼：收-A-2026-001 / 支-B-2026-001"""
    prefix = '收' if vtype == 'income' else '支'
    unit_code = ''
    if unit_id:
        u = Unit.query.get(unit_id)
        if u and u.code:
            unit_code = f'-{u.code}'
    q = Transaction.query.filter(
        Transaction.voucher_type == vtype,
        extract('year', Transaction.date) == year,
    )
    if unit_id:
        q = q.filter(Transaction.unit_id == unit_id)
    count = q.count()
    return f'{prefix}{unit_code}-{year}-{count + 1:03d}'


@app.route('/unit/<int:unit_id>')
def unit_dashboard(unit_id):
    """單位專屬儀表板"""
    unit = Unit.query.get_or_404(unit_id)
    today = date.today()
    year, month = today.year, today.month

    # 本單位本月收支統計
    monthly_stats = db.session.query(
        func.sum(Transaction.amount_in).label('income'),
        func.sum(Transaction.amount_out).label('expense')
    ).filter(
        Transaction.unit_id == unit_id,
        extract('year', Transaction.date) == year,
        extract('month', Transaction.date) == month
    ).first()

    monthly_income = monthly_stats.income or Decimal('0')
    monthly_expense = monthly_stats.expense or Decimal('0')

    # 單位最新餘額
    latest_tx = Transaction.query.filter_by(unit_id=unit_id)\
        .order_by(Transaction.date.desc(), Transaction.id.desc()).first()
    current_balance = latest_tx.balance if latest_tx else Decimal('0')

    # 年度累計收支
    yearly_stats = db.session.query(
        func.sum(Transaction.amount_in).label('income'),
        func.sum(Transaction.amount_out).label('expense')
    ).filter(
        Transaction.unit_id == unit_id,
        extract('year', Transaction.date) == year
    ).first()

    yearly_income = yearly_stats.income or Decimal('0')
    yearly_expense = yearly_stats.expense or Decimal('0')

    # 最近10筆交易
    recent_txs = Transaction.query.filter_by(unit_id=unit_id)\
        .order_by(Transaction.date.desc(), Transaction.id.desc()).limit(10).all()

    # 本月前5大收入科目
    top_income = db.session.query(
        Account.name,
        func.sum(Transaction.amount_in).label('total')
    ).join(Transaction, Transaction.account_id == Account.id).filter(
        Transaction.unit_id == unit_id,
        extract('year', Transaction.date) == year,
        extract('month', Transaction.date) == month,
        Transaction.amount_in > 0
    ).group_by(Account.id).order_by(func.sum(Transaction.amount_in).desc()).limit(5).all()

    # 最近奉獻點算
    recent_offerings = OfferingCount.query.filter_by(unit_id=unit_id)\
        .order_by(OfferingCount.date.desc()).limit(5).all()

    # 銀行帳戶
    bank_accounts = BankAccount.query.filter_by(unit_id=unit_id, is_active=True).all()
    bank_total = sum(b.current_balance for b in bank_accounts) or Decimal('0')

    return render_template('unit_dashboard.html',
        unit=unit, today=today, year=year, month=month,
        monthly_income=monthly_income, monthly_expense=monthly_expense,
        current_balance=current_balance,
        yearly_income=yearly_income, yearly_expense=yearly_expense,
        recent_txs=recent_txs, top_income=top_income,
        recent_offerings=recent_offerings,
        bank_accounts=bank_accounts, bank_total=bank_total,
    )


@app.route('/')
@app.route('/dashboard')
def dashboard():
    """主儀表板 — 全會教財務概覽"""
    active_units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    active_unit_ids = [u.id for u in active_units]
    today = date.today()
    year = request.args.get('year', today.year, type=int)
    if year not in YEAR_RANGE:
        year = today.year
    month = today.month if year == today.year else 12
    years = YEAR_RANGE

    # 本月收支統計
    monthly_income = db.session.query(
        func.sum(Transaction.amount_in)
    ).filter(
        Transaction.unit_id.in_(active_unit_ids),
        extract('year', Transaction.date) == year,
        extract('month', Transaction.date) == month
    ).scalar() or Decimal('0')

    monthly_expense = db.session.query(
        func.sum(Transaction.amount_out)
    ).filter(
        Transaction.unit_id.in_(active_unit_ids),
        extract('year', Transaction.date) == year,
        extract('month', Transaction.date) == month
    ).scalar() or Decimal('0')

    # 各單位現金簿最新餘額加總 = 帳面總結餘（優化：單一查詢）
    # 使用子查詢獲取每個單位的最新交易
    from sqlalchemy import and_
    subq = db.session.query(
        Transaction.unit_id,
        func.max(Transaction.date).label('max_date'),
        func.max(Transaction.id).label('max_id')
    ).filter(Transaction.unit_id.in_([u.id for u in active_units]))\
     .group_by(Transaction.unit_id).subquery()

    latest_balances = db.session.query(
        Transaction.unit_id,
        Transaction.balance
    ).join(subq, and_(
        Transaction.unit_id == subq.c.unit_id,
        Transaction.date == subq.c.max_date,
        Transaction.id == subq.c.max_id
    )).all()

    # 帳面餘額使用「截至所選年度末」的累計值，避免跨年度視圖不同步
    current_balance = tx_cumulative_balance(None, year, month)

    # 年度總收入 / 總支出
    yearly_income = db.session.query(
        func.sum(Transaction.amount_in)
    ).filter(
        Transaction.unit_id.in_(active_unit_ids),
        extract('year', Transaction.date) == year
    ).scalar() or Decimal('0')

    yearly_expense = db.session.query(
        func.sum(Transaction.amount_out)
    ).filter(
        Transaction.unit_id.in_(active_unit_ids),
        extract('year', Transaction.date) == year
    ).scalar() or Decimal('0')

    # 本月最近10筆交易
    recent_txs = Transaction.query.filter(
        Transaction.unit_id.in_(active_unit_ids),
        extract('year', Transaction.date) == year,
        extract('month', Transaction.date) == month
    ).order_by(Transaction.date.desc(), Transaction.id.desc()).limit(10).all()

    # 本月按科目統計（前5大收入科目）
    top_income = db.session.query(
        Account.name,
        func.sum(Transaction.amount_in).label('total')
    ).join(Transaction, Transaction.account_id == Account.id).filter(
        Transaction.unit_id.in_(active_unit_ids),
        extract('year', Transaction.date) == year,
        extract('month', Transaction.date) == month,
        Transaction.amount_in > 0
    ).group_by(Account.id).order_by(func.sum(Transaction.amount_in).desc()).limit(5).all()

    # 最近奉獻點算
    recent_offerings = OfferingCount.query.filter(
        OfferingCount.unit_id.in_(active_unit_ids)
    ).order_by(OfferingCount.date.desc()).limit(5).all()

    # 各銀行帳戶餘額
    bank_accounts = BankAccount.query.filter(
        BankAccount.is_active == True,
        BankAccount.unit_id.in_(active_unit_ids)
    ).all()
    bank_total = sum(b.current_balance for b in bank_accounts) or Decimal('0')

    # 各單位帳面結餘（for dashboard）（優化：重用上面的查詢）
    unit_balances_dash = {unit_id: balance for unit_id, balance in latest_balances}

    # 各單位年度彙總（供財務總表）
    unit_summary = []
    total_cash = Decimal('0')
    for u in active_units:
        income = db.session.query(func.coalesce(func.sum(Transaction.amount_in), 0)).filter(
            Transaction.unit_id == u.id,
            extract('year', Transaction.date) == year,
        ).scalar() or Decimal('0')
        expense = db.session.query(func.coalesce(func.sum(Transaction.amount_out), 0)).filter(
            Transaction.unit_id == u.id,
            extract('year', Transaction.date) == year,
        ).scalar() or Decimal('0')
        balance = tx_cumulative_balance(u.id, year, month)
        bank = sum(
            b.current_balance for b in BankAccount.query.filter_by(unit_id=u.id, is_active=True).all()
        ) or Decimal('0')
        cash = balance - bank
        total_cash += cash
        unit_summary.append({
            'unit': u,
            'income': income,
            'expense': expense,
            'balance': balance,
            'bank': bank,
            'cash': cash,
        })

    # 統一口徑：頂端卡片與表尾合計皆以各單位彙總為準
    yearly_income = sum(s['income'] for s in unit_summary) or Decimal('0')
    yearly_expense = sum(s['expense'] for s in unit_summary) or Decimal('0')
    current_balance = sum(s['balance'] for s in unit_summary) or Decimal('0')
    bank_total = sum(s['bank'] for s in unit_summary) or Decimal('0')

    return render_template('dashboard.html',
        today=today, year=year, month=month,
        years=years,
        monthly_income=monthly_income,
        monthly_expense=monthly_expense,
        current_balance=current_balance,
        bank_accounts=bank_accounts,
        bank_total=bank_total,
        unit_balances_dash=unit_balances_dash,
        unit_summary=unit_summary,
        total_cash=total_cash,
        recent_txs=recent_txs,
        top_income=top_income,
        yearly_income=yearly_income,
        yearly_expense=yearly_expense,
        recent_offerings=recent_offerings,
    )


# ── 銀行明細輔助 ─────────────────────────────────────────────────────────────
def recalc_unit_balances(unit_id, from_date=None):
    """重新計算某單位的累積餘額。
    from_date 指定時，只重算該日期（含）之後的記錄，大幅減少運算量。
    """
    if from_date:
        # 取 from_date 前的最後一筆餘額作為起始值
        prev = Transaction.query.filter(
            Transaction.unit_id == unit_id,
            Transaction.date < from_date
        ).order_by(Transaction.date.desc(), Transaction.id.desc()).first()
        bal = prev.balance if prev else Decimal('0')
        txs = Transaction.query.filter(
            Transaction.unit_id == unit_id,
            Transaction.date >= from_date
        ).order_by(Transaction.date, Transaction.id).all()
    else:
        bal = Decimal('0')
        txs = Transaction.query.filter_by(unit_id=unit_id)\
            .order_by(Transaction.date, Transaction.id).all()

    for tx in txs:
        bal += tx.amount_in - tx.amount_out
        tx.balance = bal
    db.session.commit()


def recalc_bank_balances(bank_account_id):
    txs = BankTransaction.query.filter_by(bank_account_id=bank_account_id)\
        .order_by(BankTransaction.date, BankTransaction.id).all()
    bal = Decimal('0')
    for t in txs:
        bal += t.amount if t.tx_type == '存入' else -t.amount
        t.balance = bal
    ba = BankAccount.query.get(bank_account_id)
    if ba:
        ba.current_balance = bal
    db.session.commit()


@app.route('/bank_tx/add', methods=['POST'])
def bank_tx_add():
    try:
        ba_id   = int(request.form['bank_account_id'])
        amount  = Decimal(request.form.get('amount') or '0')
        tx_type = request.form['tx_type']
        tx = BankTransaction(
            bank_account_id=ba_id,
            date=datetime.strptime(request.form['date'], '%Y-%m-%d').date(),
            tx_type=tx_type,
            amount=amount,
            description=request.form.get('description', '').strip(),
        )
        db.session.add(tx)
        # 直接在現有餘額上加減，保留帳戶初始餘額
        ba = BankAccount.query.get(ba_id)
        if ba:
            if tx_type == '存入':
                ba.current_balance += amount
            else:
                ba.current_balance -= amount
        db.session.commit()
        flash('銀行交易已新增', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'新增失敗：{e}', 'danger')
    return redirect(request.referrer or url_for('cashbook'))


@app.route('/bank_tx/<int:tx_id>/delete', methods=['POST'])
def bank_tx_delete(tx_id):
    tx = BankTransaction.query.get_or_404(tx_id)
    ba_id = tx.bank_account_id
    ba = BankAccount.query.get(ba_id)
    if ba:
        # 刪除時反向調整餘額
        if tx.tx_type == '存入':
            ba.current_balance -= tx.amount
        else:
            ba.current_balance += tx.amount
    db.session.delete(tx)
    db.session.commit()
    flash('銀行交易已刪除', 'warning')
    return redirect(request.referrer or url_for('cashbook'))


# ── 現金簿 ───────────────────────────────────────────────────────────────────
@app.route('/cashbook')
def cashbook():
    year    = request.args.get('year',    date.today().year,  type=int)
    month   = request.args.get('month',   date.today().month, type=int)
    unit_id = request.args.get('unit_id', 0, type=int)  # 0 = 全部

    units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    accounts = Account.query.order_by(Account.type, Account.sort_order).all()
    bank_accounts = BankAccount.query.filter_by(is_active=True).all()

    q = Transaction.query.filter(
        extract('year', Transaction.date) == year,
        extract('month', Transaction.date) == month,
    )
    if unit_id:
        q = q.filter(Transaction.unit_id == unit_id)
    txs = q.order_by(Transaction.date, Transaction.id).all()

    monthly_in  = sum(t.amount_in  for t in txs) if txs else Decimal('0')
    monthly_out = sum(t.amount_out for t in txs) if txs else Decimal('0')

    # 當月前結存（當選定單位時）
    prev_balance = Decimal('0')
    if unit_id and txs:
        first_tx = txs[0]
        prev = Transaction.query.filter(
            Transaction.unit_id == unit_id,
            Transaction.date < first_tx.date
        ).order_by(Transaction.date.desc(), Transaction.id.desc()).first()
        if not prev:
            # 同月比第一筆更早的
            prev = Transaction.query.filter(
                Transaction.unit_id == unit_id,
                Transaction.date == first_tx.date,
                Transaction.id < first_tx.id
            ).order_by(Transaction.id.desc()).first()
        prev_balance = prev.balance if prev else Decimal('0')

    # 有奉獻點算表的傳票號 → {voucher_no: oc_id}
    offering_map = {oc.attachment_no: oc.id for oc in
        OfferingCount.query.filter(OfferingCount.attachment_no != None,
                                   OfferingCount.attachment_no != '').all()}

    # 本月本單位奉獻點算記錄
    unit_offerings = []
    if unit_id:
        unit_offerings = OfferingCount.query.filter(
            OfferingCount.unit_id == unit_id,
            extract('year',  OfferingCount.date) == year,
            extract('month', OfferingCount.date) == month,
        ).order_by(OfferingCount.date, OfferingCount.id).all()

    # 單位最新帳面結餘（不限月份，與現金簿餘額欄完全一致）
    unit_closing_bal = Decimal('0')
    if unit_id:
        latest_tx = Transaction.query.filter_by(unit_id=unit_id)\
            .order_by(Transaction.date.desc(), Transaction.id.desc()).first()
        unit_closing_bal = latest_tx.balance if latest_tx else Decimal('0')

    # 年初至本月累計收支（用於累計列）
    ytd_in = ytd_out = Decimal('0')
    prev_ytd_in = prev_ytd_out = Decimal('0')
    if unit_id:
        ytd_stats = db.session.query(
            func.sum(Transaction.amount_in),
            func.sum(Transaction.amount_out)
        ).filter(
            Transaction.unit_id == unit_id,
            extract('year',  Transaction.date) == year,
            extract('month', Transaction.date) <= month,
        ).first()
        ytd_in  = ytd_stats[0] or Decimal('0')
        ytd_out = ytd_stats[1] or Decimal('0')
        if month > 1:
            prev_ytd_stats = db.session.query(
                func.sum(Transaction.amount_in),
                func.sum(Transaction.amount_out)
            ).filter(
                Transaction.unit_id == unit_id,
                extract('year',  Transaction.date) == year,
                extract('month', Transaction.date) <= month - 1,
            ).first()
            prev_ytd_in  = prev_ytd_stats[0] or Decimal('0')
            prev_ytd_out = prev_ytd_stats[1] or Decimal('0')

    # 銀行帳目摘要（當選定單位時）
    bank_summary = []
    recon = None
    if unit_id:
        for ba in BankAccount.query.filter_by(is_active=True, unit_id=unit_id).all():
            # 只顯示屬於此單位的銀行帳戶
            ba_txs = BankTransaction.query.filter_by(bank_account_id=ba.id).filter(
                extract('year',  BankTransaction.date) == year,
                extract('month', BankTransaction.date) == month,
            ).order_by(BankTransaction.date, BankTransaction.id).all()
            deposit    = sum(t.amount for t in ba_txs if t.tx_type == '存入') or Decimal('0')
            withdrawal = sum(t.amount for t in ba_txs if t.tx_type == '提領') or Decimal('0')
            bank_summary.append({
                'ba': ba,
                'deposit': deposit,
                'withdrawal': withdrawal,
                'txs': ba_txs,
            })
        recon = MonthlyReconciliation.query.filter_by(
            year=year, month=month, unit_id=unit_id).first()

    return render_template('cashbook.html',
        txs=txs, year=year, month=month,
        monthly_in=monthly_in, monthly_out=monthly_out,
        prev_balance=prev_balance,
        accounts=accounts, units=units, bank_accounts=bank_accounts,
        unit_id=unit_id,
        years=YEAR_RANGE, months=list(range(1, 13)),
        offering_map=offering_map,
        unit_offerings=unit_offerings,
        bank_summary=bank_summary, recon=recon,
        unit_closing_bal=unit_closing_bal,
        ytd_in=ytd_in, ytd_out=ytd_out,
        prev_ytd_in=prev_ytd_in, prev_ytd_out=prev_ytd_out,
    )


@app.route('/cashbook/add', methods=['GET', 'POST'])
def cashbook_add():
    """現金簿不允許直接新增，必須透過傳票"""
    flash('請透過「收入傳票」或「支出傳票」新增記錄', 'warning')
    uid = request.args.get('unit_id', 0, type=int) or request.form.get('unit_id', 0)
    return redirect(url_for('cashbook', unit_id=uid))


@app.route('/cashbook/edit/<int:tx_id>', methods=['GET', 'POST'])
def cashbook_edit(tx_id):
    tx = Transaction.query.get_or_404(tx_id)
    accounts = Account.query.order_by(Account.type, Account.sort_order).all()
    units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    bank_accounts = BankAccount.query.filter_by(is_active=True).all()

    if request.method == 'POST':
        try:
            tx.date        = datetime.strptime(request.form['date'], '%Y-%m-%d').date()
            tx.account_id  = int(request.form['account_id'])
            tx.description = request.form['description'].strip()
            tx.amount_in   = Decimal(request.form.get('amount_in')  or '0')
            tx.amount_out  = Decimal(request.form.get('amount_out') or '0')
            tx.unit_id     = int(request.form['unit_id']) if request.form.get('unit_id') else None
            tx.bank_account_id = int(request.form['bank_account_id']) if request.form.get('bank_account_id') else None
            tx.voucher_no  = request.form.get('voucher_no', '').strip()
            tx.notes       = request.form.get('notes', '').strip()
            # 重算「該單位」餘額
            prev_bal = unit_prev_balance(tx.unit_id, tx.date, exclude_id=tx_id)
            tx.balance = prev_bal + tx.amount_in - tx.amount_out
            db.session.commit()
            flash('已更新記錄', 'success')
            return redirect(url_for('cashbook', year=tx.date.year, month=tx.date.month, unit_id=tx.unit_id or 0))
        except Exception as e:
            db.session.rollback()
            flash(f'更新失敗：{e}', 'danger')

    # 查詢是否已有對應奉獻點算表
    linked_oc = OfferingCount.query.filter_by(attachment_no=tx.voucher_no).first() \
        if tx.voucher_no else None
    return render_template('cashbook_edit.html',
        tx=tx, accounts=accounts, units=units, bank_accounts=bank_accounts,
        linked_oc=linked_oc)


@app.route('/cashbook/delete/<int:tx_id>', methods=['POST'])
def cashbook_delete(tx_id):
    tx = Transaction.query.get_or_404(tx_id)
    year, month, uid = tx.date.year, tx.date.month, tx.unit_id or 0
    db.session.delete(tx)
    db.session.commit()
    flash('已刪除記錄', 'warning')
    return redirect(url_for('cashbook', year=year, month=month, unit_id=uid))


@app.route('/voucher/income', methods=['GET', 'POST'])
def voucher_income():
    units    = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    accounts = Account.query.filter_by(type='income').order_by(Account.sort_order).all()
    bank_accounts = BankAccount.query.filter_by(is_active=True).all()
    today_str = date.today().strftime('%Y-%m-%d')

    if request.method == 'POST':
        try:
            tx_date    = datetime.strptime(request.form['date'], '%Y-%m-%d').date()
            unit_id    = int(request.form['unit_id'])
            account_id = int(request.form['account_id'])
            amount_in  = Decimal(request.form.get('amount') or '0')
            description = request.form['description'].strip()
            bank_account_id = request.form.get('bank_account_id') or None
            notes = request.form.get('notes', '').strip()
            voucher_no = request.form.get('voucher_no') or next_voucher_no('income', tx_date.year, unit_id)

            prev_bal = unit_prev_balance(unit_id, tx_date)
            balance  = prev_bal + amount_in

            tx = Transaction(
                date=tx_date, voucher_no=voucher_no,
                voucher_type='income',
                account_id=account_id, unit_id=unit_id,
                bank_account_id=int(bank_account_id) if bank_account_id else None,
                description=description,
                amount_in=amount_in, amount_out=Decimal('0'), balance=balance,
                notes=notes,
            )
            db.session.add(tx)
            db.session.commit()
            flash(f'收入傳票 {voucher_no} 已儲存，已加入「{tx.unit.name}」現金簿', 'success')
            if request.form.get('_continue'):
                flash(f'收入傳票 {voucher_no} 已儲存，繼續新增', 'success')
                return redirect(url_for('voucher_income', unit_id=unit_id))
            # 儲存並回現金簿：先顯示奉獻點算表詢問畫面
            return redirect(url_for('voucher_income',
                saved='1',
                last_voucher=voucher_no,
                last_unit=unit_id,
                last_year=tx_date.year,
                last_month=tx_date.month,
                last_desc=description,
                last_amount=str(amount_in),
                last_date=tx_date.strftime('%Y-%m-%d'),
                last_account=account_id,
                last_tx_id=tx.id,
            ))
        except Exception as e:
            db.session.rollback()
            flash(f'儲存失敗：{e}', 'danger')

    preset_unit = request.args.get('unit_id', 0, type=int)
    unit_balances = {}
    unit_suggested = {}
    for u in units:
        last = Transaction.query.filter_by(unit_id=u.id)\
            .order_by(Transaction.date.desc(), Transaction.id.desc()).first()
        unit_balances[u.id] = float(last.balance) if last else 0
        unit_suggested[u.id] = next_voucher_no('income', date.today().year, u.id)
    suggested_no = unit_suggested.get(preset_unit) or next_voucher_no('income', date.today().year)
    # 儲存後詢問是否新增奉獻點算表
    saved_ctx = {
        'saved':        request.args.get('saved') == '1',
        'last_voucher': request.args.get('last_voucher', ''),
        'last_unit':    request.args.get('last_unit', 0, type=int),
        'last_year':    request.args.get('last_year',  date.today().year,  type=int),
        'last_month':   request.args.get('last_month', date.today().month, type=int),
        'last_desc':    request.args.get('last_desc', ''),
        'last_amount':  request.args.get('last_amount', '0'),
        'last_date':    request.args.get('last_date', ''),
        'last_account': request.args.get('last_account', ''),
        'last_tx_id':   request.args.get('last_tx_id', 0, type=int),
        'continue_after': False,
    }
    return render_template('voucher_income.html',
        units=units, accounts=accounts, bank_accounts=bank_accounts,
        today=today_str, suggested_no=suggested_no,
        preset_unit=preset_unit, unit_balances=unit_balances,
        unit_suggested=unit_suggested, **saved_ctx,
    )


@app.route('/voucher/expense', methods=['GET', 'POST'])
def voucher_expense():
    units    = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    accounts = Account.query.filter_by(type='expense').order_by(Account.sort_order).all()
    bank_accounts = BankAccount.query.filter_by(is_active=True).all()
    today_str = date.today().strftime('%Y-%m-%d')

    if request.method == 'POST':
        try:
            tx_date    = datetime.strptime(request.form['date'], '%Y-%m-%d').date()
            unit_id    = int(request.form['unit_id'])
            account_id = int(request.form['account_id'])
            amount_out = Decimal(request.form.get('amount') or '0')
            description = request.form['description'].strip()
            bank_account_id = request.form.get('bank_account_id') or None
            notes = request.form.get('notes', '').strip()
            voucher_no = request.form.get('voucher_no') or next_voucher_no('expense', tx_date.year, unit_id)

            prev_bal = unit_prev_balance(unit_id, tx_date)
            balance  = prev_bal - amount_out

            tx = Transaction(
                date=tx_date, voucher_no=voucher_no,
                voucher_type='expense',
                account_id=account_id, unit_id=unit_id,
                bank_account_id=int(bank_account_id) if bank_account_id else None,
                description=description,
                amount_in=Decimal('0'), amount_out=amount_out, balance=balance,
                notes=notes,
            )
            db.session.add(tx)
            db.session.commit()
            flash(f'支出傳票 {voucher_no} 已儲存，已加入「{tx.unit.name}」現金簿', 'success')
            if request.form.get('_continue'):
                return redirect(url_for('voucher_expense'))
            return redirect(url_for('cashbook', year=tx_date.year, month=tx_date.month, unit_id=unit_id))
        except Exception as e:
            db.session.rollback()
            flash(f'儲存失敗：{e}', 'danger')

    preset_unit = request.args.get('unit_id', 0, type=int)
    unit_balances = {}
    unit_suggested = {}
    for u in units:
        last = Transaction.query.filter_by(unit_id=u.id)\
            .order_by(Transaction.date.desc(), Transaction.id.desc()).first()
        unit_balances[u.id] = float(last.balance) if last else 0
        unit_suggested[u.id] = next_voucher_no('expense', date.today().year, u.id)
    suggested_no = unit_suggested.get(preset_unit) or next_voucher_no('expense', date.today().year)
    return render_template('voucher_expense.html',
        units=units, accounts=accounts, bank_accounts=bank_accounts,
        today=today_str, suggested_no=suggested_no,
        preset_unit=preset_unit, unit_balances=unit_balances,
        unit_suggested=unit_suggested,
    )


# ── 奉獻點算表 ───────────────────────────────────────────────────────────────
@app.route('/offering')
def offering_list():
    year = request.args.get('year', date.today().year, type=int)
    unit_id = request.args.get('unit_id', 0, type=int)  # 0 = 全部單位

    units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()

    q = OfferingCount.query.filter(extract('year', OfferingCount.date) == year)
    if unit_id:
        q = q.filter(OfferingCount.unit_id == unit_id)

    offerings = q.order_by(OfferingCount.date.desc()).all()
    years = YEAR_RANGE

    # 統計數據
    total_offerings = len(offerings)
    total_amount = sum(o.total_cash for o in offerings) if offerings else Decimal('0')

    # 按單位分組統計
    unit_stats = {}
    if not unit_id:  # 只在顯示全部單位時計算
        for offering in offerings:
            uid = offering.unit_id
            if uid not in unit_stats:
                unit_stats[uid] = {'count': 0, 'amount': Decimal('0'), 'unit': None}
            unit_stats[uid]['count'] += 1
            unit_stats[uid]['amount'] += offering.total_cash

        # 添加單位名稱
        for uid in unit_stats:
            unit = next((u for u in units if u.id == uid), None)
            unit_stats[uid]['unit'] = unit

    return render_template('offering_list.html',
        offerings=offerings, year=year, years=years,
        unit_id=unit_id, units=units,
        total_offerings=total_offerings, total_amount=total_amount,
        unit_stats=unit_stats,
    )


@app.route('/offering/new', methods=['GET', 'POST'])
def offering_new():
    if request.method == 'POST':
        try:
            tx_date    = datetime.strptime(request.form['date'], '%Y-%m-%d').date()
            meeting    = request.form['meeting_type']
            # 從聚會名稱查詢對應單位
            unit_obj   = Unit.query.filter_by(name=meeting, is_active=True).first()
            unit_id_f  = unit_obj.id if unit_obj else None
            account_id = int(request.form['account_id'])
            total_cash  = Decimal(request.form.get('total_cash') or '0')
            voucher_no  = request.form.get('attachment_no', '').strip()
            description = request.form.get('description', '').strip() or f'{meeting} 奉獻點算'

            # 若未填傳票號，自動產生（含單位碼）
            if not voucher_no:
                voucher_no = next_voucher_no('income', tx_date.year, unit_id_f)

            # 建立收入傳票（若該傳票號尚未存在）
            existing_tx = Transaction.query.filter_by(
                voucher_no=voucher_no, voucher_type='income').first()
            if not existing_tx:
                # 先取前一筆餘額，設定初始 balance
                prev_bal = unit_prev_balance(unit_id_f, tx_date) if unit_id_f else Decimal('0')
                tx = Transaction(
                    date=tx_date, voucher_no=voucher_no,
                    voucher_type='income',
                    account_id=account_id, unit_id=unit_id_f,
                    description=description,
                    amount_in=total_cash, amount_out=Decimal('0'),
                    balance=prev_bal + total_cash,
                )
                db.session.add(tx)

            oc = OfferingCount(
                date=tx_date,
                meeting_type=meeting,
                unit_id=unit_id_f,
                description=description,
                counter1=request.form.get('counter1', '').strip(),
                counter2=request.form.get('counter2', '').strip(),
                notes=request.form.get('notes', '').strip(),
                attachment_no=voucher_no,
                total_cash=total_cash,
                account_id=account_id,
            )
            db.session.add(oc)
            db.session.commit()
            # 重新計算餘額（處理插入中間日期的情形）
            if unit_id_f:
                recalc_unit_balances(unit_id_f)
            flash('奉獻點算表已儲存', 'success')
            return redirect(url_for('offering_list'))
        except Exception as e:
            db.session.rollback()
            flash(f'儲存失敗：{e}', 'danger')

    units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    income_accounts = Account.query.filter_by(type='income').order_by(Account.sort_order).all()
    preset_unit = request.args.get('unit', 0, type=int)
    suggested_no = next_voucher_no('income', date.today().year)
    return render_template('offering_new.html',
        units=units, income_accounts=income_accounts,
        suggested_no=suggested_no, preset_unit=preset_unit,
        today=date.today().strftime('%Y-%m-%d'))


@app.route('/offering/<int:oc_id>')
def offering_detail(oc_id):
    oc = OfferingCount.query.get_or_404(oc_id)
    return render_template('offering_detail.html', oc=oc, denominations=DENOMINATIONS)


@app.route('/offering/delete/<int:oc_id>', methods=['POST'])
def offering_delete(oc_id):
    oc = OfferingCount.query.get_or_404(oc_id)
    year = oc.date.year
    # 同步刪除對應收入傳票
    if oc.attachment_no:
        linked = Transaction.query.filter_by(
            voucher_no=oc.attachment_no, voucher_type='income').first()
        if linked:
            db.session.delete(linked)
    db.session.delete(oc)
    db.session.commit()
    flash('已刪除奉獻點算表及對應收入傳票', 'warning')
    return redirect(url_for('offering_list', year=year))


@app.route('/offering/edit/<int:oc_id>', methods=['GET', 'POST'])
def offering_edit(oc_id):
    oc = OfferingCount.query.get_or_404(oc_id)
    units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    income_accounts = Account.query.filter_by(type='income').order_by(Account.sort_order).all()

    if request.method == 'POST':
        try:
            tx_date    = datetime.strptime(request.form['date'], '%Y-%m-%d').date()
            meeting    = request.form['meeting_type']
            account_id = int(request.form['account_id'])
            total_cash = Decimal(request.form.get('total_cash') or '0')
            unit_obj   = Unit.query.filter_by(name=meeting, is_active=True).first()
            unit_id_f  = unit_obj.id if unit_obj else oc.unit_id

            description = request.form.get('description', '').strip() or f'{meeting} 奉獻點算'

            # 更新奉獻點算表
            oc.date         = tx_date
            oc.meeting_type = meeting
            oc.unit_id      = unit_id_f
            oc.account_id   = account_id
            oc.total_cash   = total_cash
            oc.description  = description
            oc.counter1     = request.form.get('counter1', '').strip()
            oc.counter2     = request.form.get('counter2', '').strip()
            oc.notes        = request.form.get('notes', '').strip()

            # 同步更新對應收入傳票
            if oc.attachment_no:
                linked_tx = Transaction.query.filter_by(
                    voucher_no=oc.attachment_no, voucher_type='income').first()
                if linked_tx:
                    linked_tx.date        = tx_date
                    linked_tx.account_id  = account_id
                    linked_tx.unit_id     = unit_id_f
                    linked_tx.amount_in   = total_cash
                    linked_tx.description = description
                    # 重新計算餘額
                    prev_bal = unit_prev_balance(unit_id_f, tx_date, exclude_id=linked_tx.id)
                    linked_tx.balance = prev_bal + total_cash

            db.session.commit()
            flash('奉獻點算表及對應收入傳票已更新', 'success')
            return redirect(url_for('offering_detail', oc_id=oc.id))
        except Exception as e:
            db.session.rollback()
            flash(f'更新失敗：{e}', 'danger')

    return render_template('offering_edit.html',
        oc=oc, units=units, income_accounts=income_accounts)


# ── 月報表 ───────────────────────────────────────────────────────────────────
@app.route('/reports')
def reports():
    year    = request.args.get('year',    date.today().year,  type=int)
    month   = request.args.get('month',   date.today().month, type=int)
    unit_id = request.args.get('unit_id', 0, type=int)
    years   = YEAR_RANGE
    units   = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    cur_unit = next((u for u in units if u.id == unit_id), None)

    income_accounts  = Account.query.filter_by(type='income').order_by(Account.sort_order).all()
    expense_accounts = Account.query.filter_by(type='expense').order_by(Account.sort_order).all()

    def get_monthly(account_id, yr, mo):
        q = db.session.query(func.coalesce(func.sum(Transaction.amount_in - Transaction.amount_out), 0)).filter(
            Transaction.account_id == account_id,
            extract('year',  Transaction.date) == yr,
            extract('month', Transaction.date) == mo,
        )
        if unit_id:
            q = q.filter(Transaction.unit_id == unit_id)
        return q.scalar() or Decimal('0')

    def get_ytd(account_id, yr, mo):
        q = db.session.query(func.coalesce(func.sum(Transaction.amount_in - Transaction.amount_out), 0)).filter(
            Transaction.account_id == account_id,
            extract('year',  Transaction.date) == yr,
            extract('month', Transaction.date) <= mo,
        )
        if unit_id:
            q = q.filter(Transaction.unit_id == unit_id)
        return q.scalar() or Decimal('0')

    income_rows, expense_rows = [], []
    for a in income_accounts:
        mo_val = abs(get_monthly(a.id, year, month))
        if mo_val:
            income_rows.append({'account': a, 'monthly': mo_val})

    for a in expense_accounts:
        mo_val = abs(get_monthly(a.id, year, month))
        if mo_val:
            expense_rows.append({'account': a, 'monthly': mo_val})

    total_income_monthly  = sum(r['monthly'] for r in income_rows)
    total_expense_monthly = sum(r['monthly'] for r in expense_rows)

    # 上月結存改用彙總計算，避免單筆 balance 與補登後不同步
    prev_balance = tx_cumulative_balance(
        unit_id if unit_id else None,
        year if month > 1 else year - 1,
        month - 1 if month > 1 else 12,
    )
    closing_balance = prev_balance + total_income_monthly - total_expense_monthly

    # 銀行帳戶總額（該單位）
    if unit_id:
        bank_accounts_list = BankAccount.query.filter_by(unit_id=unit_id, is_active=True).all()
    else:
        bank_accounts_list = BankAccount.query.filter_by(is_active=True).all()
    bank_total = sum(b.current_balance for b in bank_accounts_list) or Decimal('0')

    # 本月核對記錄
    recon = MonthlyReconciliation.query.filter_by(
        year=year, month=month, unit_id=unit_id or None).first()

    return render_template('reports.html',
        year=year, month=month, years=years, months=list(range(1, 13)),
        units=units, unit_id=unit_id, cur_unit=cur_unit,
        income_rows=income_rows, expense_rows=expense_rows,
        total_income_monthly=total_income_monthly,
        total_expense_monthly=total_expense_monthly,
        net_monthly=total_income_monthly - total_expense_monthly,
        prev_balance=prev_balance,
        closing_balance=closing_balance,
        bank_total=bank_total,
        bank_accounts_list=bank_accounts_list,
        recon=recon,
    )


@app.route('/unit/<int:unit_id>/report')
def unit_report(unit_id):
    """單位專屬月報表"""
    unit = Unit.query.get_or_404(unit_id)
    year = request.args.get('year', date.today().year, type=int)
    month = request.args.get('month', date.today().month, type=int)

    # 重用reports函數的邏輯，但只針對特定單位
    income_accounts = Account.query.filter_by(type='income').order_by(Account.sort_order).all()
    expense_accounts = Account.query.filter_by(type='expense').order_by(Account.sort_order).all()

    def get_monthly(account_id, yr, mo):
        q = db.session.query(func.coalesce(func.sum(Transaction.amount_in - Transaction.amount_out), 0)).filter(
            Transaction.account_id == account_id,
            extract('year', Transaction.date) == yr,
            extract('month', Transaction.date) == mo,
            Transaction.unit_id == unit_id
        )
        return q.scalar() or Decimal('0')

    income_rows = []
    expense_rows = []
    for a in income_accounts:
        mo_val = abs(get_monthly(a.id, year, month))
        if mo_val:
            income_rows.append({'account': a, 'monthly': mo_val})

    for a in expense_accounts:
        mo_val = abs(get_monthly(a.id, year, month))
        if mo_val:
            expense_rows.append({'account': a, 'monthly': mo_val})

    total_income_monthly = sum(r['monthly'] for r in income_rows)
    total_expense_monthly = sum(r['monthly'] for r in expense_rows)

    # 上月結存改用彙總計算，確保單位月報與交易資料同步
    prev_balance = tx_cumulative_balance(
        unit_id,
        year if month > 1 else year - 1,
        month - 1 if month > 1 else 12,
    )
    closing_balance = prev_balance + total_income_monthly - total_expense_monthly

    # 銀行帳戶
    bank_accounts_list = BankAccount.query.filter_by(unit_id=unit_id, is_active=True).all()
    bank_total = sum(b.current_balance for b in bank_accounts_list) or Decimal('0')

    return render_template('unit_report.html',
        unit=unit, year=year, month=month,
        income_rows=income_rows, expense_rows=expense_rows,
        total_income_monthly=total_income_monthly,
        total_expense_monthly=total_expense_monthly,
        net_monthly=total_income_monthly - total_expense_monthly,
        prev_balance=prev_balance,
        closing_balance=closing_balance,
        bank_total=bank_total,
        bank_accounts_list=bank_accounts_list,
        years=YEAR_RANGE, months=list(range(1, 13)),
    )


@app.route('/reports/reconcile', methods=['POST'])
def reports_reconcile():
    year    = int(request.form['year'])
    month   = int(request.form['month'])
    unit_id = request.form.get('unit_id') or None
    uid     = int(unit_id) if unit_id else None

    recon = MonthlyReconciliation.query.filter_by(
        year=year, month=month, unit_id=uid).first()
    if not recon:
        recon = MonthlyReconciliation(year=year, month=month, unit_id=uid)
        db.session.add(recon)

    recon.bank_total = Decimal(request.form.get('bank_total') or '0')
    recon.cash_amt   = Decimal(request.form.get('cash_amt')   or '0')
    recon.reason     = request.form.get('reason', '').strip()
    recon.confirmed_at = datetime.now()
    db.session.commit()
    flash('月底核對已儲存', 'success')
    return redirect(url_for('reports', year=year, month=month, unit_id=uid or 0))


# ── 出納交接 ─────────────────────────────────────────────────────────────────
@app.route('/handover')
def handover_list():
    handovers = CashierHandover.query.order_by(CashierHandover.handover_date.desc()).all()
    return render_template('handover_list.html', handovers=handovers)


@app.route('/handover/new', methods=['GET', 'POST'])
def handover_new():
    if request.method == 'POST':
        try:
            cash = Decimal(request.form.get('cash_amount') or '0')
            bank = Decimal(request.form.get('bank_amount') or '0')
            h = CashierHandover(
                handover_date=datetime.strptime(request.form['handover_date'], '%Y-%m-%d').date(),
                cash_amount=cash, bank_amount=bank, total_amount=cash + bank,
                old_cashier=request.form.get('old_cashier', '').strip(),
                new_cashier=request.form.get('new_cashier', '').strip(),
                accountant=request.form.get('accountant', '').strip(),
                advisor=request.form.get('advisor', '').strip(),
                audit_result=request.form.get('audit_result', '').strip(),
                bank_details=request.form.get('bank_details', '').strip(),
                notes=request.form.get('notes', '').strip(),
            )
            db.session.add(h)
            db.session.commit()
            flash('出納交接記錄已儲存', 'success')
            return redirect(url_for('handover_list'))
        except Exception as e:
            db.session.rollback()
            flash(f'儲存失敗：{e}', 'danger')
    bank_accounts = BankAccount.query.filter_by(is_active=True).all()
    return render_template('handover_new.html',
        today=date.today().strftime('%Y-%m-%d'), bank_accounts=bank_accounts)


@app.route('/handover/<int:h_id>')
def handover_detail(h_id):
    h = CashierHandover.query.get_or_404(h_id)
    return render_template('handover_detail.html', h=h)


# ── 查帳紀錄 ─────────────────────────────────────────────────────────────────
@app.route('/audit')
def audit_list():
    audits = AuditRecord.query.order_by(AuditRecord.audit_date.desc()).all()
    return render_template('audit_list.html', audits=audits)


@app.route('/audit/new', methods=['GET', 'POST'])
def audit_new():
    if request.method == 'POST':
        try:
            ar = AuditRecord(
                audit_date=datetime.strptime(request.form['audit_date'], '%Y-%m-%d').date(),
                period_start=datetime.strptime(request.form['period_start'], '%Y-%m-%d').date(),
                period_end=datetime.strptime(request.form['period_end'], '%Y-%m-%d').date(),
                auditor=request.form.get('auditor', '').strip(),
                audit_type=request.form.get('audit_type', '月查'),
                check_cash_match='check_cash_match' in request.form,
                check_bank_match='check_bank_match' in request.form,
                check_receipts='check_receipts' in request.form,
                check_vouchers='check_vouchers' in request.form,
                check_continuity='check_continuity' in request.form,
                check_dedicated_funds='check_dedicated_funds' in request.form,
                check_salary='check_salary' in request.form,
                check_seal='check_seal' in request.form,
                findings=request.form.get('findings', '').strip(),
                status=request.form.get('status', '待辦'),
                notes=request.form.get('notes', '').strip(),
            )
            db.session.add(ar)
            db.session.commit()
            flash('查帳紀錄已儲存', 'success')
            return redirect(url_for('audit_list'))
        except Exception as e:
            db.session.rollback()
            flash(f'儲存失敗：{e}', 'danger')
    return render_template('audit_new.html', today=date.today().strftime('%Y-%m-%d'))


@app.route('/audit/<int:ar_id>')
def audit_detail(ar_id):
    ar = AuditRecord.query.get_or_404(ar_id)
    return render_template('audit_detail.html', ar=ar)


@app.route('/audit/update_status/<int:ar_id>', methods=['POST'])
def audit_update_status(ar_id):
    ar = AuditRecord.query.get_or_404(ar_id)
    ar.status = request.form.get('status', ar.status)
    db.session.commit()
    flash('狀態已更新', 'success')
    return redirect(url_for('audit_detail', ar_id=ar_id))


# ── 設定：會計科目 ────────────────────────────────────────────────────────────
@app.route('/settings/accounts')
def settings_accounts():
    income_accounts  = Account.query.filter_by(type='income').order_by(Account.sort_order).all()
    expense_accounts = Account.query.filter_by(type='expense').order_by(Account.sort_order).all()
    add_type = request.args.get('a_type', 'income')
    if add_type not in ('income', 'expense'):
        add_type = 'income'
    return render_template('settings_accounts.html',
        income_accounts=income_accounts,
        expense_accounts=expense_accounts,
        add_type=add_type)


@app.route('/settings/accounts/add', methods=['POST'])
def settings_accounts_add():
    try:
        a_type = request.form.get('type', 'income')
        if a_type not in ('income', 'expense'):
            a_type = 'income'
        prefix = 'I' if a_type == 'income' else 'E'
        # 自動產生排序（現有最大值 + 1）
        max_order = db.session.query(func.max(Account.sort_order))\
            .filter_by(type=a_type).scalar() or 0
        new_order = max_order + 1
        # 自動產生代碼（前綴 + 兩位流水號）
        new_code = f'{prefix}{new_order:02d}'
        # 若代碼已存在則往後找空位
        existing_codes = {ac.code for ac in Account.query.filter_by(type=a_type).all()}
        while new_code in existing_codes:
            new_order += 1
            new_code = f'{prefix}{new_order:02d}'
        a = Account(
            code=new_code,
            name=request.form['name'].strip(),
            description=request.form.get('description', '').strip() or None,
            type=a_type,
            sort_order=new_order,
        )
        db.session.add(a)
        db.session.commit()
        flash(f'科目已新增，代碼 {new_code}，排序 {new_order}', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'新增失敗：{e}', 'danger')
        return redirect(url_for('settings_accounts', add='1', a_type=a_type))

    if request.form.get('next') == '1':
        return redirect(url_for('settings_accounts', add='1', a_type=a_type))
    return redirect(url_for('settings_accounts'))


@app.route('/settings/accounts/toggle/<int:a_id>', methods=['POST'])
def settings_accounts_toggle(a_id):
    a = Account.query.get_or_404(a_id)
    a.is_active = not a.is_active
    db.session.commit()
    flash('科目狀態已更新', 'success')
    return redirect(url_for('settings_accounts'))


@app.route('/settings/accounts/edit/<int:a_id>', methods=['POST'])
def settings_accounts_edit(a_id):
    a = Account.query.get_or_404(a_id)
    try:
        a.code        = request.form['code'].strip()
        a.name        = request.form['name'].strip()
        a.description = request.form.get('description', '').strip() or None
        a.type        = request.form['type']
        a.sort_order  = int(request.form.get('sort_order', a.sort_order))
        db.session.commit()
        flash(f'科目「{a.name}」已更新', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'新增失敗：{e}', 'danger')
    if request.form.get('next') == '1':
        return redirect(url_for('settings_accounts', add='1'))
    return redirect(url_for('settings_accounts'))




# ── 設定：銀行帳戶 ────────────────────────────────────────────────────────────
@app.route('/settings/banks')
def settings_banks():
    banks = BankAccount.query.filter_by(is_active=True).all()
    units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()

    # 各單位現金簿帳面結餘
    unit_balance = {}
    for u in units:
        last = Transaction.query.filter_by(unit_id=u.id)\
            .order_by(Transaction.date.desc(), Transaction.id.desc()).first()
        unit_balance[u.id] = last.balance if last else Decimal('0')

    # 各單位最新手上現金（MonthlyReconciliation 最近一筆）
    unit_cash = {}
    for u in units:
        recon = MonthlyReconciliation.query.filter_by(unit_id=u.id)\
            .order_by(MonthlyReconciliation.year.desc(),
                      MonthlyReconciliation.month.desc()).first()
        unit_cash[u.id] = recon.cash_amt if recon else Decimal('0')

    total_cashbook = sum(unit_balance.values()) or Decimal('0')
    total_bank     = sum(b.current_balance for b in banks) or Decimal('0')
    total_cash     = sum(unit_cash.values()) or Decimal('0')

    banks = BankAccount.query.filter_by(is_active=True)\
        .outerjoin(Unit, BankAccount.unit_id == Unit.id)\
        .order_by(Unit.sort_order.nullslast(), Unit.id.nullslast(), BankAccount.id).all()
    return render_template('settings_banks.html',
        banks=banks, units=units,
        unit_balance=unit_balance, unit_cash=unit_cash,
        total_cashbook=total_cashbook,
        total_bank=total_bank, total_cash=total_cash,
    )


@app.route('/settings/banks/add', methods=['POST'])
def settings_banks_add():
    try:
        uid = request.form.get('unit_id') or None
        b = BankAccount(
            bank_name=request.form['bank_name'].strip(),
            account_type=request.form.get('account_type', '活期'),
            account_no=request.form['account_no'].strip(),
            account_name=request.form.get('account_name', '').strip(),
            current_balance=Decimal(request.form.get('current_balance') or '0'),
            unit_id=int(uid) if uid else None,
            notes=request.form.get('notes', '').strip(),
        )
        db.session.add(b)
        db.session.commit()
        flash('銀行帳戶已新增', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'新增失敗：{e}', 'danger')
    return redirect(url_for('settings_banks'))


@app.route('/settings/accounts/delete/<int:a_id>', methods=['POST'])
def settings_accounts_delete(a_id):
    a = Account.query.get_or_404(a_id)
    # 檢查實際財務記錄（預算視為計畫資料，可隨科目一起刪除）
    tx_count = Transaction.query.filter_by(account_id=a_id).count()
    oc_count = OfferingCount.query.filter_by(account_id=a_id).count()
    po_count = PersonalOffering.query.filter_by(account_id=a_id).count()
    total = tx_count + oc_count + po_count
    if total > 0:
        parts = []
        if tx_count: parts.append(f'現金簿 {tx_count} 筆')
        if oc_count: parts.append(f'奉獻點算 {oc_count} 筆')
        if po_count: parts.append(f'個人奉獻 {po_count} 筆')
        flash(f'無法刪除「{a.name}」：仍有關聯記錄（{"、".join(parts)}）', 'danger')
    else:
        # 預算資料隨科目一起刪除
        Budget.query.filter_by(account_id=a_id).delete()
        a_type = a.type
        db.session.delete(a)
        db.session.flush()
        remaining = Account.query.filter_by(type=a_type)\
            .order_by(Account.sort_order, Account.id).all()
        prefix = 'I' if a_type == 'income' else 'E'
        import re
        for idx, acc in enumerate(remaining, start=1):
            acc.sort_order = idx
            if re.fullmatch(rf'{prefix}\d+', acc.code):
                acc.code = f'{prefix}{idx:02d}'
        db.session.commit()
        flash(f'科目「{a.name}」已刪除，排序已重新整理', 'warning')
    return redirect(url_for('settings_accounts'))


@app.route('/settings/banks/update_balance/<int:b_id>', methods=['POST'])
def settings_banks_update_balance(b_id):
    b = BankAccount.query.get_or_404(b_id)
    b.current_balance = Decimal(request.form.get('current_balance') or '0')
    db.session.commit()
    flash('餘額已更新', 'success')
    return redirect(url_for('settings_banks'))


@app.route('/settings/banks/delete/<int:b_id>', methods=['POST'])
def settings_banks_delete(b_id):
    b = BankAccount.query.get_or_404(b_id)
    b.is_active = False
    db.session.commit()
    flash('帳戶已停用', 'warning')
    return redirect(url_for('settings_banks'))


# ── 設定：各單位 ──────────────────────────────────────────────────────────────
@app.route('/settings/units')
def settings_units():
    units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    unit_balance = {}
    for u in units:
        last = Transaction.query.filter_by(unit_id=u.id)\
            .order_by(Transaction.date.desc(), Transaction.id.desc()).first()
        unit_balance[u.id] = last.balance if last else Decimal('0')
    return render_template('settings_units.html', units=units, unit_balance=unit_balance)


@app.route('/settings/units/add', methods=['POST'])
def settings_units_add():
    try:
        u = Unit(
            name=request.form['name'].strip(),
            code=request.form.get('code', '').strip().upper() or None,
            type=request.form.get('type', '團契'),
            treasurer=request.form.get('treasurer', '').strip(),
            notes=request.form.get('notes', '').strip(),
        )
        db.session.add(u)
        db.session.commit()
        flash('單位已新增', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'新增失敗：{e}', 'danger')
    return redirect(url_for('settings_units'))


@app.route('/settings/units/edit/<int:u_id>', methods=['POST'])
def settings_units_edit(u_id):
    u = Unit.query.get_or_404(u_id)
    try:
        old_code = u.code or ''
        new_code = request.form.get('code', '').strip().upper() or None
        u.name      = request.form['name'].strip()
        u.code      = new_code
        u.type      = request.form.get('type', u.type)
        u.treasurer = request.form.get('treasurer', '').strip()
        u.notes     = request.form.get('notes', '').strip()
        # 簡碼變更時同步更新該單位所有傳票號
        if new_code and old_code and new_code != old_code:
            txs = Transaction.query.filter_by(unit_id=u_id).all()
            for tx in txs:
                if tx.voucher_no:
                    # 格式：收-{code}-{year}-{seq} 或 支-{code}-{year}-{seq}
                    tx.voucher_no = tx.voucher_no.replace(
                        f'-{old_code}-', f'-{new_code}-', 1
                    )
            updated = len(txs)
            flash(f'單位已更新，{updated} 筆傳票號簡碼同步為 {new_code}', 'success')
        else:
            flash('單位已更新', 'success')
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        flash(f'更新失敗：{e}', 'danger')
    return redirect(url_for('settings_units'))


@app.route('/settings/units/move/<int:u_id>/<direction>', methods=['POST'])
def settings_units_move(u_id, direction):
    units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    idx = next((i for i, u in enumerate(units) if u.id == u_id), None)
    if idx is None:
        return redirect(url_for('settings_units'))
    if direction == 'up' and idx > 0:
        swap = units[idx - 1]
    elif direction == 'down' and idx < len(units) - 1:
        swap = units[idx + 1]
    else:
        return redirect(url_for('settings_units'))
    u = units[idx]
    # 交換 sort_order
    u.sort_order, swap.sort_order = swap.sort_order, u.sort_order
    # 若兩者 sort_order 相同則用 id 區分
    if u.sort_order == swap.sort_order:
        u.sort_order = swap.sort_order - 1 if direction == 'up' else swap.sort_order + 1
    db.session.commit()
    return redirect(url_for('settings_units'))


@app.route('/settings/units/delete/<int:u_id>', methods=['POST'])
def settings_units_delete(u_id):
    u = Unit.query.get_or_404(u_id)
    u.is_active = False
    db.session.commit()
    flash('單位已停用', 'warning')
    return redirect(url_for('settings_units'))


# ── 年度預算 ──────────────────────────────────────────────────────────────────
@app.route('/budget')
def budget():
    year = request.args.get('year', date.today().year, type=int)
    income_accounts = Account.query.filter_by(type='income').order_by(Account.sort_order).all()
    expense_accounts = Account.query.filter_by(type='expense').order_by(Account.sort_order).all()

    budgets = {b.account_id: b.amount for b in Budget.query.filter_by(year=year).all()}
    years = YEAR_RANGE

    return render_template('budget.html',
        year=year, years=years,
        income_accounts=income_accounts,
        expense_accounts=expense_accounts,
        budgets=budgets,
    )


@app.route('/budget/save', methods=['POST'])
def budget_save():
    year = int(request.form['year'])
    try:
        for key, value in request.form.items():
            if key.startswith('budget_'):
                account_id = int(key.replace('budget_', ''))
                amount = Decimal(value or '0')
                b = Budget.query.filter_by(year=year, account_id=account_id).first()
                if b:
                    b.amount = amount
                else:
                    db.session.add(Budget(year=year, account_id=account_id, amount=amount))
        db.session.commit()
        flash(f'{year} 年度預算已儲存', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'儲存失敗：{e}', 'danger')
    return redirect(url_for('budget', year=year))


# ── API：各月收支資料（儀表板圖表用）────────────────────────────────────────
@app.route('/api/monthly_chart/<int:year>')
def api_monthly_chart(year):
    labels = [f'{m}月' for m in range(1, 13)]
    incomes, expenses = [], []
    for m in range(1, 13):
        inc = db.session.query(func.coalesce(func.sum(Transaction.amount_in), 0)).filter(
            extract('year', Transaction.date) == year,
            extract('month', Transaction.date) == m,
        ).scalar() or 0
        exp = db.session.query(func.coalesce(func.sum(Transaction.amount_out), 0)).filter(
            extract('year', Transaction.date) == year,
            extract('month', Transaction.date) == m,
        ).scalar() or 0
        incomes.append(float(inc))
        expenses.append(float(exp))
    return jsonify({'labels': labels, 'incomes': incomes, 'expenses': expenses})


@app.route('/api/accounting-check/prefill')
def api_accounting_check_prefill():
    """查帳表自動帶入：依單位與查帳期間回傳帳冊與銀行資料。"""
    unit_id = request.args.get('unit_id', type=int)
    year = request.args.get('year', type=int)
    audit_type = request.args.get('audit_type', '第一季查帳')

    if not unit_id or not year:
        return jsonify({'error': '缺少 unit_id 或 year'}), 400

    unit = Unit.query.get_or_404(unit_id)
    start_month, end_month, quarter, period_label = accounting_period(audit_type)

    total_income, total_expense = tx_period_totals(unit.id, year, start_month, end_month)

    # 上期結存：Q1/年度總查取前一年度結存；Q2-Q4取同年前一季累計
    if audit_type in ('第一季查帳', '年度總查'):
        prev_period_balance = tx_cumulative_balance(unit.id, year - 1, 12)
    else:
        prev_period_balance = tx_cumulative_balance(unit.id, year, start_month - 1)

    banks = BankAccount.query.filter_by(unit_id=unit.id, is_active=True).all()
    bank_rows = [
        {
            'bank_name': b.bank_name,
            'account_type': b.account_type or '活期',
            'account_name': b.account_name,
            'account_no': b.account_no,
            'balance': float(b.current_balance or 0),
        }
        for b in banks
    ]

    return jsonify({
        'unit_id': unit.id,
        'unit_name': unit.name,
        'audit_type': audit_type,
        'year': year,
        'quarter': quarter,
        'start_month': start_month,
        'end_month': end_month,
        'period': period_label,
        'prev_period_balance': float(prev_period_balance or 0),
        'total_income': float(total_income or 0),
        'total_expense': float(total_expense or 0),
        'banks': bank_rows,
    })


# ── 會友資料 ──────────────────────────────────────────────────────────────────
@app.route('/members')
def member_list():
    q = request.args.get('q', '').strip()
    status = request.args.get('status', '')
    unit_id = request.args.get('unit_id', '', type=str)

    query = Member.query.filter_by(is_active=True)
    if q:
        query = query.filter(Member.name.contains(q))
    if status:
        query = query.filter_by(status=status)
    if unit_id:
        query = query.filter_by(unit_id=int(unit_id))
    members = query.order_by(Member.name).all()

    units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    statuses = ['在籍', '人在籍不在', '人不在籍在', '轉出', '安息']

    # 統計
    total = Member.query.filter_by(is_active=True).count()
    active = Member.query.filter_by(is_active=True, status='在籍').count()

    return render_template('member_list.html',
        members=members, units=units, statuses=statuses,
        q=q, status=status, unit_id=unit_id,
        total=total, active=active,
    )


@app.route('/members/new', methods=['GET', 'POST'])
def member_new():
    units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    if request.method == 'POST':
        try:
            name = request.form['name'].strip()
            # 重複檢查（同名且在籍/未停用）
            existing = Member.query.filter(
                Member.name == name,
                Member.is_active == True
            ).first()
            if existing:
                flash(f'會友「{name}」已存在（請先確認是否重複）', 'danger')
                return render_template('member_new.html', units=units,
                                       today=date.today().strftime('%Y-%m-%d'))

            def parse_date(s):
                return datetime.strptime(s, '%Y-%m-%d').date() if s else None

            m = Member(
                name=name,
                gender=request.form.get('gender', ''),
                birth_date=parse_date(request.form.get('birth_date', '')),
                phone=request.form.get('phone', '').strip(),
                address=request.form.get('address', '').strip(),
                email=request.form.get('email', '').strip(),
                join_date=parse_date(request.form.get('join_date', '')),
                baptism_date=parse_date(request.form.get('baptism_date', '')),
                status=request.form.get('status', '在籍'),
                unit_id=int(request.form['unit_id']) if request.form.get('unit_id') else None,
                notes=request.form.get('notes', '').strip(),
            )
            db.session.add(m)
            db.session.commit()
            flash(f'會友「{m.name}」已新增', 'success')
            if request.form.get('next') == '1':
                return redirect(url_for('member_new'))
            return redirect(url_for('member_detail', m_id=m.id))
        except Exception as e:
            db.session.rollback()
            flash(f'新增失敗：{e}', 'danger')
    return render_template('member_new.html', units=units,
                           today=date.today().strftime('%Y-%m-%d'))


@app.route('/members/<int:m_id>')
def member_detail(m_id):
    m = Member.query.get_or_404(m_id)
    year = request.args.get('year', YEAR_RANGE[0], type=int)

    # 本年度個人奉獻記錄
    offerings = PersonalOffering.query.filter_by(
        member_id=m_id, year=year
    ).order_by(PersonalOffering.date).all()

    # 按月彙總
    monthly = {mo: Decimal('0') for mo in range(1, 13)}
    for o in offerings:
        monthly[o.month] += o.amount
    year_total = sum(monthly.values())

    # 按奉獻類型彙總
    by_type = {}
    for o in offerings:
        by_type[o.offering_type] = by_type.get(o.offering_type, Decimal('0')) + o.amount

    return render_template('member_detail.html',
        m=m, year=year, years=YEAR_RANGE,
        offerings=offerings, monthly=monthly, year_total=year_total,
        by_type=by_type,
    )


@app.route('/members/edit/<int:m_id>', methods=['GET', 'POST'])
def member_edit(m_id):
    m = Member.query.get_or_404(m_id)
    units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    if request.method == 'POST':
        try:
            def parse_date(s):
                return datetime.strptime(s, '%Y-%m-%d').date() if s else None

            m.name = request.form['name'].strip()
            m.gender = request.form.get('gender', '')
            m.birth_date = parse_date(request.form.get('birth_date', ''))
            m.phone = request.form.get('phone', '').strip()
            m.address = request.form.get('address', '').strip()
            m.email = request.form.get('email', '').strip()
            m.join_date = parse_date(request.form.get('join_date', ''))
            m.baptism_date = parse_date(request.form.get('baptism_date', ''))
            m.status = request.form.get('status', '在籍')
            if 'unit_id' in request.form:
                m.unit_id = int(request.form['unit_id']) if request.form.get('unit_id') else None
            m.notes = request.form.get('notes', '').strip()
            db.session.commit()
            flash('會友資料已更新', 'success')
            return redirect(url_for('member_detail', m_id=m.id))
        except Exception as e:
            db.session.rollback()
            flash(f'更新失敗：{e}', 'danger')
    return render_template('member_edit.html', m=m, units=units)


@app.route('/members/delete/<int:m_id>', methods=['POST'])
def member_delete(m_id):
    m = Member.query.get_or_404(m_id)
    po_count = PersonalOffering.query.filter_by(member_id=m.id).count()
    name = m.name
    db.session.delete(m)
    db.session.commit()
    if po_count:
        flash(f'會友「{name}」已從資料庫刪除（含個人奉獻 {po_count} 筆）', 'warning')
    else:
        flash(f'會友「{name}」已從資料庫刪除', 'warning')
    return redirect(url_for('member_list'))


# ── 個人奉獻記錄 ──────────────────────────────────────────────────────────────
@app.route('/personal_offering')
def personal_offering_list():
    year = request.args.get('year', YEAR_RANGE[0], type=int)
    month = request.args.get('month', 0, type=int)
    member_id = request.args.get('member_id', '', type=str)

    query = PersonalOffering.query.filter_by(year=year)
    if month:
        query = query.filter_by(month=month)
    if member_id:
        query = query.filter_by(member_id=int(member_id))

    offerings = query.order_by(PersonalOffering.date.desc(), PersonalOffering.member_id).all()
    members = Member.query.filter_by(is_active=True).order_by(Member.name).all()

    total = sum(o.amount for o in offerings)
    return render_template('personal_offering_list.html',
        offerings=offerings, members=members,
        year=year, month=month, member_id=member_id,
        years=YEAR_RANGE, months=list(range(1, 13)),
        total=total,
    )


@app.route('/personal_offering/new', methods=['GET', 'POST'])
def personal_offering_new():
    members  = Member.query.filter_by(is_active=True, status='在籍').order_by(Member.name).all()
    units    = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    accounts = Account.query.filter_by(type='income').order_by(Account.sort_order).all()
    preset_member_id = request.args.get('member_id', '', type=str)
    preset_member_name = ''
    if preset_member_id:
        picked = next((m for m in members if str(m.id) == str(preset_member_id)), None)
        if picked:
            preset_member_name = picked.name
    if request.method == 'POST':
        try:
            o_date = datetime.strptime(request.form['date'], '%Y-%m-%d').date()
            uid = request.form.get('unit_id') or None
            aid = request.form.get('account_id') or None
            member_id_raw = request.form.get('member_id', '').strip()
            if not member_id_raw.isdigit():
                raise ValueError('請先從搜尋結果選擇有效會友')
            o = PersonalOffering(
                member_id=int(member_id_raw),
                unit_id=int(uid) if uid else None,
                account_id=int(aid) if aid else None,
                date=o_date,
                year=o_date.year,
                month=o_date.month,
                offering_type=request.form.get('offering_type', ''),
                amount=Decimal(request.form.get('amount') or '0'),
                receipt_no=request.form.get('receipt_no', '').strip(),
                notes=request.form.get('notes', '').strip(),
            )
            db.session.add(o)
            db.session.commit()
            flash('個人奉獻記錄已儲存', 'success')
            if request.form.get('_continue'):
                return redirect(url_for('personal_offering_new'))
            return redirect(url_for('personal_offering_list', year=o_date.year))
        except Exception as e:
            db.session.rollback()
            flash(f'儲存失敗：{e}', 'danger')
    default_year = YEAR_RANGE[0]
    return render_template('personal_offering_new.html',
        members=members, units=units, accounts=accounts, offering_types=OFFERING_TYPES,
        today=date.today().strftime('%Y-%m-%d'),
        preset_member_id=preset_member_id,
        preset_member_name=preset_member_name,
        default_year=default_year,
    )


@app.route('/personal_offering/delete/<int:o_id>', methods=['POST'])
def personal_offering_delete(o_id):
    o = PersonalOffering.query.get_or_404(o_id)
    year = o.year
    db.session.delete(o)
    db.session.commit()
    flash('已刪除個人奉獻記錄', 'warning')
    return redirect(url_for('personal_offering_list', year=year))


@app.route('/personal_offering/report')
def personal_offering_report():
    """個人奉獻年報表 — 橫軸：月份，縱軸：會友"""
    year = request.args.get('year', YEAR_RANGE[0], type=int)
    offering_type = request.args.get('offering_type', '')
    unit_id = request.args.get('unit_id', '', type=str)

    # 取得所有在籍會友
    mq = Member.query.filter_by(is_active=True, status='在籍')
    if unit_id:
        mq = mq.filter_by(unit_id=int(unit_id))
    members = mq.order_by(Member.name).all()

    # 取得該年所有個人奉獻
    oq = PersonalOffering.query.filter_by(year=year)
    if offering_type:
        oq = oq.filter_by(offering_type=offering_type)
    if unit_id:
        member_ids = [m.id for m in members]
        oq = oq.filter(PersonalOffering.member_id.in_(member_ids))
    all_offerings = oq.all()

    # 建立 {member_id: {month: total}} 矩陣
    matrix = {}
    for m in members:
        matrix[m.id] = {mo: Decimal('0') for mo in range(1, 13)}
    for o in all_offerings:
        if o.member_id in matrix:
            matrix[o.member_id][o.month] += o.amount

    # 每月合計
    month_totals = {mo: sum(matrix[m.id][mo] for m in members) for mo in range(1, 13)}
    grand_total = sum(month_totals.values())

    units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()

    return render_template('personal_offering_report.html',
        year=year, years=YEAR_RANGE,
        members=members, matrix=matrix,
        month_totals=month_totals, grand_total=grand_total,
        offering_types=OFFERING_TYPES,
        offering_type=offering_type,
        units=units, unit_id=unit_id,
        months=list(range(1, 13)),
    )


# ── 會計檢帳表 ────────────────────────────────────────────────────────────────
@app.route('/accounting-check/')
@performance_monitor
def accounting_check_list():
    year  = request.args.get('year',  date.today().year,  type=int)
    checks = AccountingCheck.query\
        .options(joinedload(AccountingCheck.unit))\
        .filter_by(year=year)\
        .order_by(AccountingCheck.year.desc(), AccountingCheck.month.desc(),
                  AccountingCheck.audit_date.desc()).all()
    units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()
    return render_template('accounting_check_list.html',
        checks=checks, year=year, years=YEAR_RANGE, units=units, today=date.today())


@app.route('/accounting-check/new', methods=['GET', 'POST'])
@performance_monitor
def accounting_check_new():
    units = Unit.query.filter_by(is_active=True).order_by(Unit.sort_order, Unit.id).all()

    if request.method == 'POST':
        try:
            audit_type = request.form.get('audit_type', '第一季查帳')
            _, _, quarter_auto, _ = accounting_period(audit_type)
            unit_id_raw = request.form.get('unit_id')
            chk = AccountingCheck(
                audit_date   = datetime.strptime(request.form['audit_date'], '%Y-%m-%d').date(),
                audit_type   = audit_type,
                year         = int(request.form['year']),
                quarter      = int(request.form.get('quarter') or '0') or quarter_auto,
                month        = int(request.form.get('month') or '0') or None,
                unit_id      = int(unit_id_raw) if unit_id_raw else None,
                auditor      = request.form.get('auditor', ''),
                prev_period_balance = Decimal(request.form.get('prev_period_balance') or '0'),
                total_income       = Decimal(request.form.get('total_income') or '0'),
                total_expense      = Decimal(request.form.get('total_expense') or '0'),
                net_balance        = Decimal(request.form.get('net_balance') or '0'),
                current_balance    = Decimal(request.form.get('current_balance') or '0'),
                cumulative_balance = Decimal(request.form.get('cumulative_balance') or '0'),
                cash_on_hand       = Decimal(request.form.get('cash_on_hand') or '0'),
                post_savings       = Decimal(request.form.get('post_savings') or '0'),
                post_fixed         = Decimal(request.form.get('post_fixed') or '0'),
                bank_savings       = Decimal(request.form.get('bank_savings') or '0'),
                bank_fixed         = Decimal(request.form.get('bank_fixed') or '0'),
                credit_union_stock = Decimal(request.form.get('credit_union_stock') or '0'),
                credit_union_fixed = Decimal(request.form.get('credit_union_fixed') or '0'),
                total_deposits     = Decimal(request.form.get('total_deposits') or '0'),
                post_account_name  = request.form.get('post_account_name', ''),
                post_account_no    = request.form.get('post_account_no', ''),
                bank_name          = request.form.get('bank_name', ''),
                bank_account_name  = request.form.get('bank_account_name', ''),
                bank_account_no    = request.form.get('bank_account_no', ''),
                audit_result       = request.form.get('audit_result', ''),
                difference         = Decimal(request.form.get('difference') or '0'),
                suggestions        = request.form.get('suggestions', ''),
            )
            db.session.add(chk)
            db.session.commit()
            flash('會計檢帳表已儲存', 'success')
            return redirect(url_for('accounting_check_detail', chk_id=chk.id))
        except Exception as e:
            db.session.rollback()
            flash(f'儲存失敗：{e}', 'danger')

    # GET — 顯示表單
    year       = request.args.get('year', date.today().year, type=int)
    audit_type = request.args.get('audit_type', '第一季查帳')
    unit_id    = request.args.get('unit_id', None, type=int)

    return render_template('accounting_check_new.html',
        units=units, year=year, audit_type=audit_type, unit_id=unit_id,
        years=YEAR_RANGE, today=date.today())


if __name__ == '__main__':
    with app.app_context():
        init_db()
    app.run(debug=True, port=5000)
