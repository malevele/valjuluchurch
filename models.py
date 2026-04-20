from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, date

db = SQLAlchemy()

class Account(db.Model):
    """會計科目"""
    __tablename__ = 'accounts'
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(20), unique=True, nullable=False)
    name = db.Column(db.String(100), nullable=False)
    description = db.Column(db.String(200))  # 說明
    type = db.Column(db.String(10), nullable=False)  # income / expense
    parent_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=True)
    is_active = db.Column(db.Boolean, default=True)
    sort_order = db.Column(db.Integer, default=0)

    children = db.relationship('Account', backref=db.backref('parent', remote_side=[id]))
    transactions = db.relationship('Transaction', backref='account', lazy=True)
    budgets = db.relationship('Budget', backref='account', lazy=True)

    def __repr__(self):
        return f'<Account {self.code} {self.name}>'


class BankAccount(db.Model):
    """銀行帳戶"""
    __tablename__ = 'bank_accounts'
    id = db.Column(db.Integer, primary_key=True)
    bank_name = db.Column(db.String(100), nullable=False)
    account_type = db.Column(db.String(20), default='活期')  # 活期 / 定期
    account_no = db.Column(db.String(50), nullable=False)
    account_name = db.Column(db.String(100))
    current_balance = db.Column(db.Numeric(12, 2), default=0)
    unit_id = db.Column(db.Integer, db.ForeignKey('units.id'), nullable=True)
    is_active = db.Column(db.Boolean, default=True)
    notes = db.Column(db.Text)

    unit = db.relationship('Unit', backref='bank_accounts', foreign_keys=[unit_id])

    def __repr__(self):
        return f'<BankAccount {self.bank_name} {self.account_no}>'


class BankTransaction(db.Model):
    """銀行存提款明細"""
    __tablename__ = 'bank_transactions'
    id = db.Column(db.Integer, primary_key=True)
    bank_account_id = db.Column(db.Integer, db.ForeignKey('bank_accounts.id'), nullable=False)
    date = db.Column(db.Date, nullable=False, default=date.today)
    tx_type = db.Column(db.String(10), nullable=False)  # 存入 / 提領
    amount = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    description = db.Column(db.String(200))
    balance = db.Column(db.Numeric(12, 2), default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    bank_account = db.relationship('BankAccount', backref='bank_txs')


class Unit(db.Model):
    """各單位/團契"""
    __tablename__ = 'units'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    code = db.Column(db.String(10))   # 傳票簡碼，例如 A / B / 大 / 建
    type = db.Column(db.String(50))  # 團契 / 小組 / 其他
    treasurer = db.Column(db.String(50))
    is_active = db.Column(db.Boolean, default=True)
    notes = db.Column(db.Text)
    sort_order = db.Column(db.Integer, default=999)

    transactions = db.relationship('Transaction', backref='unit', lazy=True)

    def __repr__(self):
        return f'<Unit {self.name}>'


class Transaction(db.Model):
    """現金簿/傳票"""
    __tablename__ = 'transactions'
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, nullable=False, default=date.today)
    voucher_no = db.Column(db.String(30))
    voucher_type = db.Column(db.String(10))  # income / expense
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False)
    unit_id = db.Column(db.Integer, db.ForeignKey('units.id'), nullable=True)
    bank_account_id = db.Column(db.Integer, db.ForeignKey('bank_accounts.id'), nullable=True)
    description = db.Column(db.String(200), nullable=False)
    amount_in = db.Column(db.Numeric(12, 2), default=0)   # 收入
    amount_out = db.Column(db.Numeric(12, 2), default=0)  # 支出
    balance = db.Column(db.Numeric(12, 2), default=0)     # 餘額
    preparer = db.Column(db.String(50))     # 製票人
    reviewer = db.Column(db.String(50))     # 覆核人
    approver = db.Column(db.String(50))     # 財務負責人
    receipt_verified = db.Column(db.Boolean, default=False)  # 憑證核驗
    notes = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    bank_account = db.relationship('BankAccount', backref='transactions')

    def __repr__(self):
        return f'<Transaction {self.date} {self.description}>'


class OfferingCount(db.Model):
    """奉獻點算表"""
    __tablename__ = 'offering_counts'
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, nullable=False, default=date.today)
    meeting_type = db.Column(db.String(50), nullable=False)  # 主日崇拜/建設基金/禱告會/小組/團契
    counter1 = db.Column(db.String(50))   # 點算人1
    counter2 = db.Column(db.String(50))   # 點算人2
    total_cash = db.Column(db.Numeric(12, 2), default=0)
    description = db.Column(db.String(200))  # 摘要
    notes = db.Column(db.Text)
    attachment_no = db.Column(db.String(30))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=True)
    unit_id    = db.Column(db.Integer, db.ForeignKey('units.id'), nullable=True)
    account = db.relationship('Account', backref='offering_counts')
    unit    = db.relationship('Unit', backref='offering_counts')

    denominations = db.relationship('OfferingDenomination', backref='offering_count',
                                     cascade='all, delete-orphan', lazy=True)

    def __repr__(self):
        return f'<OfferingCount {self.date} {self.meeting_type}>'


class OfferingDenomination(db.Model):
    """奉獻面額明細"""
    __tablename__ = 'offering_denominations'
    id = db.Column(db.Integer, primary_key=True)
    offering_count_id = db.Column(db.Integer, db.ForeignKey('offering_counts.id'), nullable=False)
    denomination = db.Column(db.Integer, nullable=False)  # 面額 2000/1000/500/200/100/50/10/5/1
    count = db.Column(db.Integer, default=0)              # 張數/枚數
    amount = db.Column(db.Numeric(12, 2), default=0)      # 小計


class Budget(db.Model):
    """年度預算"""
    __tablename__ = 'budgets'
    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, nullable=False)
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False)
    amount = db.Column(db.Numeric(12, 2), default=0)
    notes = db.Column(db.Text)

    __table_args__ = (db.UniqueConstraint('year', 'account_id', name='uq_budget_year_account'),)


class CashierHandover(db.Model):
    """出納交接確認表"""
    __tablename__ = 'cashier_handovers'
    id = db.Column(db.Integer, primary_key=True)
    handover_date = db.Column(db.Date, nullable=False, default=date.today)
    cash_amount = db.Column(db.Numeric(12, 2), default=0)      # 現金金額
    bank_amount = db.Column(db.Numeric(12, 2), default=0)      # 存款金額
    total_amount = db.Column(db.Numeric(12, 2), default=0)     # 合計
    old_cashier = db.Column(db.String(50))   # 原任出納
    new_cashier = db.Column(db.String(50))   # 新任出納
    accountant = db.Column(db.String(50))    # 會計
    advisor = db.Column(db.String(50))       # 顧問
    audit_result = db.Column(db.String(200)) # 查帳結果
    bank_details = db.Column(db.Text)        # 存款明細（帳號等）
    notes = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class AuditRecord(db.Model):
    """查帳紀錄"""
    __tablename__ = 'audit_records'
    id = db.Column(db.Integer, primary_key=True)
    audit_date = db.Column(db.Date, nullable=False, default=date.today)
    period_start = db.Column(db.Date, nullable=False)
    period_end = db.Column(db.Date, nullable=False)
    auditor = db.Column(db.String(100))
    audit_type = db.Column(db.String(20))  # 月查/半年查/年查
    # 查核項目
    check_cash_match = db.Column(db.Boolean, default=False)       # 現金核對相符
    check_bank_match = db.Column(db.Boolean, default=False)       # 銀行存款相符
    check_receipts = db.Column(db.Boolean, default=False)         # 憑證合規
    check_vouchers = db.Column(db.Boolean, default=False)         # 傳票用印完整
    check_continuity = db.Column(db.Boolean, default=False)       # 連續性相符
    check_dedicated_funds = db.Column(db.Boolean, default=False)  # 專款專用
    check_salary = db.Column(db.Boolean, default=False)           # 薪資支給表
    check_seal = db.Column(db.Boolean, default=False)             # 帳簿騎縫章
    findings = db.Column(db.Text)    # 發現缺失
    status = db.Column(db.String(20), default='待辦')  # 待辦/完成/待改善
    notes = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class Member(db.Model):
    """會友資料"""
    __tablename__ = 'members'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)
    gender = db.Column(db.String(5))           # 男 / 女
    birth_date = db.Column(db.Date, nullable=True)
    phone = db.Column(db.String(30))
    address = db.Column(db.String(200))
    email = db.Column(db.String(100))
    join_date = db.Column(db.Date, nullable=True)
    status = db.Column(db.String(10), default='在籍')  # 在籍/請假/轉出/安息
    unit_id = db.Column(db.Integer, db.ForeignKey('units.id'), nullable=True)
    baptism_date = db.Column(db.Date, nullable=True)   # 受洗日期
    notes = db.Column(db.Text)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    unit = db.relationship('Unit', backref='members')
    personal_offerings = db.relationship('PersonalOffering', backref='member',
                                          cascade='all, delete-orphan', lazy=True)

    def __repr__(self):
        return f'<Member {self.name}>'


class PersonalOffering(db.Model):
    """個人奉獻記錄"""
    __tablename__ = 'personal_offerings'
    id = db.Column(db.Integer, primary_key=True)
    member_id = db.Column(db.Integer, db.ForeignKey('members.id'), nullable=False)
    unit_id = db.Column(db.Integer, db.ForeignKey('units.id'))
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'))
    date = db.Column(db.Date, nullable=False, default=date.today)
    year = db.Column(db.Integer, nullable=False)
    month = db.Column(db.Integer, nullable=False)
    offering_type = db.Column(db.String(30), nullable=False)
    amount = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    receipt_no = db.Column(db.String(30))
    notes = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    unit    = db.relationship('Unit', foreign_keys=[unit_id])
    account = db.relationship('Account', foreign_keys=[account_id])

    def __repr__(self):
        return f'<PersonalOffering {self.member.name} {self.date} {self.amount}>'


class MonthlyReconciliation(db.Model):
    """月底核對記錄（現金簿結存 vs 銀行＋現金）"""
    __tablename__ = 'monthly_reconciliations'
    id         = db.Column(db.Integer, primary_key=True)
    year       = db.Column(db.Integer, nullable=False)
    month      = db.Column(db.Integer, nullable=False)
    unit_id    = db.Column(db.Integer, db.ForeignKey('units.id'), nullable=True)
    bank_total = db.Column(db.Numeric(12, 2), default=0)   # 銀行存款
    cash_amt   = db.Column(db.Numeric(12, 2), default=0)   # 現金
    reason     = db.Column(db.Text)                         # 差異說明
    confirmed_at = db.Column(db.DateTime, default=datetime.utcnow)

    unit = db.relationship('Unit', backref='reconciliations')

    __table_args__ = (db.UniqueConstraint('year', 'month', 'unit_id', name='uq_reconcile'),)


class AccountingCheck(db.Model):
    """會計檢帳表"""
    __tablename__ = 'accounting_checks'
    id = db.Column(db.Integer, primary_key=True)
    audit_date   = db.Column(db.Date, nullable=False, default=date.today)
    audit_type   = db.Column(db.String(10), default='第一季查帳')  # 第N季查帳 / 年度總查
    year         = db.Column(db.Integer, nullable=False)
    quarter      = db.Column(db.Integer, nullable=True)   # 1-4，年度總查為 null
    month        = db.Column(db.Integer, nullable=True)   # 保留相容，季度/年度均為 null
    unit_id      = db.Column(db.Integer, db.ForeignKey('units.id'), nullable=True)
    auditor      = db.Column(db.String(50))       # 檢帳員

    # 帳冊數字（自動帶入）
    prev_period_balance = db.Column(db.Numeric(12, 2), default=0)  # 上年度/上季結存
    total_income        = db.Column(db.Numeric(12, 2), default=0)  # 總收入
    total_expense       = db.Column(db.Numeric(12, 2), default=0)  # 總支出
    net_balance         = db.Column(db.Numeric(12, 2), default=0)  # 結餘
    current_balance     = db.Column(db.Numeric(12, 2), default=0)  # 結存（帳面）
    cumulative_balance  = db.Column(db.Numeric(12, 2), default=0)  # 累計結存

    # 現金及存款明細（手動填寫）
    cash_on_hand       = db.Column(db.Numeric(12, 2), default=0)  # 庫存現金
    post_savings       = db.Column(db.Numeric(12, 2), default=0)  # 郵局活期存款
    post_fixed         = db.Column(db.Numeric(12, 2), default=0)  # 郵局定期存款
    bank_savings       = db.Column(db.Numeric(12, 2), default=0)  # 銀行活期存款
    bank_fixed         = db.Column(db.Numeric(12, 2), default=0)  # 銀行定期存款
    credit_union_stock = db.Column(db.Numeric(12, 2), default=0)  # 儲蓄互助社股金
    credit_union_fixed = db.Column(db.Numeric(12, 2), default=0)  # 儲蓄互助社定存
    total_deposits     = db.Column(db.Numeric(12, 2), default=0)  # 存款及現金總額

    # 帳戶資訊
    post_account_name  = db.Column(db.String(100))  # 郵局戶名
    post_account_no    = db.Column(db.String(50))   # 郵局帳號
    bank_name          = db.Column(db.String(100))  # 銀行名稱
    bank_account_name  = db.Column(db.String(100))  # 銀行戶名
    bank_account_no    = db.Column(db.String(50))   # 銀行存摺帳號

    # 結果
    audit_result       = db.Column(db.String(200))  # 檢帳結果
    difference         = db.Column(db.Numeric(12, 2), default=0)  # 與手帳之差額
    suggestions        = db.Column(db.Text)          # 建議事項

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    unit = db.relationship('Unit', backref='accounting_checks')

    def __repr__(self):
        return f'<AccountingCheck {self.year}/{self.month} {self.audit_date}>'


DENOMINATIONS = [2000, 1000, 500, 200, 100, 50, 10, 5, 1]
MEETING_TYPES = ['主日崇拜', '建設基金', '禱告會', '小組/團契', '聖餐獻金', '節期獻金', '復活節', '其他']
OFFERING_TYPES = ['月定奉獻', '禮拜獻金', '感恩奉獻', '感謝奉獻', '特別奉獻', '建堂奉獻', '中會奉獻', '百分百奉獻', '獻工', '其他']
YEAR_RANGE = list(range(2025, 2036))  # 2025–2035
