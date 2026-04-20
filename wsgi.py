"""
PythonAnywhere WSGI 設定檔
━━━━━━━━━━━━━━━━━━━━━━━━━━
部署步驟（在 PythonAnywhere Web 頁面設定）：

WSGI configuration file 路徑填寫：
  /home/valjuluchurch/church_finance/wsgi.py

Source code 目錄：
  /home/valjuluchurch/church_finance

Working directory：
  /home/valjuluchurch/church_finance
"""
import sys
import os

# 加入專案路徑
project_home = '/home/valjuluchurch/church_finance'
if project_home not in sys.path:
    sys.path.insert(0, project_home)

# 設定工作目錄（讓 SQLite 資料庫建在正確位置）
os.chdir(project_home)

from app import app as application  # noqa

# 初始化資料庫（第一次部署時）
from app import init_db
with application.app_context():
    init_db()
