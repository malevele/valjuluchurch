// 全域：設定 now 給 base template 使用（由後端 context 處理）
// 數字格式化
function fmtNTD(n) {
  return 'NT$ ' + Math.round(n).toLocaleString();
}

// 自動隱藏 flash 訊息
document.addEventListener('DOMContentLoaded', function() {
  document.querySelectorAll('.alert:not(.alert-permanent)').forEach(function(el) {
    setTimeout(function() {
      const bsAlert = bootstrap.Alert.getOrCreateInstance(el);
      if (bsAlert) bsAlert.close();
    }, 4000);
  });
});
