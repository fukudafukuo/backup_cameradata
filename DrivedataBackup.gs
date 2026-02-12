/**
 * Google Drive カメラマンデータ自動バックアップスクリプト v3
 *
 * タイムアウト対策: 4分経過で自動中断 → 1分後に続きから再開
 * PropertiesService でチェックポイントを保存
 * Drive API一時エラー対策: リトライ+指数バックオフ
 *
 * ソース: カメラマン > 月
 * バックアップ先: 月 > カメラマン に変換して複製
 */

var CONFIG = {
  SOURCE_FOLDER_ID: '1CyxRN30gHXnmziPWnFnmPpP_M93k8Asc',
  BACKUP_FOLDER_ID: '1OLQgJQMHBuV9Xrvzaz1X0RJhLO7NnWJY',
  MAX_RUNTIME_MS: 4 * 60 * 1000,
  RETRY_MAX: 3,
  RETRY_WAIT_MS: 2000,
  NOTIFICATION_EMAIL: ''
};

function backupPhotographerData() {
  var props = PropertiesService.getScriptProperties();
  var startTime = new Date().getTime();
  var state = JSON.parse(props.getProperty('backupState') || '{}');

  var isResuming = !!state.photographerIndex;
  var pIdx = state.photographerIndex || 0;
  var mIdx = state.monthIndex || 0;
  var totalCopied = state.totalCopied || 0;
  var totalFolders = state.totalFolders || 0;

  if (!isResuming) {
    Logger.log('=== バックアップ開始 ===');
  } else {
    Logger.log('=== バックアップ再開 (カメラマン#' + pIdx + ', 月#' + mIdx + ') ===');
    Logger.log('前回まで: ' + totalCopied + '件コピー済み');
  }

  try {
    var sourceFolder = DriveApp.getFolderById(CONFIG.SOURCE_FOLDER_ID);
    var backupFolder = DriveApp.getFolderById(CONFIG.BACKUP_FOLDER_ID);

    var photographers = getFolderList_(sourceFolder);
    Logger.log('カメラマン数: ' + photographers.length);

    for (var p = pIdx; p < photographers.length; p++) {
      var photographer = photographers[p];
      var photographerName = photographer.getName();
      Logger.log('  処理中: ' + photographerName);

      var months = getFolderList_(photographer);
      var startM = (p === pIdx) ? mIdx : 0;

      for (var m = startM; m < months.length; m++) {
        if (new Date().getTime() - startTime > CONFIG.MAX_RUNTIME_MS) {
          state.photographerIndex = p;
          state.monthIndex = m;
          state.totalCopied = totalCopied;
          state.totalFolders = totalFolders;
          props.setProperty('backupState', JSON.stringify(state));
          saveSyncCache_();
          scheduleContinuation_();
          Logger.log('--- 4分経過: 中断して1分後に再開 ---');
          Logger.log('進捗: カメラマン ' + (p+1) + '/' + photographers.length + ', 月 ' + (m+1) + '/' + months.length);
          Logger.log('ここまで ' + totalCopied + '件コピー');
          return;
        }

        var monthFolder = months[m];
        var monthName = monthFolder.getName();

        var bMonth = getOrCreateFolder_(backupFolder, monthName);
        if (bMonth.isNew) totalFolders++;

        var bPhotographer = getOrCreateFolder_(bMonth.folder, photographerName);
        if (bPhotographer.isNew) totalFolders++;

        var result = copyNewFiles_(monthFolder, bPhotographer.folder, startTime);
        totalCopied += result.copied;

        if (result.copied > 0) {
          Logger.log('    ' + monthName + ': ' + result.copied + '件複製');
        }

        if (result.timedOut) {
          state.photographerIndex = p;
          state.monthIndex = m;
          state.totalCopied = totalCopied;
          state.totalFolders = totalFolders;
          props.setProperty('backupState', JSON.stringify(state));
          saveSyncCache_();
          scheduleContinuation_();
          Logger.log('--- ファイルコピー中に中断 ---');
          return;
        }

        var subResult = copySubfolders_(monthFolder, bPhotographer.folder, startTime);
        totalCopied += subResult.copied;
        totalFolders += subResult.folders;

        if (subResult.timedOut) {
          state.photographerIndex = p;
          state.monthIndex = m + 1;
          state.totalCopied = totalCopied;
          state.totalFolders = totalFolders;
          props.setProperty('backupState', JSON.stringify(state));
          saveSyncCache_();
          scheduleContinuation_();
          Logger.log('--- サブフォルダ処理中に中断 ---');
          return;
        }
      }

      var directResult = copyDirectFiles_(photographer, backupFolder, photographerName, startTime);
      totalCopied += directResult.copied;
      totalFolders += directResult.folders;

      if (directResult.timedOut) {
        state.photographerIndex = p + 1;
        state.monthIndex = 0;
        state.totalCopied = totalCopied;
        state.totalFolders = totalFolders;
        props.setProperty('backupState', JSON.stringify(state));
        saveSyncCache_();
        scheduleContinuation_();
        Logger.log('--- 直接ファイルコピー中に中断 ---');
        return;
      }
    }

    props.deleteProperty('backupState');
    saveSyncCache_();
    removeContinuationTriggers_();

    var msg = '=== バックアップ完了 ===\n' +
      'フォルダ作成: ' + totalFolders + '件\n' +
      'ファイル複製: ' + totalCopied + '件';
    Logger.log(msg);

    if (CONFIG.NOTIFICATION_EMAIL) {
      MailApp.sendEmail(CONFIG.NOTIFICATION_EMAIL,
        '[完了] カメラマンデータバックアップ - ' + totalCopied + '件', msg);
    }

  } catch (e) {
    Logger.log('[エラー] ' + e.message + '\n' + e.stack);
    // エラー時も進捗を保存して次回続きから再開できるようにする
    state.totalCopied = totalCopied;
    state.totalFolders = totalFolders;
    props.setProperty('backupState', JSON.stringify(state));
    saveSyncCache_();
    scheduleContinuation_();
    Logger.log('--- エラー発生: 1分後に続きから再開予定 ---');
  }
}

function getFolderList_(parent) {
  var list = [];
  var iter = parent.getFolders();
  while (iter.hasNext()) list.push(iter.next());
  return list;
}

// 実行中のフォルダ検索結果キャッシュ（getFoldersByName の API 呼び出しを削減）
var FOLDER_CACHE_ = {};

// 永続同期キャッシュ: 前回同期時のソースフォルダのファイル数を記憶
// キャッシュヒット→dest側のAPIコールを丸ごとスキップ
// キー: ソースフォルダID, 値: ファイル数（"s:" プレフィックス付きはサブフォルダ数）
var SYNC_CACHE_ = null;

function loadSyncCache_() {
  if (SYNC_CACHE_ !== null) return;
  var raw = PropertiesService.getScriptProperties().getProperty('syncCache');
  SYNC_CACHE_ = raw ? JSON.parse(raw) : {};
}

function saveSyncCache_() {
  if (!SYNC_CACHE_) return;
  var json = JSON.stringify(SYNC_CACHE_);
  // PropertiesService は 1プロパティ 9KB まで。超えたらキャッシュをリセット
  if (json.length > 8000) {
    Logger.log('  [キャッシュ] サイズ超過のためリセット (' + json.length + 'bytes)');
    SYNC_CACHE_ = {};
    json = '{}';
  }
  PropertiesService.getScriptProperties().setProperty('syncCache', json);
}

function getOrCreateFolder_(parent, name) {
  var cacheKey = parent.getId() + '/' + name;
  if (FOLDER_CACHE_[cacheKey]) {
    return { folder: FOLDER_CACHE_[cacheKey], isNew: false };
  }
  var iter = parent.getFoldersByName(name);
  if (iter.hasNext()) {
    var folder = iter.next();
    FOLDER_CACHE_[cacheKey] = folder;
    return { folder: folder, isNew: false };
  }
  var folder = parent.createFolder(name);
  FOLDER_CACHE_[cacheKey] = folder;
  return { folder: folder, isNew: true };
}

/**
 * Drive API呼び出しをリトライ付きで実行する
 */
function retryDriveCall_(fn, label) {
  for (var attempt = 0; attempt <= CONFIG.RETRY_MAX; attempt++) {
    try {
      return fn();
    } catch (e) {
      if (attempt === CONFIG.RETRY_MAX) throw e;
      var wait = CONFIG.RETRY_WAIT_MS * Math.pow(2, attempt);
      Logger.log('    [リトライ ' + (attempt + 1) + '/' + CONFIG.RETRY_MAX + '] ' +
        (label || '') + ' (' + wait + 'ms待機)');
      Utilities.sleep(wait);
    }
  }
}

function copyNewFiles_(src, dest, startTime) {
  // ソースファイルを先に収集（空なら dest の列挙を丸ごとスキップ）
  var srcIter = retryDriveCall_(function() { return src.getFiles(); }, 'getFiles(src)');
  var srcFiles = [];
  while (srcIter.hasNext()) srcFiles.push(srcIter.next());
  if (srcFiles.length === 0) return { copied: 0, timedOut: false };

  // 永続キャッシュ: 前回同期時と同じファイル数なら dest 側を丸ごとスキップ
  loadSyncCache_();
  var srcId = src.getId();
  if (SYNC_CACHE_[srcId] === srcFiles.length) {
    return { copied: 0, timedOut: false };
  }

  // バックアップ先の既存ファイルマップを構築
  var existing = {};
  var destCount = 0;
  var destIter = retryDriveCall_(function() { return dest.getFiles(); }, 'getFiles(dest)');
  while (destIter.hasNext()) {
    existing[destIter.next().getName()] = true;
    destCount++;
  }

  // dest が src 以上のファイル数なら、全名前一致チェックだけで判定
  if (destCount >= srcFiles.length) {
    var allExist = true;
    for (var i = 0; i < srcFiles.length; i++) {
      if (!existing[srcFiles[i].getName()]) { allExist = false; break; }
    }
    if (allExist) {
      SYNC_CACHE_[srcId] = srcFiles.length;
      return { copied: 0, timedOut: false };
    }
  }

  // 新規ファイルのみコピー
  var copied = 0;
  var skipped = 0;
  for (var i = 0; i < srcFiles.length; i++) {
    if (startTime && new Date().getTime() - startTime > CONFIG.MAX_RUNTIME_MS) {
      if (skipped > 0) Logger.log('    スキップ: ' + skipped + '件');
      return { copied: copied, timedOut: true };
    }
    var f = srcFiles[i];
    var name = f.getName();
    if (!existing[name]) {
      try {
        retryDriveCall_(function() { f.makeCopy(name, dest); }, 'makeCopy: ' + name);
        copied++;
      } catch (e) {
        Logger.log('    [スキップ] ' + name + ': ' + e.message);
        skipped++;
      }
    }
  }
  if (skipped > 0) Logger.log('    スキップ: ' + skipped + '件');
  // タイムアウトせず完了した場合のみキャッシュ更新
  SYNC_CACHE_[srcId] = srcFiles.length;
  return { copied: copied, timedOut: false };
}

function copySubfolders_(src, dest, startTime) {
  // サブフォルダ数の永続キャッシュ: 0件なら getFolderList_ 自体をスキップ
  loadSyncCache_();
  var srcId = 's:' + src.getId();
  if (SYNC_CACHE_[srcId] === 0) return { copied: 0, folders: 0, timedOut: false };

  var subs = getFolderList_(src);
  SYNC_CACHE_[srcId] = subs.length;
  if (subs.length === 0) return { copied: 0, folders: 0, timedOut: false };

  var copied = 0, folders = 0;
  for (var i = 0; i < subs.length; i++) {
    if (new Date().getTime() - startTime > CONFIG.MAX_RUNTIME_MS) {
      return { copied: copied, folders: folders, timedOut: true };
    }
    var sub = subs[i];
    var d = getOrCreateFolder_(dest, sub.getName());
    if (d.isNew) folders++;

    // copyNewFiles_ 内部で永続キャッシュを使い、変更なしなら dest 側をスキップ
    var r = copyNewFiles_(sub, d.folder, startTime);
    copied += r.copied;
    if (r.timedOut) return { copied: copied, folders: folders, timedOut: true };

    var sr = copySubfolders_(sub, d.folder, startTime);
    copied += sr.copied;
    folders += sr.folders;
    if (sr.timedOut) return { copied: copied, folders: folders, timedOut: true };
  }
  return { copied: copied, folders: folders, timedOut: false };
}

function copyDirectFiles_(photographer, backupFolder, photographerName, startTime) {
  var srcIter = retryDriveCall_(function() { return photographer.getFiles(); }, 'getFiles(photographer)');
  if (!srcIter.hasNext()) return { copied: 0, folders: 0, timedOut: false };

  var srcFiles = [];
  while (srcIter.hasNext()) srcFiles.push(srcIter.next());

  // 永続キャッシュ: 直接ファイル数が前回と同じならスキップ
  loadSyncCache_();
  var srcId = 'd:' + photographer.getId();
  if (SYNC_CACHE_[srcId] === srcFiles.length) {
    return { copied: 0, folders: 0, timedOut: false };
  }

  var copied = 0, folders = 0;
  var uf = getOrCreateFolder_(backupFolder, '_未分類');
  if (uf.isNew) folders++;
  var up = getOrCreateFolder_(uf.folder, photographerName);
  if (up.isNew) folders++;

  // 1回の getFiles() でバックアップ先の既存ファイルマップを構築
  var existing = {};
  var destIter = retryDriveCall_(function() { return up.folder.getFiles(); }, 'getFiles(dest)');
  while (destIter.hasNext()) existing[destIter.next().getName()] = true;

  for (var i = 0; i < srcFiles.length; i++) {
    if (startTime && new Date().getTime() - startTime > CONFIG.MAX_RUNTIME_MS) {
      return { copied: copied, folders: folders, timedOut: true };
    }
    var f = srcFiles[i];
    var name = f.getName();
    if (!existing[name]) {
      try {
        retryDriveCall_(function() { f.makeCopy(name, up.folder); }, 'makeCopy: ' + name);
        copied++;
      } catch (e) {
        Logger.log('    [スキップ] ' + name + ': ' + e.message);
      }
    }
  }
  SYNC_CACHE_[srcId] = srcFiles.length;
  return { copied: copied, folders: folders, timedOut: false };
}

function scheduleContinuation_() {
  removeContinuationTriggers_();
  ScriptApp.newTrigger('backupPhotographerData')
    .timeBased()
    .after(60 * 1000)
    .create();
}

function removeContinuationTriggers_() {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = triggers.length - 1; i >= 0; i--) {
    var t = triggers[i];
    if (t.getHandlerFunction() === 'backupPhotographerData' &&
        t.getEventType() === ScriptApp.EventType.CLOCK) {
      // after() で作成されたワンショットトリガーを削除
      // everyHours トリガーも CLOCK だが、after トリガーと区別できないため
      // 全て削除し、定期実行が必要なら setupTwiceDailyTrigger() を再実行する
      ScriptApp.deleteTrigger(t);
    }
  }
}

function setupTwiceDailyTrigger() {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === 'backupPhotographerData') {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }
  ScriptApp.newTrigger('backupPhotographerData')
    .timeBased()
    .everyHours(12)
    .create();
  Logger.log('12時間ごとの定期実行トリガーを設定しました。');
}

function removeAllTriggers() {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    ScriptApp.deleteTrigger(triggers[i]);
  }
  var sp = PropertiesService.getScriptProperties();
  sp.deleteProperty('backupState');
  sp.deleteProperty('syncCache');
  SYNC_CACHE_ = null;
  Logger.log('すべてのトリガーと状態をリセットしました。');
}

function resetState() {
  var sp = PropertiesService.getScriptProperties();
  sp.deleteProperty('backupState');
  sp.deleteProperty('syncCache');
  SYNC_CACHE_ = null;
  Logger.log('バックアップ状態と同期キャッシュをリセットしました。');
}