/**
 * Google Drive カメラマンデータ自動バックアップスクリプト v2
 *
 * タイムアウト対策: 5分経過で自動中断 → 1分後に続きから再開
 * PropertiesService でチェックポイントを保存
 *
 * ソース: カメラマン > 月
 * バックアップ先: 月 > カメラマン に変換して複製
 */

var CONFIG = {
  SOURCE_FOLDER_ID: '1CyxRN30gHXnmziPWnFnmPpP_M93k8Asc',
  BACKUP_FOLDER_ID: '1OLQgJQMHBuV9Xrvzaz1X0RJhLO7NnWJY',
  MAX_RUNTIME_MS: 5 * 60 * 1000,
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
          scheduleContinuation_();
          Logger.log('--- 5分経過: 中断して1分後に再開 ---');
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

        var result = copyNewFiles_(monthFolder, bPhotographer.folder);
        totalCopied += result.copied;

        if (result.copied > 0) {
          Logger.log('    ' + monthName + ': ' + result.copied + '件複製');
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
          scheduleContinuation_();
          Logger.log('--- サブフォルダ処理中に中断 ---');
          return;
        }
      }

      var directResult = copyDirectFiles_(photographer, backupFolder, photographerName);
      totalCopied += directResult.copied;
      totalFolders += directResult.folders;
    }

    props.deleteProperty('backupState');
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
    props.deleteProperty('backupState');
    removeContinuationTriggers_();
    throw e;
  }
}

function getFolderList_(parent) {
  var list = [];
  var iter = parent.getFolders();
  while (iter.hasNext()) list.push(iter.next());
  return list;
}

function getOrCreateFolder_(parent, name) {
  var iter = parent.getFoldersByName(name);
  if (iter.hasNext()) return { folder: iter.next(), isNew: false };
  return { folder: parent.createFolder(name), isNew: true };
}

function copyNewFiles_(src, dest) {
  var existing = {};
  var iter = dest.getFiles();
  while (iter.hasNext()) existing[iter.next().getName()] = true;

  var copied = 0;
  iter = src.getFiles();
  while (iter.hasNext()) {
    var f = iter.next();
    if (!existing[f.getName()]) {
      f.makeCopy(f.getName(), dest);
      copied++;
    }
  }
  return { copied: copied };
}

function copySubfolders_(src, dest, startTime) {
  var copied = 0, folders = 0, timedOut = false;
  var subs = getFolderList_(src);

  for (var i = 0; i < subs.length; i++) {
    if (new Date().getTime() - startTime > CONFIG.MAX_RUNTIME_MS) {
      return { copied: copied, folders: folders, timedOut: true };
    }
    var sub = subs[i];
    var d = getOrCreateFolder_(dest, sub.getName());
    if (d.isNew) folders++;

    var r = copyNewFiles_(sub, d.folder);
    copied += r.copied;

    var sr = copySubfolders_(sub, d.folder, startTime);
    copied += sr.copied;
    folders += sr.folders;
    if (sr.timedOut) return { copied: copied, folders: folders, timedOut: true };
  }
  return { copied: copied, folders: folders, timedOut: false };
}

function copyDirectFiles_(photographer, backupFolder, photographerName) {
  var iter = photographer.getFiles();
  var copied = 0, folders = 0;
  if (!iter.hasNext()) return { copied: 0, folders: 0 };

  var uf = getOrCreateFolder_(backupFolder, '_未分類');
  if (uf.isNew) folders++;
  var up = getOrCreateFolder_(uf.folder, photographerName);
  if (up.isNew) folders++;

  while (iter.hasNext()) {
    var f = iter.next();
    var ex = up.folder.getFilesByName(f.getName());
    if (!ex.hasNext()) {
      f.makeCopy(f.getName(), up.folder);
      copied++;
    }
  }
  return { copied: copied, folders: folders };
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
    if (t.getHandlerFunction() === 'backupPhotographerData') {
      // everyHours トリガーは残す、after() トリガーだけ削除
      // after() トリガーは getTriggerSource() が CLOCK で isTimeBased
      // 区別が難しいので全部消してから12hを再設定する方式に変更せず
      // ここでは何もしない（setupで管理）
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
  PropertiesService.getScriptProperties().deleteProperty('backupState');
  Logger.log('すべてのトリガーと状態をリセットしました。');
}

function resetState() {
  PropertiesService.getScriptProperties().deleteProperty('backupState');
  Logger.log('バックアップ状態をリセットしました。');
}