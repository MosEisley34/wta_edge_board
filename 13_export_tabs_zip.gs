function exportAllTabsZip_(mode) {
  const normalizedMode = mode === 'auto' ? 'auto' : 'manual';
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = ss.getSheets();
  const generatedAtUtc = new Date().toISOString();
  const timestampUtc = buildUtcTimestampForZip_();

  const manifestTabs = [];
  const blobs = [];

  sheets.forEach((sheet, index) => {
    const sheetName = sheet.getName();
    const values = sheet.getDataRange().getValues();
    const rowCount = values.length;
    const columnCount = rowCount ? values[0].length : 0;
    const csvText = valuesToCsv_(values);
    const safeFileName = sanitizeTabNameForFilename_(sheetName) || ('tab_' + (index + 1));

    blobs.push(Utilities.newBlob(csvText, 'text/csv', safeFileName + '.csv'));
    manifestTabs.push({
      tab_name: sheetName,
      csv_filename: safeFileName + '.csv',
      row_count: rowCount,
      column_count: columnCount,
    });
  });

  const manifest = {
    generated_at_utc: generatedAtUtc,
    spreadsheet_id: ss.getId(),
    spreadsheet_name: ss.getName(),
    tab_list: manifestTabs.map((tab) => tab.tab_name),
    tabs: manifestTabs,
    export_version: 'v1',
    mode: normalizedMode,
  };

  blobs.push(Utilities.newBlob(JSON.stringify(manifest, null, 2), 'application/json', 'manifest.json'));

  const zipName = normalizedMode === 'auto'
    ? 'wta_tabs_' + timestampUtc + '_auto.zip'
    : 'wta_tabs_' + timestampUtc + '_manual.zip';
  const zipBlob = Utilities.zip(blobs, zipName);

  if (normalizedMode === 'auto') {
    const config = getConfig_();
    const retentionDays = Math.max(
      0,
      Number(config.EXPORT_AUTO_ZIP_RETENTION_DAYS || 30)
    );
    const destination = ensureDriveFolderAtRoot_('wta_edge_board');
    const file = destination.createFile(zipBlob);
    cleanupAutoZipRetention_(retentionDays, destination);
    appendLogRow_({
      row_type: 'ops',
      run_id: buildRunId_(),
      stage: 'exportAllTabsZip',
      status: 'success',
      reason_code: 'tabs_zip_exported_auto',
      message: JSON.stringify({
        mode: normalizedMode,
        file_id: file.getId(),
        file_name: file.getName(),
        folder_id: destination.getId(),
        tab_count: manifestTabs.length,
      }),
    });
    return {
      mode: normalizedMode,
      file_id: file.getId(),
      file_name: file.getName(),
      folder_id: destination.getId(),
      generated_at_utc: generatedAtUtc,
    };
  }

  showZipDownloadDialog_(zipBlob);
  return {
    mode: normalizedMode,
    file_name: zipName,
    generated_at_utc: generatedAtUtc,
  };
}

function valuesToCsv_(values) {
  if (!values || !values.length) return '';
  return values
    .map((row) => row.map((cell) => escapeCsvCell_(cell)).join(','))
    .join('\n');
}

function escapeCsvCell_(value) {
  if (value === null || value === undefined) return '';

  let text;
  if (value instanceof Date) {
    text = value.toISOString();
  } else if (typeof value === 'boolean' || typeof value === 'number') {
    text = String(value);
  } else {
    text = String(value);
  }

  if (/[",\n\r]/.test(text)) {
    return '"' + text.replace(/"/g, '""') + '"';
  }
  return text;
}

function sanitizeTabNameForFilename_(tabName) {
  return String(tabName || '')
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 120);
}

function buildUtcTimestampForZip_() {
  const iso = new Date().toISOString();
  return iso.replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z');
}

function ensureDriveFolderAtRoot_(folderName) {
  const root = DriveApp.getRootFolder();
  const folders = root.getFoldersByName(folderName);
  if (folders.hasNext()) return folders.next();
  return root.createFolder(folderName);
}

function showZipDownloadDialog_(zipBlob) {
  const html = HtmlService.createHtmlOutput(
    '<div style="font-family:Arial,sans-serif;padding:16px;">'
      + '<h3 style="margin-top:0;">Export all tabs (ZIP)</h3>'
      + '<p>Your ZIP is ready. Click the link below to download it.</p>'
      + '<p><a href="data:application/zip;base64,' + Utilities.base64Encode(zipBlob.getBytes()) + '" download="' + zipBlob.getName() + '">Download ' + zipBlob.getName() + '</a></p>'
      + '</div>'
  ).setWidth(420).setHeight(180);

  SpreadsheetApp.getUi().showModalDialog(html, 'WTA Edge Export');
}

function cleanupAutoZipRetention_(daysToKeep, destinationFolder) {
  const retentionDays = Math.max(0, Number(daysToKeep || 30));
  const folder = destinationFolder || ensureDriveFolderAtRoot_('wta_edge_board');
  const nowMs = Date.now();
  const cutoffMs = nowMs - (retentionDays * 24 * 60 * 60 * 1000);
  const files = folder.getFiles();
  const autoZipPattern = /^wta_tabs_.*_auto\.zip$/;
  const runId = buildRunId_();

  let scannedCount = 0;
  let matchedCount = 0;
  let deletedCount = 0;
  let errorCount = 0;

  while (files.hasNext()) {
    scannedCount += 1;
    const file = files.next();
    const fileName = String(file.getName() || '');
    if (!autoZipPattern.test(fileName)) continue;
    matchedCount += 1;

    try {
      const createdAt = getSafeAutoZipCreatedAt_(file);
      const createdAtMs = createdAt ? createdAt.getTime() : NaN;
      if (!isFinite(createdAtMs)) {
        throw new Error('could_not_resolve_created_at');
      }

      if (createdAtMs < cutoffMs) {
        file.setTrashed(true);
        deletedCount += 1;
      }
    } catch (error) {
      errorCount += 1;
      appendLogRow_({
        row_type: 'ops',
        run_id: runId,
        stage: 'cleanupAutoZipRetention',
        status: 'error',
        reason_code: 'tabs_zip_retention_cleanup_error',
        message: JSON.stringify({
          file_id: file.getId(),
          file_name: fileName,
          retention_days: retentionDays,
          error: String(error && error.message ? error.message : error),
        }),
      });
    }
  }

  appendLogRow_({
    row_type: 'ops',
    run_id: runId,
    stage: 'cleanupAutoZipRetention',
    status: errorCount ? 'warning' : 'success',
    reason_code: 'tabs_zip_retention_cleanup_summary',
    message: JSON.stringify({
      retention_days: retentionDays,
      scanned_count: scannedCount,
      matched_count: matchedCount,
      deleted_count: deletedCount,
      error_count: errorCount,
      cutoff_utc: new Date(cutoffMs).toISOString(),
      folder_id: folder.getId(),
    }),
  });
}

function getSafeAutoZipCreatedAt_(file) {
  const parsedFromName = parseAutoZipTimestampFromName_(file.getName());
  if (parsedFromName) return parsedFromName;

  const createdAt = file.getDateCreated();
  if (createdAt instanceof Date && !isNaN(createdAt.getTime())) {
    return createdAt;
  }
  return null;
}

function parseAutoZipTimestampFromName_(fileName) {
  const text = String(fileName || '');
  const match = text.match(/^wta_tabs_(\d{8}T\d{6}Z)_auto\.zip$/);
  if (!match) return null;

  const compact = match[1];
  const iso = compact.replace(
    /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/,
    '$1-$2-$3T$4:$5:$6Z'
  );
  const parsed = new Date(iso);
  if (isNaN(parsed.getTime())) return null;
  return parsed;
}
