const fs = require('fs/promises');
const path = require('path');

const MAS_MSB_I6_PAGE_URL = 'https://www.mas.gov.sg/statistics/monthly-statistical-bulletin/i-6-commercial-banks-loan-limits-granted-to-non-bank-customers-by-industry';

async function ensureDir(filePath) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
}

async function downloadMasI6Csv({ outputPath = '/tmp/mas_i6.csv', verifyMode = false } = {}) {
  let playwright;
  try {
    playwright = require('playwright');
  } catch (err) {
    throw new Error(`Playwright unavailable for MAS I.6 CSV download: ${err.message}`);
  }

  await ensureDir(outputPath);

  const browser = await playwright.chromium.launch({ headless: true });
  const context = await browser.newContext({ acceptDownloads: true });
  const page = await context.newPage();

  try {
    await page.goto(MAS_MSB_I6_PAGE_URL, { waitUntil: 'domcontentloaded', timeout: 60_000 });

    await page.waitForFunction(() => {
      const bodyText = document.body?.innerText || '';
      const hasDownloadText = /download/i.test(bodyText);
      const hasFrequency = /frequency/i.test(bodyText);
      const hasFrom = /\bfrom\b/i.test(bodyText);
      const hasTo = /\bto\b/i.test(bodyText);
      return hasDownloadText && hasFrequency && hasFrom && hasTo;
    }, { timeout: 60_000 });

    const monthlySelect = page.getByLabel(/frequency/i).first();
    if (await monthlySelect.count()) {
      try {
        await monthlySelect.selectOption({ label: 'Monthly' });
      } catch (_) {
        // Keep default when option select is not supported by the control.
      }
    }

    const downloadButton = page.getByRole('button', { name: /download/i }).first();
    if (!(await downloadButton.count())) {
      throw new Error('Download button not found on MAS I.6 page');
    }

    const [download] = await Promise.all([
      page.waitForEvent('download', { timeout: 60_000 }),
      downloadButton.click()
    ]);

    await download.saveAs(outputPath);

    const stat = await fs.stat(outputPath);
    if (!stat.size) {
      throw new Error('MAS I.6 CSV download completed but file is empty');
    }

    if (verifyMode) {
      console.log(`[verify-mas-msb-i6] csv_downloaded_path=${outputPath}`);
      console.log(`[verify-mas-msb-i6] csv_downloaded_size_bytes=${stat.size}`);
    }

    return { outputPath, sizeBytes: stat.size };
  } finally {
    await context.close();
    await browser.close();
  }
}

module.exports = {
  MAS_MSB_I6_PAGE_URL,
  downloadMasI6Csv
};
