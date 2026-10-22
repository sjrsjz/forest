import { test, expect } from '@playwright/test'

test('logs filter locally, follow the tail, and remain inside the desktop viewport', async ({
  page,
}) => {
  const entries = Array.from({ length: 150 }, (_, i) => ({
    sequence: i + 1,
    timestamp_ms: 1700000000000 + i,
    level: i % 5 === 4 ? 'Warn' : 'Info',
    source: i % 2 === 0 ? '/module/agent' : '/core/onebot',
    message: `console row ${i}`,
  }))
  await page.route('**/api/inspect/logs', (route) =>
    route.fulfill({
      contentType: 'text/event-stream',
      body: `id: 150\nevent: logs\ndata: ${JSON.stringify({ cursor: 150, reset: true, entries })}\n\n`,
    }),
  )
  await page.goto('/')
  await page.getByLabel('管理口令').fill(process.env.FOREST_TEST_WEB_PASSWORD || 'admin')
  await page.getByRole('button', { name: '进入调试台' }).click()
  await expect(page.locator('.connection.live')).toBeVisible()
  const selected = await page.locator('.object-path').innerText()
  for (const entry of entries) if (entry.source === '/module/agent') entry.source = selected
  await page.setViewportSize({ width: 1280, height: 720 })
  await page.getByRole('button', { name: '运行日志', exact: true }).click()
  await expect(page.locator('.log-entry')).toHaveCount(150)
  const viewport = page.getByLabel('运行日志', { exact: true })
  await expect
    .poll(() =>
      viewport.evaluate(
        (element) => element.scrollHeight - element.scrollTop - element.clientHeight,
      ),
    )
    .toBeLessThan(4)
  await viewport.evaluate((element) => {
    element.scrollTop = 0
    element.dispatchEvent(new Event('scroll'))
  })
  await expect(page.getByLabel('自动滚动')).not.toBeChecked()
  await page.getByRole('button', { name: '回到最新' }).click()
  await expect(page.getByLabel('自动滚动')).toBeChecked()
  await page.getByLabel('日志级别').selectOption('Warn')
  await expect(page.locator('.log-entry')).toHaveCount(30)
  await page.getByLabel('仅当前对象').check()
  await expect(page.locator('.log-entry')).toHaveCount(15)
  await page.getByLabel('仅当前对象').uncheck()
  await page.getByLabel('搜索日志').fill('console row 149')
  await expect(page.locator('.log-entry')).toHaveCount(1)
  await expect(page.locator('.log-entry pre')).toHaveText('console row 149')
  await page.getByLabel('搜索日志').clear()
  await page.getByLabel('日志级别').selectOption('')
  expect(
    await page.evaluate(
      () =>
        document.documentElement.scrollWidth <= innerWidth &&
        document.documentElement.scrollHeight <= innerHeight,
    ),
  ).toBe(true)
  await page.screenshot({ path: 'test-results/logs-1280.png' })
})
