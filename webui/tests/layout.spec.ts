import { test, expect } from '@playwright/test'

test('desktop panes resize and remember widths; array fields remain typed', async ({ page }) => {
  await page.goto('/')
  await page.getByLabel('管理口令').fill(process.env.FOREST_TEST_WEB_PASSWORD || 'admin')
  await page.getByRole('button', { name: '进入调试台' }).click()
  await expect(page.locator('.connection.live')).toBeVisible()
  const snapshot = await page.request.get('/api/inspect/snapshot').then((r) => r.json())
  test.skip(
    !snapshot.modules.some((m: { object: string }) => m.object === '/module/agent'),
    'Agent settings are used for this check.',
  )
  await page.setViewportSize({ width: 1280, height: 720 })
  async function configuration() {
    await page
      .locator('.object-button code')
      .filter({ hasText: /^\/module\/agent$/ })
      .click()
    await page.getByRole('button', { name: '配置表', exact: true }).click()
    await page.locator('.config-key').filter({ hasText: '/settings' }).click()
  }
  await configuration()
  const sidebar = page.getByRole('separator', { name: '模块侧栏宽度' })
  const keys = page.getByRole('separator', { name: '配置列表宽度' })
  async function drag(handle: typeof sidebar, delta: number) {
    const box = (await handle.boundingBox())!
    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2)
    await page.mouse.down()
    await page.mouse.move(box.x + box.width / 2 + delta, box.y + box.height / 2, { steps: 8 })
    await page.mouse.up()
  }
  await sidebar.dblclick()
  await keys.dblclick()
  await drag(sidebar, 60)
  await drag(keys, 75)
  await expect(sidebar).toHaveAttribute('aria-valuenow', '290')
  await expect(keys).toHaveAttribute('aria-valuenow', '305')
  await page.reload()
  await expect(page.locator('.connection.live')).toBeVisible()
  await configuration()
  await expect(sidebar).toHaveAttribute('aria-valuenow', '290')
  await expect(keys).toHaveAttribute('aria-valuenow', '305')
  await keys.focus()
  await page.keyboard.press('ArrowLeft')
  await expect(keys).toHaveAttribute('aria-valuenow', '295')
  await sidebar.dblclick()
  await keys.dblclick()
  await expect(sidebar).toHaveAttribute('aria-valuenow', '230')
  await expect(keys).toHaveAttribute('aria-valuenow', '230')
  await expect(page.locator('.config-key > span')).toHaveCount(0)

  const object = '/module/agent'
  const key = '/settings'
  const valueUrl = `/api/inspect/config-value?${new URLSearchParams({ object, key })}`
  const original = await page.request.get(valueUrl).then((r) => r.text())
  try {
    await page.getByRole('button', { name: '编辑', exact: true }).click()
    const items = page.getByRole('textbox', { name: /^allowed_groups \d+$/ })
    await expect(items.first()).toBeVisible()
    const count = await items.count()
    for (const item of await items.all()) expect(await item.inputValue()).toMatch(/^\d+$/)
    await page.getByRole('button', { name: '添加一项', exact: true }).click()
    await expect(items).toHaveCount(count + 1)
    await items.last().fill('123')
    await page
      .getByRole('button', { name: `删除 allowed_groups ${count + 1}`, exact: true })
      .click()
    await expect(items).toHaveCount(count)
    await keys.focus()
    await page.keyboard.press('End')
    await expect(keys).toHaveAttribute('aria-valuenow', '520')
    const labelBox = (await page
      .locator('.config-field-label')
      .filter({ hasText: /^allowed_groups$/ })
      .boundingBox())!
    const listBox = (await page.locator('.config-list').boundingBox())!
    expect(Math.abs(labelBox.x - listBox.x)).toBeLessThan(2)
    await keys.dblclick()
    await expect(page.locator('.config-field-label').filter({ hasText: /^bot_name$/ })).toHaveCount(
      1,
    )
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
    expect(await page.evaluate(() => document.documentElement.scrollHeight <= innerHeight)).toBe(
      true,
    )
    await page.screenshot({ path: 'test-results/config-layout-1280.png' })
    await page.getByRole('button', { name: '保存', exact: true }).click()
    await expect(page.locator('.config-editor')).toHaveCount(0)
    expect((await page.request.get(valueUrl).then((r) => r.text())) === original).toBe(true)
  } finally {
    const restored = await page.request.post('/api/config/set', {
      data: { object, key, value: original },
    })
    expect(restored.status()).toBe(204)
  }
})
