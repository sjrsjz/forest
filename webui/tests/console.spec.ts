import { test, expect } from '@playwright/test'

const password = process.env.FOREST_TEST_WEB_PASSWORD || 'admin'

test('real console: authentication, runtime stream, dependency SVG and config inspection', async ({
  page,
}) => {
  const errors: string[] = []
  page.on('pageerror', (error) => errors.push(error.message))
  const unauthorized = await page.request.get('/api/inspect/snapshot')
  expect(unauthorized.status()).toBe(401)
  await page.goto('/')
  await expect(page.getByRole('button', { name: '进入调试台' })).toBeEnabled()
  await page.getByLabel('管理口令').fill('incorrect-password')
  await page.getByRole('button', { name: '进入调试台' }).click()
  await expect(page.getByRole('alert')).toContainText('管理口令不正确')
  await page.getByLabel('管理口令').fill(password)
  await page.getByRole('button', { name: '进入调试台' }).click()
  await expect(page.getByText('实时更新', { exact: true })).toBeVisible()
  await expect(page.locator('.workspace-heading h1')).toHaveText('Forest Test')
  await expect(page.locator('.stats-strip')).toContainText('占用请求名额')
  const firstSample = await page.locator('.sample-time').textContent()
  await expect.poll(() => page.locator('.sample-time').textContent()).not.toBe(firstSample)
  await page.screenshot({ path: 'test-results/runtime.png' })

  await page.getByRole('button', { name: '依赖关系', exact: true }).click()
  await expect(page.locator('.graph-svg svg')).toBeVisible()
  await expect(page.locator('.graph-svg')).toContainText('/module/forest-test')
  await expect(page.locator('.graph-svg')).toContainText('/core/onebot')
  await expect(page.locator('.graph-svg')).toContainText('/send_group_message')
  await expect(page.locator('.graph-view [role=alert]')).toHaveCount(0)
  await page.screenshot({ path: 'test-results/dependencies.png' })
  const graphId = await page.locator('.graph-svg svg').getAttribute('id')
  const secondSample = await page.locator('.sample-time').textContent()
  await expect.poll(() => page.locator('.sample-time').textContent()).not.toBe(secondSample)
  expect(await page.locator('.graph-svg svg').getAttribute('id')).toBe(graphId)

  await page.getByRole('button', { name: '配置表', exact: true }).click()
  await expect(page.locator('.config-key.active')).toContainText('/state/replies')
  await expect(page.locator('.entry-value .value-scalar')).toContainText('i64')
  const result = await page.request.get(
    '/api/inspect/config-entry?object=/module/forest-test&key=/state/replies',
  )
  const entry = await result.json()
  await expect(page.locator('.entry-value pre')).toHaveText(entry.data.current.text)
  await page.getByRole('button', { name: '默认值', exact: true }).click()
  await expect(page.locator('.entry-value pre')).toHaveText('0')
  await page.getByRole('button', { name: 'Schema', exact: true }).click()
  await expect(page.locator('.schema-text')).toContainText('i64')
  await page.getByRole('button', { name: '当前值', exact: true }).click()
  await page.screenshot({ path: 'test-results/configuration.png' })
  const downloadPromise = page.waitForEvent('download')
  await page.getByRole('link', { name: '下载完整值' }).click()
  const download = await downloadPromise
  expect(download.suggestedFilename()).toBe('forest-value.txt')
  await page.getByLabel('自动刷新').check()
  await expect.poll(() => page.request.get('/api/session').then((r) => r.status())).toBe(204)
  await page.setViewportSize({ width: 1280, height: 720 })
  const layout = await page.evaluate(() => ({
    height: document.documentElement.scrollHeight,
    width: document.documentElement.scrollWidth,
    viewportHeight: innerHeight,
    viewportWidth: innerWidth,
  }))
  expect(layout.height).toBeLessThanOrEqual(layout.viewportHeight)
  expect(layout.width).toBeLessThanOrEqual(layout.viewportWidth)
  await page.screenshot({ path: 'test-results/desktop-1280.png' })
  await page.getByRole('button', { name: '退出', exact: true }).click()
  await expect(page.getByRole('button', { name: '进入调试台' })).toBeVisible()
  expect((await page.request.get('/api/inspect/snapshot')).status()).toBe(401)
  expect(errors).toEqual([])
})

test('module management: inspect local package, reload, unload and load', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (error) => errors.push(error.message))
  await page.goto('/')
  await page.getByLabel('管理口令').fill(password)
  await page.getByRole('button', { name: '进入调试台' }).click()
  await expect(page.locator('.connection.live')).toBeVisible()
  const object = '/module/forest-test'
  const before = await page.request.get('/api/inspect/snapshot').then((r) => r.json())
  const original = before.modules.find((m: { object: string }) => m.object === object)
  expect(original.lifecycle.phase).toBe('serving')
  const counterUrl = '/api/inspect/config-value?object=/module/forest-test&key=/state/replies'
  const count = await page.request.get(counterUrl).then((r) => r.text())
  try {
    await page.getByRole('button', { name: '本地包', exact: true }).click()
    const packages = page.getByRole('dialog', { name: '本地包', exact: true })
    await packages.getByLabel('包目录').selectOption('forest-test')
    await expect(packages.locator('.load-plan')).toContainText('已加载')
    await packages.getByRole('button', { name: 'Manifest', exact: true }).click()
    await expect(packages.locator('.manifest-text')).toContainText(object)
    await page.screenshot({ path: 'test-results/packages.png' })
    await page.keyboard.press('Escape')
    await expect(packages).not.toBeVisible()

    await page.getByRole('button', { name: '重载', exact: true }).click()
    const reload = page.getByRole('dialog', { name: '重载模块', exact: true })
    await expect(reload.getByRole('button', { name: '执行重载' })).toBeEnabled()
    await reload.getByRole('button', { name: '执行重载' }).click()
    await expect(reload.locator('.management-success')).toHaveText('已重载', { timeout: 30_000 })
    const after = await page.request.get('/api/inspect/snapshot').then((r) => r.json())
    expect(
      after.modules.find((m: { object: string }) => m.object === object).lifecycle.instance.id,
    ).not.toBe(original.lifecycle.instance.id)
    expect(await page.request.get(counterUrl).then((r) => r.text())).toBe(count)
    await page.screenshot({ path: 'test-results/reload.png' })
    await reload.getByRole('button', { name: '关闭模块管理' }).click()

    await page.getByRole('button', { name: '卸载', exact: true }).click()
    const unload = page.getByRole('dialog', { name: '卸载模块', exact: true })
    await unload.locator('input[value="at_yield"]').check()
    await unload.getByRole('button', { name: '卸载', exact: true }).click()
    await expect(unload.locator('.management-success')).toHaveText('已卸载')
    await unload.getByRole('button', { name: '关闭模块管理' }).click()
    await expect(page.locator('.object-button').filter({ hasText: object })).toHaveCount(0)

    await page.getByRole('button', { name: '本地包', exact: true }).click()
    const load = page.getByRole('dialog', { name: '本地包', exact: true })
    await load.getByLabel('包目录').selectOption('forest-test')
    await expect(load.locator('.load-plan')).toContainText(object)
    await load.getByRole('button', { name: '加载', exact: true }).click()
    await expect(load.locator('.management-success')).toHaveText('已加载', { timeout: 30_000 })
    await load.getByRole('button', { name: '关闭模块管理' }).click()
    await expect(page.locator('.workspace-heading h1')).toHaveText('Forest Test')
    expect(await page.request.get(counterUrl).then((r) => r.text())).toBe(count)
    await page.setViewportSize({ width: 1280, height: 720 })
    await page.getByRole('button', { name: '重载', exact: true }).click()
    await page.screenshot({ path: 'test-results/management-1280.png' })
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
    expect(errors).toEqual([])
  } finally {
    // Restore the test module even if a browser assertion failed after unloading.
    await page.request.post('/api/modules/load', { data: { directory: 'forest-test' } })
  }
})
