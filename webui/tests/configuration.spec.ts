import { test, expect } from '@playwright/test'

test('configuration: edit through ACL, retain invalid draft and protect private state', async ({
  page,
}) => {
  const password = process.env.FOREST_TEST_WEB_PASSWORD || 'admin'
  await page.goto('/')
  await page.getByLabel('管理口令').fill(password)
  await page.getByRole('button', { name: '进入调试台' }).click()
  await expect(page.locator('.connection.live')).toBeVisible()
  const snapshot = await page.request.get('/api/inspect/snapshot').then((r) => r.json())
  test.skip(
    !snapshot.modules.some((m: { object: string }) => m.object === '/module/context'),
    'This check uses the Agent context package.',
  )

  const object = '/module/context'
  const key = '/prompt/agent'
  const valueUrl = `/api/inspect/config-value?${new URLSearchParams({ object, key })}`
  const original = await page.request.get(valueUrl).then((r) => r.text())
  const updated = JSON.stringify(`${JSON.parse(original)}\n`)
  try {
    await page.locator('.object-button').filter({ hasText: object }).click()
    await page.getByRole('button', { name: '配置表', exact: true }).click()
    await page.getByRole('button', { name: '编辑', exact: true }).click()
    await page.getByRole('button', { name: 'Forest 文本', exact: true }).click()
    const draft = page.getByRole('textbox', { name: '配置值', exact: true })
    await expect(draft).toHaveValue(original)
    await draft.fill('true')
    await page.getByRole('button', { name: '保存', exact: true }).click()
    await expect(page.locator('.config-editor [role=alert]')).toContainText('schema')
    await expect(draft).toHaveValue('true')
    expect(await page.request.get(valueUrl).then((r) => r.text())).toBe(original)

    await draft.fill(updated)
    await page.getByRole('button', { name: '保存', exact: true }).click()
    await expect(page.locator('.config-editor')).toHaveCount(0)
    expect(await page.request.get(valueUrl).then((r) => r.text())).toBe(updated)
    await page.setViewportSize({ width: 1280, height: 720 })
    await page.getByRole('button', { name: '编辑', exact: true }).click()
    await expect(page.getByRole('textbox', { name: '当前值', exact: true })).toHaveValue(
      JSON.parse(updated),
    )
    expect(await page.evaluate(() => document.documentElement.scrollHeight <= innerHeight)).toBe(
      true,
    )
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
    await page.getByRole('button', { name: '取消', exact: true }).click()

    await page
      .locator('.object-button')
      .filter({ hasText: '/module/agent' })
      .filter({ hasNotText: '/module/agent-tools' })
      .click()
    await page.locator('.config-key').filter({ hasText: '/state/sessions' }).click()
    await expect(page.locator('.entry-value > .value-branch')).toBeVisible()
    await expect(page.getByRole('button', { name: '编辑', exact: true })).toHaveCount(0)
  } finally {
    const restored = await page.request.post('/api/config/set', {
      data: { object, key, value: original },
    })
    expect(restored.status()).toBe(204)
    expect(await page.request.get(valueUrl).then((r) => r.text())).toBe(original)
  }
})

test('API and model settings: schema forms preserve the complete stored values', async ({
  page,
}) => {
  const password = process.env.FOREST_TEST_WEB_PASSWORD || 'admin'
  await page.goto('/')
  await page.getByLabel('管理口令').fill(password)
  await page.getByRole('button', { name: '进入调试台' }).click()
  await expect(page.locator('.connection.live')).toBeVisible()
  const snapshot = await page.request.get('/api/inspect/snapshot').then((r) => r.json())
  test.skip(
    !snapshot.modules.some((m: { object: string }) => m.object === '/module/openai-provider'),
    'This check uses the Agent API packages.',
  )
  await page.setViewportSize({ width: 1280, height: 720 })
  const errors: string[] = []
  page.on('pageerror', (error) => errors.push(error.message))

  for (const [object, key] of [
    ['/module/openai-provider', '/settings'],
    ['/module/qwen-vl-embedding', '/connection'],
    ['/module/deepseek-web', '/connection'],
    ['/module/memory', '/settings'],
  ] as const) {
    const valueUrl = `/api/inspect/config-value?${new URLSearchParams({ object, key })}`
    const original = await page.request.get(valueUrl).then((r) => r.text())
    try {
      await page.locator('.object-button').filter({ hasText: object }).click()
      await page.getByRole('button', { name: '配置表', exact: true }).click()
      await page.locator('.config-key').filter({ hasText: key }).click()
      await page.getByRole('button', { name: '编辑', exact: true }).click()
      await expect(page.getByRole('textbox', { name: 'endpoint', exact: true })).toBeVisible()
      await expect(page.getByRole('textbox', { name: 'api_key', exact: true })).toBeVisible()
      if (object === '/module/openai-provider') {
        await expect(page.getByRole('textbox', { name: 'model', exact: true })).toBeVisible()
        const flag = page.getByRole('checkbox', { name: 'multimodal', exact: true })
        const enabled = await flag.isChecked()
        await flag.setChecked(!enabled)
        await flag.setChecked(enabled)
        const choice = page.getByRole('combobox', { name: 'output_limit', exact: true })
        const selected = await choice.inputValue()
        await choice.selectOption(selected === 'ProviderDefault' ? 'Tokens' : 'ProviderDefault')
        await choice.selectOption(selected)
        const reasoning = page.getByRole('checkbox', {
          name: 'reasoning_effort · 自定义',
          exact: true,
        })
        const custom = await reasoning.isChecked()
        await reasoning.setChecked(!custom)
        await reasoning.setChecked(custom)
      } else if (object === '/module/qwen-vl-embedding') {
        await expect(page.getByRole('textbox', { name: 'model', exact: true })).toHaveCount(0)
      }
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(
        true,
      )
      await page.getByRole('button', { name: '保存', exact: true }).click()
      await expect(page.locator('.config-editor')).toHaveCount(0)
      // No field, token width, optional value or API credential is lost to the form.
      expect((await page.request.get(valueUrl).then((r) => r.text())) === original).toBe(true)
    } finally {
      const restored = await page.request.post('/api/config/set', {
        data: { object, key, value: original },
      })
      expect(restored.status()).toBe(204)
    }
  }
  expect(errors).toEqual([])
})
