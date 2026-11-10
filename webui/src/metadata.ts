export function webLink(value: string | null | undefined): string | undefined {
  return value && /^https?:\/\//i.test(value) ? value : undefined
}
