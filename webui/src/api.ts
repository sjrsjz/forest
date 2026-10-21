export class HttpError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message)
  }
}

export async function request<T>(url: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(url, { ...init, credentials: 'same-origin', cache: 'no-store' })
  if (!response.ok) {
    const text = await response.text()
    let message = text || `HTTP ${response.status}`
    try {
      message = (JSON.parse(text) as { message: string }).message ?? message
    } catch {
      /* Framework query errors may be plain text. */
    }
    throw new HttpError(response.status, message)
  }
  return response.status === 204 ? (undefined as T) : ((await response.json()) as T)
}
export function query(path: string, params: Record<string, string>): string {
  return `/api/inspect/${path}?${new URLSearchParams(params)}`
}
