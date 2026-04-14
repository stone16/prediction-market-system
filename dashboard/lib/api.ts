export async function apiGet<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`/api/pms${path}`, {
    ...init,
    cache: 'no-store'
  });
  if (!response.ok) throw new Error(`API ${path} returned ${response.status}`);
  return (await response.json()) as T;
}

export async function apiPost<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`/api/pms${path}`, {
    method: 'POST',
    ...init
  });
  if (!response.ok) throw new Error(`API ${path} returned ${response.status}`);
  return (await response.json()) as T;
}
