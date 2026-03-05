const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export async function fetchApi<T>(path: string): Promise<T> {
  const r = await fetch(`${API}${path}`, { cache: 'no-store' });
  if (!r.ok) throw new Error(`API ${path} failed`);
  return r.json();
}
