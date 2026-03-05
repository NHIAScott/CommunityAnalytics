import { fetchApi } from '@/lib/api';

export default async function NetworkPage() {
  const data = await fetchApi<any>('/api/network');
  return <div><h2>Knowledge Network</h2><pre>{JSON.stringify(data, null, 2)}</pre></div>;
}
