import { fetchApi } from '@/lib/api';

export default async function OverviewPage() {
  const data = await fetchApi<any>('/api/overview');
  return <div><h2>Executive Overview</h2><pre>{JSON.stringify(data, null, 2)}</pre></div>;
}
