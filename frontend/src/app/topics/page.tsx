import { fetchApi } from '@/lib/api';

export default async function TopicsPage() {
  const data = await fetchApi<any[]>('/api/topics');
  return <div><h2>Discussion & Topic Intelligence</h2><pre>{JSON.stringify(data, null, 2)}</pre></div>;
}
