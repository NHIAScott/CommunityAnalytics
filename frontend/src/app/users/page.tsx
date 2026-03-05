import { fetchApi } from '@/lib/api';

export default async function UsersPage() {
  const data = await fetchApi<any[]>('/api/users');
  return <div><h2>Individual Engagement</h2><pre>{JSON.stringify(data.slice(0, 100), null, 2)}</pre></div>;
}
