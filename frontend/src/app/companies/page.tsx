import { fetchApi } from '@/lib/api';

export default async function CompaniesPage() {
  const data = await fetchApi<any[]>('/api/companies');
  return <div><h2>Company Engagement & Health</h2><pre>{JSON.stringify(data, null, 2)}</pre></div>;
}
