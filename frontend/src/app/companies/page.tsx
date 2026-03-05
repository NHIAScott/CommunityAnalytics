'use client';

import { DataTable, LoadingError, SectionFrame, SimpleBarList, useApiData } from '@/components/dashboard';

export default function CompaniesPage() {
  const { data, loading, error } = useApiData<Array<Record<string, unknown>>>('/api/companies');
  const rows = data ?? [];

  return (
    <SectionFrame title='Company Engagement & Health'>
      <LoadingError loading={loading} error={error} />
      {rows.length > 0 ? (
        <>
          <SimpleBarList title='Top Companies by Health Score' data={rows} labelKey='company_name_canonical' valueKey='company_health_score_0_100' />
          <DataTable columns={['company_name_canonical', 'company_health_score_0_100', 'active_users', 'engaged_users', 'risk_flags_json']} rows={rows.slice(0, 100)} />
        </>
      ) : null}
    </SectionFrame>
  );
}
