'use client';

import { DataTable, LoadingError, SectionFrame, SimpleBarList, useApiData } from '@/components/dashboard';

export default function UsersPage() {
  const { data, loading, error } = useApiData<Array<Record<string, unknown>>>('/api/users');
  const rows = data ?? [];

  return (
    <SectionFrame title='Individual Engagement'>
      <LoadingError loading={loading} error={error} />
      {rows.length > 0 ? (
        <>
          <SimpleBarList title='Top Users by Engagement Score' data={rows} labelKey='user_id' valueKey='engagement_score_0_100' />
          <DataTable columns={['user_id', 'first_name', 'last_name', 'company_id', 'engagement_score_0_100', 'super_user_score_0_100', 'engagement_tier']} rows={rows.slice(0, 100)} />
        </>
      ) : <p>No data available for current ingestion/filter.</p>}
    </SectionFrame>
  );
}
