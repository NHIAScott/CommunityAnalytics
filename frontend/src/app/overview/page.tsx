'use client';

import { DataTable, KpiCards, LoadingError, SectionFrame, SimpleBarList, useApiData } from '@/components/dashboard';

type OverviewResponse = {
  kpis: Record<string, number>;
  trends: Array<Record<string, unknown>>;
};

export default function OverviewPage() {
  const { data, loading, error } = useApiData<OverviewResponse>('/api/overview');

  const kpis = data?.kpis ?? {};
  const trendRows = data?.trends ?? [];

  return (
    <SectionFrame title='Executive Overview'>
      <LoadingError loading={loading} error={error} />
      {data ? (
        <>
          <KpiCards
            items={[
              { label: 'Total Members', value: kpis.total_members },
              { label: 'Active Users', value: kpis.active_users },
              { label: 'Engaged Users', value: kpis.engaged_users },
              { label: 'Contributors', value: kpis.contributors },
              { label: 'Threads', value: kpis.total_threads },
              { label: 'Replies', value: kpis.total_replies },
              { label: 'Downloads', value: kpis.downloads },
            ]}
          />
          <SimpleBarList title='Trend: Logins by Period' data={trendRows} labelKey='period_start_date' valueKey='logins' />
          <DataTable columns={['period_start_date', 'logins', 'downloads', 'threads', 'replies']} rows={trendRows} />
        </>
      ) : null}
    </SectionFrame>
  );
}
