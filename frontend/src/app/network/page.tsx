'use client';

import { DataTable, LoadingError, SectionFrame, useApiData } from '@/components/dashboard';

type NetworkResponse = {
  edges: Array<Record<string, unknown>>;
  metrics: Array<Record<string, unknown>>;
};

export default function NetworkPage() {
  const { data, loading, error } = useApiData<NetworkResponse>('/api/network');

  return (
    <SectionFrame title='Knowledge Network'>
      <LoadingError loading={loading} error={error} />
      {data ? (
        <>
          <h3>Top Influential Users (PageRank)</h3>
          <DataTable columns={['user_id', 'pagerank', 'betweenness', 'in_degree', 'out_degree', 'reciprocity_rate']} rows={data.metrics.slice(0, 100)} />
          <h3 style={{ marginTop: 20 }}>Network Edges (Sample)</h3>
          <DataTable columns={['from_user_id', 'to_user_id', 'weight', 'interaction_type', 'period_start_date']} rows={data.edges.slice(0, 100)} />
        </>
      ) : <p>No network data available yet.</p>}
    </SectionFrame>
  );
}
