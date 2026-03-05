'use client';

import { DataTable, LoadingError, SectionFrame, SimpleBarList, useApiData } from '@/components/dashboard';

export default function TopicsPage() {
  const { data, loading, error } = useApiData<Array<Record<string, unknown>>>('/api/topics');
  const rows = data ?? [];

  return (
    <SectionFrame title='Discussion & Topic Intelligence'>
      <LoadingError loading={loading} error={error} />
      {rows.length > 0 ? (
        <>
          <SimpleBarList title='Most Influential Topics' data={rows} labelKey='topic_label' valueKey='influence_score' />
          <DataTable columns={['topic_id', 'topic_label', 'threads', 'replies', 'influence_score', 'top_keywords_json']} rows={rows.slice(0, 100)} />
        </>
      ) : null}
    </SectionFrame>
  );
}
