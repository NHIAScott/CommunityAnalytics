'use client';

import { CSSProperties, useEffect, useMemo, useState } from 'react';

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

const cardStyle: CSSProperties = { border: '1px solid #ddd', borderRadius: 8, padding: 12, minWidth: 150, background: '#fafafa' };

export function useApiData<T>(path: string) {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    setLoading(true);
    fetch(`${API}${path}`, { cache: 'no-store' })
      .then(async (r) => {
        if (!r.ok) throw new Error(`Request failed (${r.status})`);
        return r.json();
      })
      .then((json) => {
        if (!active) return;
        setData(json);
        setError('');
      })
      .catch((e: Error) => {
        if (!active) return;
        setError(e.message);
      })
      .finally(() => {
        if (!active) return;
        setLoading(false);
      });

    return () => {
      active = false;
    };
  }, [path]);

  return { data, loading, error };
}

export function SectionFrame({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section>
      <h2>{title}</h2>
      {children}
    </section>
  );
}

export function LoadingError({ loading, error }: { loading: boolean; error: string }) {
  if (loading) return <p>Loading...</p>;
  if (error) return <p style={{ color: 'crimson' }}>Error: {error}</p>;
  return null;
}

export function KpiCards({ items }: { items: Array<{ label: string; value: number | string | null | undefined }> }) {
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12, marginBottom: 20 }}>
      {items.map((item) => (
        <div key={item.label} style={cardStyle}>
          <div style={{ fontSize: 12, color: '#555' }}>{item.label}</div>
          <div style={{ fontSize: 22, fontWeight: 700 }}>{item.value ?? '—'}</div>
        </div>
      ))}
    </div>
  );
}

export function SimpleBarList({
  title,
  data,
  labelKey,
  valueKey,
  maxItems = 10,
}: {
  title: string;
  data: Record<string, unknown>[];
  labelKey: string;
  valueKey: string;
  maxItems?: number;
}) {
  const rows = useMemo(() => {
    return [...data]
      .map((d) => ({
        label: String(d[labelKey] ?? 'Unknown'),
        value: Number(d[valueKey] ?? 0),
      }))
      .sort((a, b) => b.value - a.value)
      .slice(0, maxItems);
  }, [data, labelKey, valueKey, maxItems]);

  const max = rows.length ? Math.max(...rows.map((r) => r.value), 1) : 1;

  return (
    <div style={{ marginBottom: 20 }}>
      <h3>{title}</h3>
      {rows.map((r) => (
        <div key={r.label} style={{ marginBottom: 8 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12 }}>
            <span>{r.label}</span>
            <strong>{r.value}</strong>
          </div>
          <div style={{ height: 10, background: '#eee', borderRadius: 4 }}>
            <div style={{ height: 10, width: `${(r.value / max) * 100}%`, background: '#2f6fed', borderRadius: 4 }} />
          </div>
        </div>
      ))}
    </div>
  );
}

export function DataTable({ columns, rows }: { columns: string[]; rows: Record<string, unknown>[] }) {
  return (
    <div style={{ overflowX: 'auto', border: '1px solid #ddd', borderRadius: 8 }}>
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c} style={{ textAlign: 'left', padding: 8, borderBottom: '1px solid #ddd', background: '#f6f6f6' }}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => (
            <tr key={idx}>
              {columns.map((c) => (
                <td key={c} style={{ padding: 8, borderBottom: '1px solid #eee', fontSize: 13 }}>{String(row[c] ?? '—')}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
