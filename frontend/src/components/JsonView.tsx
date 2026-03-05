'use client';

import { useEffect, useState } from 'react';

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export default function JsonView({ path, title }: { path: string; title: string }) {
  const [data, setData] = useState<unknown>(null);
  const [error, setError] = useState<string>('');

  useEffect(() => {
    let active = true;
    fetch(`${API}${path}`, { cache: 'no-store' })
      .then(async (res) => {
        if (!res.ok) throw new Error(`Failed (${res.status})`);
        return res.json();
      })
      .then((json) => {
        if (active) setData(json);
      })
      .catch((e: Error) => {
        if (active) setError(e.message);
      });
    return () => {
      active = false;
    };
  }, [path]);

  return (
    <div>
      <h2>{title}</h2>
      {error ? <p style={{ color: 'crimson' }}>Error: {error}</p> : null}
      <pre>{JSON.stringify(data, null, 2)}</pre>
    </div>
  );
}
