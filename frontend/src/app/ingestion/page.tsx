'use client';

import { useState } from 'react';

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export default function IngestionPage() {
  const [msg, setMsg] = useState('');
  const [force, setForce] = useState(false);

  async function upload(files: FileList | null) {
    if (!files?.length) return;
    const fd = new FormData();
    Array.from(files).forEach((f) => fd.append('files', f));
    const res = await fetch(`${API}/api/ingestions?force=${force}`, { method: 'POST', body: fd });
    setMsg(JSON.stringify(await res.json(), null, 2));
  }

  return (
    <div>
      <h2>Ingestion & Data Quality</h2>
      <label style={{ display: 'block', marginBottom: 10 }}>
        <input type='checkbox' checked={force} onChange={(e) => setForce(e.target.checked)} /> Force re-ingest duplicate files
      </label>
      <input type='file' multiple onChange={(e) => upload(e.target.files)} />
      <p>Supports periodic Excel exports, dedupe by file hash, and logs ingestion runs.</p>
      <pre>{msg}</pre>
    </div>
  );
}
