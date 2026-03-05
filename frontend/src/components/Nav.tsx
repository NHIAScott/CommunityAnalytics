import Link from 'next/link';

const links = [
  ['Ingestion', '/ingestion'],
  ['Executive Overview', '/overview'],
  ['Individual Engagement', '/users'],
  ['Company Health', '/companies'],
  ['Topic Intelligence', '/topics'],
  ['Knowledge Network', '/network'],
  ['Metrics Dictionary', '/metrics-dictionary'],
];

export default function Nav() {
  return (
    <nav style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 20 }}>
      {links.map(([label, href]) => (
        <Link key={href} href={href}>{label}</Link>
      ))}
    </nav>
  );
}
