import Nav from '@/components/Nav';

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html>
      <body style={{ fontFamily: 'Arial', margin: 20 }}>
        <h1>NHIA Community Analytics</h1>
        <p>Timezone display/bucketing: America/New_York</p>
        <Nav />
        {children}
      </body>
    </html>
  );
}
